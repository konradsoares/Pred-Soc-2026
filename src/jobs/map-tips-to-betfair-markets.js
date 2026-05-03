const db = require('../db/connection');

function todayDateISO() {
  return new Date().toISOString().slice(0, 10);
}

function normalize(value) {
  return String(value || '')
    .toLowerCase()
    .normalize('NFKD')
    .replace(/&/g, 'and')
    .replace(/[^\w\s.]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function mapTipToBetfairMarketType(market, pick) {
  if (market === '1X2') return 'MATCH_ODDS';
  if (market === 'double_chance') return 'DOUBLE_CHANCE';

  if (market === 'goals') {
    if (pick === 'over_1_5' || pick === 'under_1_5') return 'OVER_UNDER_15';
    if (pick === 'over_2_5' || pick === 'under_2_5') return 'OVER_UNDER_25';
  }

  return null;
}

function expectedRunnerNames(market, pick, fixture) {
  if (market === '1X2') {
    if (pick === '1') return [fixture.home_team];
    if (pick === 'X') return ['The Draw', 'Draw'];
    if (pick === '2') return [fixture.away_team];
  }

  if (market === 'double_chance') {
    if (pick === '1X') {
      return [
        'Home or Draw',
        `${fixture.home_team} or Draw`,
        `${fixture.home_team} / Draw`
      ];
    }

    if (pick === 'X2') {
      return [
        'Draw or Away',
        `Draw or ${fixture.away_team}`,
        `Draw / ${fixture.away_team}`
      ];
    }

    if (pick === '12') {
      return [
        'Home or Away',
        `${fixture.home_team} or ${fixture.away_team}`,
        `${fixture.home_team} / ${fixture.away_team}`
      ];
    }
  }

  if (market === 'goals') {
    if (pick === 'over_1_5') return ['Over 1.5 Goals'];
    if (pick === 'under_1_5') return ['Under 1.5 Goals'];
    if (pick === 'over_2_5') return ['Over 2.5 Goals'];
    if (pick === 'under_2_5') return ['Under 2.5 Goals'];
  }

  return [];
}

async function ensureMappingTable(client) {
  await client.query(`
    CREATE TABLE IF NOT EXISTS betfair_tip_mappings (
      id BIGSERIAL PRIMARY KEY,
      sent_tip_id BIGINT NOT NULL REFERENCES sent_tips(id) ON DELETE CASCADE,
      sent_tip_leg_id BIGINT REFERENCES sent_tip_legs(id) ON DELETE CASCADE,
      fixture_id BIGINT NOT NULL REFERENCES fixtures(id) ON DELETE CASCADE,
      betfair_event_id TEXT NOT NULL,
      betfair_market_id TEXT,
      market_type_code TEXT,
      selection_id BIGINT,
      runner_name TEXT,
      tip_market TEXT NOT NULL,
      tip_pick TEXT NOT NULL,
      mapping_status TEXT NOT NULL DEFAULT 'pending',
      reason TEXT,
      raw JSONB,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_betfair_tip_mappings_tip
    ON betfair_tip_mappings(sent_tip_id)
  `);

  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_betfair_tip_mappings_leg
    ON betfair_tip_mappings(sent_tip_leg_id)
  `);

  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_betfair_tip_mappings_market
    ON betfair_tip_mappings(betfair_market_id)
  `);
}

async function deleteExistingMappings(client, targetDate) {
  await client.query(
    `
    DELETE FROM betfair_tip_mappings m
    USING sent_tips st
    JOIN sent_tip_batches b ON b.id = st.batch_id
    WHERE m.sent_tip_id = st.id
      AND b.tip_date = $1::date
    `,
    [targetDate]
  );
}

async function loadSingleTips(client, targetDate) {
  const result = await client.query(
    `
    SELECT
      st.id AS sent_tip_id,
      NULL::bigint AS sent_tip_leg_id,
      st.fixture_id,
      st.market,
      st.pick,
      st.odds,
      st.stake,
      f.external_id AS betfair_event_id,
      ht.name AS home_team,
      at.name AS away_team
    FROM sent_tips st
    JOIN sent_tip_batches b ON b.id = st.batch_id
    JOIN fixtures f ON f.id = st.fixture_id
    JOIN teams ht ON ht.id = f.home_team_id
    JOIN teams at ON at.id = f.away_team_id
    WHERE b.tip_date = $1::date
      AND st.bet_type = 'single'
    ORDER BY st.id
    `,
    [targetDate]
  );

  return result.rows;
}

async function loadLegTips(client, targetDate) {
  const result = await client.query(
    `
    SELECT
      st.id AS sent_tip_id,
      l.id AS sent_tip_leg_id,
      l.fixture_id,
      l.market,
      l.pick,
      l.odds,
      st.stake,
      f.external_id AS betfair_event_id,
      ht.name AS home_team,
      at.name AS away_team
    FROM sent_tip_legs l
    JOIN sent_tips st ON st.id = l.sent_tip_id
    JOIN sent_tip_batches b ON b.id = st.batch_id
    JOIN fixtures f ON f.id = l.fixture_id
    JOIN teams ht ON ht.id = f.home_team_id
    JOIN teams at ON at.id = f.away_team_id
    WHERE b.tip_date = $1::date
      AND st.bet_type IN ('accumulator', 'system')
    ORDER BY st.id, l.id
    `,
    [targetDate]
  );

  return result.rows;
}

async function findBetfairMarket(client, betfairEventId, marketTypeCode) {
  const result = await client.query(
    `
    SELECT
      betfair_market_id,
      betfair_event_id,
      market_name,
      market_type_code,
      market_start_time,
      raw
    FROM betfair_markets
    WHERE betfair_event_id = $1
      AND market_type_code = $2
    ORDER BY market_start_time ASC
    `,
    [betfairEventId, marketTypeCode]
  );

  return result.rows;
}

async function findRunner(client, betfairMarketId, expectedNames) {
  const runnersResult = await client.query(
    `
    SELECT
      betfair_market_id,
      selection_id,
      runner_name,
      handicap,
      sort_priority,
      raw
    FROM betfair_runners
    WHERE betfair_market_id = $1
    ORDER BY sort_priority ASC NULLS LAST, runner_name ASC
    `,
    [betfairMarketId]
  );

  const runners = runnersResult.rows;
  const expected = expectedNames.map(normalize).filter(Boolean);

  const exact = runners.filter((r) =>
    expected.includes(normalize(r.runner_name))
  );

  if (exact.length === 1) {
    return {
      status: 'mapped',
      runner: exact[0],
      reason: null,
      allRunners: runners
    };
  }

  if (exact.length > 1) {
    return {
      status: 'ambiguous_runner',
      runner: null,
      reason: `Multiple exact runner matches for expected names: ${expectedNames.join(', ')}`,
      allRunners: runners
    };
  }

  const contains = runners.filter((r) => {
    const runner = normalize(r.runner_name);
    return expected.some((e) => runner.includes(e) || e.includes(runner));
  });

  if (contains.length === 1) {
    return {
      status: 'mapped',
      runner: contains[0],
      reason: null,
      allRunners: runners
    };
  }

  return {
    status: 'no_runner',
    runner: null,
    reason: `Runner not found. Expected: ${expectedNames.join(', ')}. Available: ${runners.map((r) => r.runner_name).join(', ')}`,
    allRunners: runners
  };
}

async function insertMapping(client, item, mapping) {
  await client.query(
    `
    INSERT INTO betfair_tip_mappings (
      sent_tip_id,
      sent_tip_leg_id,
      fixture_id,
      betfair_event_id,
      betfair_market_id,
      market_type_code,
      selection_id,
      runner_name,
      tip_market,
      tip_pick,
      mapping_status,
      reason,
      raw,
      updated_at
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,NOW())
    `,
    [
      item.sent_tip_id,
      item.sent_tip_leg_id,
      item.fixture_id,
      item.betfair_event_id,
      mapping.betfair_market_id || null,
      mapping.market_type_code || null,
      mapping.selection_id || null,
      mapping.runner_name || null,
      item.market,
      item.pick,
      mapping.status,
      mapping.reason || null,
      JSON.stringify(mapping.raw || {})
    ]
  );
}

async function mapItem(client, item) {
  const marketTypeCode = mapTipToBetfairMarketType(item.market, item.pick);

  if (!marketTypeCode) {
    return {
      status: 'unsupported_market',
      reason: `Unsupported tip market/pick: ${item.market} ${item.pick}`,
      raw: { item }
    };
  }

  if (!item.betfair_event_id) {
    return {
      status: 'no_event',
      market_type_code: marketTypeCode,
      reason: 'Fixture has no Betfair event id in fixtures.external_id',
      raw: { item }
    };
  }

  const markets = await findBetfairMarket(
    client,
    item.betfair_event_id,
    marketTypeCode
  );

  if (!markets.length) {
    return {
      status: 'no_market',
      market_type_code: marketTypeCode,
      reason: `No Betfair market found for event=${item.betfair_event_id}, marketType=${marketTypeCode}`,
      raw: { item }
    };
  }

  if (markets.length > 1) {
    return {
      status: 'ambiguous_market',
      market_type_code: marketTypeCode,
      reason: `Multiple Betfair markets found for event=${item.betfair_event_id}, marketType=${marketTypeCode}`,
      raw: { item, markets }
    };
  }

  const market = markets[0];
  const expectedNames = expectedRunnerNames(item.market, item.pick, item);

  if (!expectedNames.length) {
    return {
      status: 'unsupported_runner',
      betfair_market_id: market.betfair_market_id,
      market_type_code: marketTypeCode,
      reason: `No runner mapping rule for ${item.market} ${item.pick}`,
      raw: { item, market }
    };
  }

  const runnerMatch = await findRunner(
    client,
    market.betfair_market_id,
    expectedNames
  );

  if (runnerMatch.status !== 'mapped') {
    return {
      status: runnerMatch.status,
      betfair_market_id: market.betfair_market_id,
      market_type_code: marketTypeCode,
      reason: runnerMatch.reason,
      raw: {
        item,
        market,
        expectedNames,
        availableRunners: runnerMatch.allRunners
      }
    };
  }

  return {
    status: 'mapped',
    betfair_market_id: market.betfair_market_id,
    market_type_code: marketTypeCode,
    selection_id: runnerMatch.runner.selection_id,
    runner_name: runnerMatch.runner.runner_name,
    reason: null,
    raw: {
      item,
      market,
      expectedNames,
      runner: runnerMatch.runner
    }
  };
}

async function main() {
  const targetDate = process.argv[2] || todayDateISO();

  console.log(`Mapping tips to Betfair markets for ${targetDate}`);

  const client = await db.getClient();

  try {
    await client.query('BEGIN');

    await ensureMappingTable(client);
    await deleteExistingMappings(client, targetDate);

    const singles = await loadSingleTips(client, targetDate);
    const legs = await loadLegTips(client, targetDate);
    const items = [...singles, ...legs];

    console.log(`Tips/legs to map: ${items.length}`);

    let mapped = 0;
    let rejected = 0;

    for (const item of items) {
      const mapping = await mapItem(client, item);
      await insertMapping(client, item, mapping);

      if (mapping.status === 'mapped') {
        mapped += 1;
        console.log(
          `Mapped: tip=${item.sent_tip_id}` +
          `${item.sent_tip_leg_id ? ` leg=${item.sent_tip_leg_id}` : ''} | ` +
          `${item.home_team} v ${item.away_team} | ` +
          `${item.market} ${item.pick} -> ${mapping.market_type_code} / ${mapping.runner_name} ` +
          `(marketId=${mapping.betfair_market_id}, selectionId=${mapping.selection_id})`
        );
      } else {
        rejected += 1;
        console.log(
          `Rejected: tip=${item.sent_tip_id}` +
          `${item.sent_tip_leg_id ? ` leg=${item.sent_tip_leg_id}` : ''} | ` +
          `${item.home_team} v ${item.away_team} | ${item.market} ${item.pick} | ` +
          `${mapping.status} | ${mapping.reason}`
        );
      }
    }

    await client.query('COMMIT');

    console.log(`Mapping finished. Mapped: ${mapped}, rejected: ${rejected}`);
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Mapping failed:', err.message);
    process.exitCode = 1;
  } finally {
    client.release();
    await db.pool.end();
  }
}

main();
