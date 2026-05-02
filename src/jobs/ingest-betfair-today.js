const db = require('../db/connection');
const BetfairClient = require('../betfair/client');

const FOOTBALL_EVENT_TYPE_ID = '1';

const MARKET_TYPES = [
  'MATCH_ODDS',
  'DOUBLE_CHANCE',
  'OVER_UNDER_15',
  'OVER_UNDER_25',
  'OVER_UNDER_35',
  'BOTH_TEAMS_TO_SCORE',
  'CORRECT_SCORE'
];

function todayRangeUtc() {
  const now = new Date();

  const end = new Date(now);
  end.setUTCHours(23, 59, 59, 999);

  return {
    from: now.toISOString(),
    to: end.toISOString()
  };
}

async function upsertEvent(client, eventResult) {
  const event = eventResult.event;
  const competition = eventResult.competition || null;

  await client.query(
    `
    INSERT INTO betfair_events (
      betfair_event_id,
      event_name,
      country_code,
      timezone,
      open_date,
      competition_id,
      competition_name,
      raw,
      updated_at
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NOW())
    ON CONFLICT (betfair_event_id)
    DO UPDATE SET
      event_name = EXCLUDED.event_name,
      country_code = EXCLUDED.country_code,
      timezone = EXCLUDED.timezone,
      open_date = EXCLUDED.open_date,
      competition_id = EXCLUDED.competition_id,
      competition_name = EXCLUDED.competition_name,
      raw = EXCLUDED.raw,
      updated_at = NOW()
    `,
    [
      event.id,
      event.name,
      event.countryCode || null,
      event.timezone || null,
      event.openDate || null,
      competition?.id || null,
      competition?.name || null,
      eventResult
    ]
  );
}

async function upsertMarket(client, market) {
  await client.query(
    `
    INSERT INTO betfair_markets (
      betfair_market_id,
      betfair_event_id,
      market_name,
      market_type_code,
      total_matched,
      market_start_time,
      raw,
      updated_at
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,NOW())
    ON CONFLICT (betfair_market_id)
    DO UPDATE SET
      betfair_event_id = EXCLUDED.betfair_event_id,
      market_name = EXCLUDED.market_name,
      market_type_code = EXCLUDED.market_type_code,
      total_matched = EXCLUDED.total_matched,
      market_start_time = EXCLUDED.market_start_time,
      raw = EXCLUDED.raw,
      updated_at = NOW()
    `,
    [
      market.marketId,
      market.event.id,
      market.marketName,
      market.description?.marketType || null,
      market.totalMatched || null,
      market.marketStartTime || null,
      market
    ]
  );
}

async function upsertRunner(client, marketId, runner) {
  await client.query(
    `
    INSERT INTO betfair_runners (
      betfair_market_id,
      selection_id,
      runner_name,
      handicap,
      sort_priority,
      raw,
      updated_at
    )
    VALUES ($1,$2,$3,$4,$5,$6,NOW())
    ON CONFLICT (betfair_market_id, selection_id, handicap)
    DO UPDATE SET
      runner_name = EXCLUDED.runner_name,
      sort_priority = EXCLUDED.sort_priority,
      raw = EXCLUDED.raw,
      updated_at = NOW()
    `,
    [
      marketId,
      runner.selectionId,
      runner.runnerName,
      runner.handicap || 0,
      runner.sortPriority || null,
      runner
    ]
  );
}

async function main() {
  const betfair = new BetfairClient();
  const marketStartTime = todayRangeUtc();

  const baseFilter = {
    eventTypeIds: [FOOTBALL_EVENT_TYPE_ID],
    marketStartTime
  };

  console.log('Betfair football ingestion started');
  console.log('Time range:', marketStartTime.from, '→', marketStartTime.to);

  const events = await betfair.listEvents(baseFilter);
  console.log(`Events found: ${events.length}`);

  const client = await db.getClient();

  try {
    await client.query('BEGIN');

    for (const eventResult of events) {
      await upsertEvent(client, eventResult);
    }

    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }

  const marketFilter = {
    ...baseFilter,
    marketTypeCodes: MARKET_TYPES
  };

  const markets = await betfair.listMarketCatalogue(marketFilter, '1000');
  console.log(`Markets found: ${markets.length}`);

  const client2 = await db.getClient();

  try {
    await client2.query('BEGIN');

    for (const market of markets) {
      await upsertEvent(client2, {
        event: market.event,
        competition: market.competition || null,
        source: 'listMarketCatalogue'
      });

      await upsertMarket(client2, market);

      for (const runner of market.runners || []) {
        await upsertRunner(client2, market.marketId, runner);
      }
    }

    await client2.query('COMMIT');
  } catch (err) {
    await client2.query('ROLLBACK');
    throw err;
  } finally {
    client2.release();
    await db.pool.end();
  }

  console.log('Betfair ingestion completed');
}

main().catch((err) => {
  console.error('Betfair ingestion failed');
  console.error(err.message);
  process.exit(1);
});
