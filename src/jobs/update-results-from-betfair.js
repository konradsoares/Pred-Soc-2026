const db = require('../db/connection');
const BetfairClient = require('../betfair/client');

function todayDateISO() {
  return new Date().toISOString().slice(0, 10);
}

function chunkArray(items, size) {
  const chunks = [];

  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size));
  }

  return chunks;
}

function calculateProfitLoss(status, stake, odds) {
  const s = Number(stake || 0);
  const o = Number(odds || 0);

  if (status === 'won') return Number((s * (o - 1)).toFixed(2));
  if (status === 'lost') return Number((-s).toFixed(2));
  if (status === 'void') return 0;

  return null;
}

function mapRunnerStatus(runnerStatus) {
  if (runnerStatus === 'WINNER') return 'won';
  if (runnerStatus === 'LOSER') return 'lost';
  if (runnerStatus === 'REMOVED' || runnerStatus === 'REMOVED_VACANT') return 'void';
  return 'pending';
}

async function loadMappedPendingSelections(client, targetDate) {
  const result = await client.query(
    `
    SELECT
      m.id AS mapping_id,
      m.sent_tip_id,
      m.sent_tip_leg_id,
      m.fixture_id,
      m.betfair_event_id,
      m.betfair_market_id,
      m.market_type_code,
      m.selection_id,
      m.runner_name,
      m.tip_market,
      m.tip_pick,
      st.bet_type,
      st.odds AS tip_odds,
      st.stake AS tip_stake,
      l.odds AS leg_odds,
      ht.name AS home_team,
      at.name AS away_team
    FROM betfair_tip_mappings m
    JOIN sent_tips st ON st.id = m.sent_tip_id
    JOIN sent_tip_batches b ON b.id = st.batch_id
    LEFT JOIN sent_tip_legs l ON l.id = m.sent_tip_leg_id
    JOIN fixtures f ON f.id = m.fixture_id
    JOIN teams ht ON ht.id = f.home_team_id
    JOIN teams at ON at.id = f.away_team_id
    WHERE b.tip_date = $1::date
      AND m.mapping_status = 'mapped'
      AND (
        (m.sent_tip_leg_id IS NULL AND st.status = 'pending')
        OR
        (m.sent_tip_leg_id IS NOT NULL AND l.status = 'pending')
      )
    ORDER BY m.betfair_market_id, m.selection_id
    `,
    [targetDate]
  );

  return result.rows;
}

async function fetchMarketBooks(betfair, marketIds) {
  const uniqueMarketIds = [...new Set(marketIds.filter(Boolean))];
  const chunks = chunkArray(uniqueMarketIds, 40);
  const books = [];

  for (let i = 0; i < chunks.length; i += 1) {
    console.log(`Fetching Betfair market books ${i + 1}/${chunks.length}`);
    const result = await betfair.listMarketBook(chunks[i]);
    books.push(...result);
  }

  return books;
}

function buildMarketBookMap(marketBooks) {
  const map = new Map();

  for (const book of marketBooks) {
    map.set(book.marketId, book);
  }

  return map;
}

async function updateSingleFromBetfair(client, item, status) {
  const profitLoss = calculateProfitLoss(status, item.tip_stake, item.tip_odds);

  await client.query(
    `
    UPDATE sent_tips
    SET status = $1,
        profit_loss = $2
    WHERE id = $3
    `,
    [status, profitLoss, item.sent_tip_id]
  );

  console.log(
    `Single ${item.sent_tip_id}: ${status} P/L ${profitLoss} | ` +
    `${item.home_team} v ${item.away_team} | ${item.tip_market} ${item.tip_pick}`
  );
}

async function updateLegFromBetfair(client, item, status) {
  await client.query(
    `
    UPDATE sent_tip_legs
    SET status = $1
    WHERE id = $2
    `,
    [status, item.sent_tip_leg_id]
  );

  console.log(
    `Leg ${item.sent_tip_leg_id}: ${status} | ` +
    `${item.home_team} v ${item.away_team} | ${item.tip_market} ${item.tip_pick}`
  );
}

async function settleAccumulators(client, targetDate) {
  const result = await client.query(
    `
    SELECT
      st.id,
      st.odds,
      st.stake,
      COUNT(l.id) AS total_legs,
      COUNT(*) FILTER (WHERE l.status = 'won') AS won_legs,
      COUNT(*) FILTER (WHERE l.status = 'lost') AS lost_legs,
      COUNT(*) FILTER (WHERE l.status = 'void') AS void_legs,
      COUNT(*) FILTER (WHERE l.status = 'pending') AS pending_legs
    FROM sent_tips st
    JOIN sent_tip_batches b ON b.id = st.batch_id
    JOIN sent_tip_legs l ON l.sent_tip_id = st.id
    WHERE b.tip_date = $1::date
      AND st.bet_type = 'accumulator'
      AND st.status = 'pending'
    GROUP BY st.id, st.odds, st.stake
    ORDER BY st.id
    `,
    [targetDate]
  );

  for (const acca of result.rows) {
    let status = 'pending';

    if (Number(acca.lost_legs) > 0) {
      status = 'lost';
    } else if (
      Number(acca.pending_legs) === 0 &&
      Number(acca.won_legs) + Number(acca.void_legs) === Number(acca.total_legs)
    ) {
      status = 'won';
    }

    if (status === 'pending') continue;

    const profitLoss = calculateProfitLoss(status, acca.stake, acca.odds);

    await client.query(
      `
      UPDATE sent_tips
      SET status = $1,
          profit_loss = $2
      WHERE id = $3
      `,
      [status, profitLoss, acca.id]
    );

    console.log(`Accumulator ${acca.id}: ${status} P/L ${profitLoss}`);
  }
}

async function main() {
  const targetDate = process.argv[2] || todayDateISO();

  console.log(`Updating results from Betfair for ${targetDate}`);

  const betfair = new BetfairClient();
  const client = await db.getClient();

  try {
    const mappedSelections = await loadMappedPendingSelections(client, targetDate);

    console.log(`Mapped pending selections: ${mappedSelections.length}`);

    if (!mappedSelections.length) {
      console.log('Nothing to update.');
      return;
    }

    const marketBooks = await fetchMarketBooks(
      betfair,
      mappedSelections.map((m) => m.betfair_market_id)
    );

    const marketBookMap = buildMarketBookMap(marketBooks);

    await client.query('BEGIN');

    let updated = 0;
    let pending = 0;
    let missing = 0;

    for (const item of mappedSelections) {
      const book = marketBookMap.get(item.betfair_market_id);

      if (!book) {
        missing += 1;
        console.log(`Missing market book: ${item.betfair_market_id}`);
        continue;
      }

      if (book.status !== 'CLOSED') {
        pending += 1;
        continue;
      }

      const runner = (book.runners || []).find(
        (r) => Number(r.selectionId) === Number(item.selection_id)
      );

      if (!runner) {
        missing += 1;
        console.log(
          `Runner missing: market=${item.betfair_market_id}, selection=${item.selection_id}`
        );
        continue;
      }

      const status = mapRunnerStatus(runner.status);

      if (status === 'pending') {
        pending += 1;
        continue;
      }

      if (item.sent_tip_leg_id) {
        await updateLegFromBetfair(client, item, status);
      } else {
        await updateSingleFromBetfair(client, item, status);
      }

      updated += 1;
    }

    await settleAccumulators(client, targetDate);

    await client.query('COMMIT');

    console.log(
      `Betfair results update finished. Updated: ${updated}, pending: ${pending}, missing: ${missing}`
    );
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    console.error('Betfair results update failed:', err.message);
    process.exitCode = 1;
  } finally {
    client.release();
    await db.pool.end();
  }
}

main();
