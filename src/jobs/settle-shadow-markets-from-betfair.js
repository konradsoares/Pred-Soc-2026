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

function mapRunnerStatus(status) {
  if (status === 'WINNER') return 'won';
  if (status === 'LOSER') return 'lost';
  if (status === 'REMOVED') return 'void';
  return 'pending';
}

function calcPL(status, odds) {
  const o = Number(odds || 0);

  if (status === 'won') return Number((o - 1).toFixed(2));
  if (status === 'lost') return -1;
  if (status === 'void') return 0;

  return null;
}

async function loadPendingShadowMarkets(client, targetDate) {
  return (
    await client.query(
      `
      SELECT smt.*, f.external_id AS betfair_event_id
      FROM shadow_market_tests smt
      JOIN fixtures f ON f.id = smt.fixture_id
      WHERE smt.status = 'pending'
        AND f.fixture_date = $1::date
      `,
      [targetDate]
    )
  ).rows;
}

async function loadBetfairMarkets(client, eventIds) {
  return (
    await client.query(
      `
      SELECT *
      FROM betfair_markets
      WHERE betfair_event_id = ANY($1)
      `,
      [eventIds]
    )
  ).rows;
}

async function loadBetfairRunners(client, marketIds) {
  return (
    await client.query(
      `
      SELECT *
      FROM betfair_runners
      WHERE betfair_market_id = ANY($1)
      `,
      [marketIds]
    )
  ).rows;
}

function findMatchingMarket(markets, shadow) {
  return markets.find(
    (m) =>
      m.market_type_code === mapMarketType(shadow.market, shadow.pick)
  );
}

function mapMarketType(market, pick) {
  if (market === '1X2') return 'MATCH_ODDS';
  if (market === 'double_chance') return 'DOUBLE_CHANCE';

  if (market === 'goals') {
    if (pick.includes('1_5')) return 'OVER_UNDER_15';
    if (pick.includes('2_5')) return 'OVER_UNDER_25';
  }

  return null;
}

function matchRunner(runners, shadow) {
  return runners.find((r) => {
    const name = r.runner_name.toLowerCase();

    if (shadow.market === '1X2') {
      if (shadow.pick === '1') return name.includes('home') || name.includes(shadow.home_team?.toLowerCase());
      if (shadow.pick === 'x') return name.includes('draw');
      if (shadow.pick === '2') return name.includes('away') || name.includes(shadow.away_team?.toLowerCase());
    }

    if (shadow.market === 'double_chance') {
      if (shadow.pick === '1x') return name.includes('home') && name.includes('draw');
      if (shadow.pick === 'x2') return name.includes('draw') && name.includes('away');
      if (shadow.pick === '12') return name.includes('home') && name.includes('away');
    }

    if (shadow.market === 'goals') {
      return name.includes(shadow.pick.replace('_', '.'));
    }

    return false;
  });
}

async function main() {
  const targetDate = process.argv[2] || todayDateISO();

  console.log(`Settling shadow markets for ${targetDate}`);

  const client = await db.getClient();
  const betfair = new BetfairClient();

  try {
    const shadow = await loadPendingShadowMarkets(client, targetDate);

    if (!shadow.length) {
      console.log('No shadow markets pending.');
      return;
    }

    const eventIds = [...new Set(shadow.map((s) => s.betfair_event_id))];
    const markets = await loadBetfairMarkets(client, eventIds);

    const marketIds = markets.map((m) => m.betfair_market_id);
    const runners = await loadBetfairRunners(client, marketIds);

    const uniqueMarketIds = [...new Set(marketIds.filter(Boolean))];
    const marketIdChunks = chunkArray(uniqueMarketIds, 40);
    
    const books = [];
    
    for (let i = 0; i < marketIdChunks.length; i += 1) {
      console.log(`Fetching market books ${i + 1}/${marketIdChunks.length}`);
      const chunkBooks = await betfair.listMarketBook(marketIdChunks[i]);
      books.push(...chunkBooks);
    }
    
    const bookMap = new Map(books.map((b) => [b.marketId, b]));

    await client.query('BEGIN');

    let updated = 0;

    for (const s of shadow) {
      const market = findMatchingMarket(markets, s);
      if (!market) continue;

      const marketRunners = runners.filter(
        (r) => r.betfair_market_id === market.betfair_market_id
      );

      const runner = matchRunner(marketRunners, s);
      if (!runner) continue;

      const book = bookMap.get(market.betfair_market_id);
      if (!book || book.status !== 'CLOSED') continue;

      const bfRunner = book.runners.find(
        (r) => r.selectionId === runner.selection_id
      );

      if (!bfRunner) continue;

      const status = mapRunnerStatus(bfRunner.status);
      if (status === 'pending') continue;

      const pl = calcPL(status, s.synthetic_odds);

      await client.query(
        `
        UPDATE shadow_market_tests
        SET status = $1,
            profit_loss = $2,
            settled_at = NOW()
        WHERE id = $3
        `,
        [status, pl, s.id]
      );

      console.log(`Shadow ${s.id}: ${status} P/L ${pl}`);

      updated++;
    }

    await client.query('COMMIT');

    console.log(`Shadow settlement done: ${updated}`);
  } catch (err) {
    await client.query('ROLLBACK');
    console.error(err.message);
  } finally {
    client.release();
    await db.pool.end();
  }
}

main();
