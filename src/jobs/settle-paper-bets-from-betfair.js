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
  if (status === 'REMOVED' || status === 'REMOVED_VACANT') return 'void';
  return 'pending';
}

function calcPL(status, stake, odds) {
  const s = Number(stake || 0);
  const o = Number(odds || 0);

  if (status === 'won') return Number((s * (o - 1)).toFixed(2));
  if (status === 'lost') return Number((-s).toFixed(2));
  if (status === 'void') return 0;

  return null;
}

async function loadPendingPaperBets(client, targetDate) {
  const res = await client.query(
    `
    SELECT pb.*
    FROM paper_bets pb
    JOIN sent_tips st ON st.id = pb.sent_tip_id
    JOIN sent_tip_batches b ON b.id = st.batch_id
    WHERE b.tip_date = $1::date
      AND pb.status = 'pending'
    `,
    [targetDate]
  );

  return res.rows;
}

async function fetchMarketBooks(betfair, marketIds) {
  const unique = [...new Set(marketIds.filter(Boolean))];
  const chunks = chunkArray(unique, 40);

  const books = [];

  for (let i = 0; i < chunks.length; i++) {
    console.log(`Fetching market books ${i + 1}/${chunks.length}`);
    const res = await betfair.listMarketBook(chunks[i]);
    books.push(...res);
  }

  return books;
}

function buildMarketMap(books) {
  const map = new Map();
  for (const b of books) {
    map.set(b.marketId, b);
  }
  return map;
}

async function main() {
  const targetDate = process.argv[2] || todayDateISO();

  console.log(`Settling paper bets for ${targetDate}`);

  const betfair = new BetfairClient();
  const client = await db.getClient();

  try {
    const bets = await loadPendingPaperBets(client, targetDate);

    if (!bets.length) {
      console.log('No pending paper bets.');
      return;
    }

    const books = await fetchMarketBooks(
      betfair,
      bets.map((b) => b.betfair_market_id)
    );

    const map = buildMarketMap(books);

    await client.query('BEGIN');

    let updated = 0;
    let pending = 0;

    for (const bet of bets) {
      const book = map.get(bet.betfair_market_id);

      if (!book || book.status !== 'CLOSED') {
        pending++;
        continue;
      }

      const runner = book.runners.find(
        (r) => Number(r.selectionId) === Number(bet.selection_id)
      );

      if (!runner) {
        console.log(`Runner not found for bet ${bet.id}`);
        continue;
      }

      const status = mapRunnerStatus(runner.status);

      if (status === 'pending') {
        pending++;
        continue;
      }

      const pl = calcPL(status, bet.stake, bet.paper_odds);

      await client.query(
        `
        UPDATE paper_bets
        SET status = $1,
            profit_loss = $2,
            settled_at = NOW()
        WHERE id = $3
        `,
        [status, pl, bet.id]
      );

      console.log(
        `Paper bet ${bet.id}: ${status} P/L ${pl} | ${bet.runner_name}`
      );

      updated++;
    }

    await client.query('COMMIT');

    console.log(`Finished. Updated: ${updated}, pending: ${pending}`);
  } catch (err) {
    await client.query('ROLLBACK');
    console.error(err.message);
  } finally {
    client.release();
    await db.pool.end();
  }
}

main();
