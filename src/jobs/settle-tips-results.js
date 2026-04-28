const db = require('../db/connection');

function todayDateISO() {
  return new Date().toISOString().slice(0, 10);
}

function getMatchResult(score) {
  if (score.home_goals === null || score.away_goals === null) return null;

  if (score.home_goals > score.away_goals) return '1';
  if (score.home_goals < score.away_goals) return '2';
  return 'X';
}

function settleMarket(market, pick, score) {
  if (!score || score.home_goals === null || score.away_goals === null) {
    return 'pending';
  }

  const result = getMatchResult(score);
  const totalGoals = Number(score.home_goals) + Number(score.away_goals);

  if (market === '1X2') {
    return pick === result ? 'won' : 'lost';
  }

  if (market === 'double_chance') {
    if (pick === '1X') return ['1', 'X'].includes(result) ? 'won' : 'lost';
    if (pick === 'X2') return ['X', '2'].includes(result) ? 'won' : 'lost';
    if (pick === '12') return ['1', '2'].includes(result) ? 'won' : 'lost';
  }

  if (market === 'goals') {
    if (pick === 'over_0_5') return totalGoals > 0.5 ? 'won' : 'lost';
    if (pick === 'over_1_5') return totalGoals > 1.5 ? 'won' : 'lost';
    if (pick === 'over_2_5') return totalGoals > 2.5 ? 'won' : 'lost';

    if (pick === 'under_0_5') return totalGoals < 0.5 ? 'won' : 'lost';
    if (pick === 'under_1_5') return totalGoals < 1.5 ? 'won' : 'lost';
    if (pick === 'under_2_5') return totalGoals < 2.5 ? 'won' : 'lost';
  }

  return 'pending';
}

function calculateProfitLoss(status, stake, odds) {
  const s = Number(stake || 0);
  const o = Number(odds || 0);

  if (status === 'won') return Number((s * (o - 1)).toFixed(2));
  if (status === 'lost') return Number((-s).toFixed(2));
  if (status === 'void') return 0;

  return null;
}

async function loadPendingSingles(client, targetDate) {
  const result = await client.query(
    `
    SELECT
      st.id AS sent_tip_id,
      st.fixture_id,
      st.market,
      st.pick,
      st.odds,
      st.stake,
      fs.home_goals,
      fs.away_goals
    FROM sent_tips st
    JOIN sent_tip_batches b ON b.id = st.batch_id
    LEFT JOIN fixture_scores fs ON fs.fixture_id = st.fixture_id
    WHERE b.tip_date = $1::date
      AND st.bet_type = 'single'
      AND st.status = 'pending'
    ORDER BY st.id
    `,
    [targetDate]
  );

  return result.rows;
}

async function loadPendingAccumulatorLegs(client, targetDate) {
  const result = await client.query(
    `
    SELECT
      l.id AS leg_id,
      l.sent_tip_id,
      l.fixture_id,
      l.market,
      l.pick,
      l.odds,
      fs.home_goals,
      fs.away_goals
    FROM sent_tip_legs l
    JOIN sent_tips st ON st.id = l.sent_tip_id
    JOIN sent_tip_batches b ON b.id = st.batch_id
    LEFT JOIN fixture_scores fs ON fs.fixture_id = l.fixture_id
    WHERE b.tip_date = $1::date
      AND st.bet_type = 'accumulator'
      AND l.status = 'pending'
    ORDER BY l.sent_tip_id, l.id
    `,
    [targetDate]
  );

  return result.rows;
}

async function loadPendingAccumulators(client, targetDate) {
  const result = await client.query(
    `
    SELECT
      st.id,
      st.odds,
      st.stake,
      COUNT(l.id) AS total_legs,
      COUNT(*) FILTER (WHERE l.status = 'won') AS won_legs,
      COUNT(*) FILTER (WHERE l.status = 'lost') AS lost_legs,
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

  return result.rows;
}

async function settleSingles(client, targetDate) {
  const singles = await loadPendingSingles(client, targetDate);

  for (const single of singles) {
    const status = settleMarket(
      single.market,
      single.pick,
      {
        home_goals: single.home_goals,
        away_goals: single.away_goals
      }
    );

    if (status === 'pending') continue;

    const profitLoss = calculateProfitLoss(status, single.stake, single.odds);

    await client.query(
      `
      UPDATE sent_tips
      SET status = $1,
          profit_loss = $2
      WHERE id = $3
      `,
      [status, profitLoss, single.sent_tip_id]
    );

    console.log(`Single ${single.sent_tip_id}: ${status} P/L ${profitLoss}`);
  }
}

async function settleAccumulatorLegs(client, targetDate) {
  const legs = await loadPendingAccumulatorLegs(client, targetDate);

  for (const leg of legs) {
    const status = settleMarket(
      leg.market,
      leg.pick,
      {
        home_goals: leg.home_goals,
        away_goals: leg.away_goals
      }
    );

    if (status === 'pending') continue;

    await client.query(
      `
      UPDATE sent_tip_legs
      SET status = $1
      WHERE id = $2
      `,
      [status, leg.leg_id]
    );

    console.log(`Leg ${leg.leg_id}: ${status}`);
  }
}

async function settleAccumulators(client, targetDate) {
  const accas = await loadPendingAccumulators(client, targetDate);

  for (const acca of accas) {
    let status = 'pending';

    if (Number(acca.lost_legs) > 0) {
      status = 'lost';
    } else if (
      Number(acca.pending_legs) === 0 &&
      Number(acca.won_legs) === Number(acca.total_legs)
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
  console.log(`Settling tips for ${targetDate}`);

  const client = await db.getClient();

  try {
    await client.query('BEGIN');

    await settleSingles(client, targetDate);
    await settleAccumulatorLegs(client, targetDate);
    await settleAccumulators(client, targetDate);

    await client.query('COMMIT');
    console.log('Settlement finished.');
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Settlement failed:', err.message);
    process.exitCode = 1;
  } finally {
    client.release();
    await db.pool.end();
  }
}

main();
