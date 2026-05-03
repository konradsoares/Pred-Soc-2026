const db = require('../db/connection');

function todayDateISO() {
  return new Date().toISOString().slice(0, 10);
}

async function main() {
  const targetDate = process.argv[2] || todayDateISO();

  console.log(`Creating paper bets for ${targetDate}`);

  const client = await db.getClient();

  try {
    await client.query('BEGIN');

    const result = await client.query(
      `
      INSERT INTO paper_bets (
        sent_tip_id,
        sent_tip_leg_id,
        fixture_id,
        betfair_market_id,
        selection_id,
        runner_name,
        bet_type,
        market,
        pick,
        paper_odds,
        stake,
        raw
      )
      SELECT
        m.sent_tip_id,
        m.sent_tip_leg_id,
        m.fixture_id,
        m.betfair_market_id,
        m.selection_id,
        m.runner_name,
        st.bet_type,
        m.tip_market,
        m.tip_pick,
        COALESCE(l.odds, st.odds),
        CASE
          WHEN m.sent_tip_leg_id IS NULL THEN st.stake
          ELSE ROUND((st.stake / NULLIF(leg_count.total_legs, 0))::numeric, 2)
        END AS stake,
        jsonb_build_object(
          'mapping_id', m.id,
          'sent_tip_id', m.sent_tip_id,
          'sent_tip_leg_id', m.sent_tip_leg_id,
          'bet_type', st.bet_type
        )
      FROM betfair_tip_mappings m
      JOIN sent_tips st ON st.id = m.sent_tip_id
      JOIN sent_tip_batches b ON b.id = st.batch_id
      LEFT JOIN sent_tip_legs l ON l.id = m.sent_tip_leg_id
      LEFT JOIN (
        SELECT sent_tip_id, COUNT(*) AS total_legs
        FROM sent_tip_legs
        GROUP BY sent_tip_id
      ) leg_count ON leg_count.sent_tip_id = st.id
      WHERE b.tip_date = $1::date
        AND m.mapping_status = 'mapped'
      ON CONFLICT (sent_tip_id, sent_tip_leg_id)
      DO NOTHING
      RETURNING id
      `,
      [targetDate]
    );

    await client.query('COMMIT');

    console.log(`Paper bets created: ${result.rowCount}`);
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Create paper bets failed:', err.message);
    process.exitCode = 1;
  } finally {
    client.release();
    await db.pool.end();
  }
}

main();
