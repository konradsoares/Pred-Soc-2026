const db = require('../db/connection');
const { fetchFixturesByDate } = require('../scrapers/statareaFixtures');

function getTodayDate() {
  const d = new Date();
  return d.toISOString().slice(0, 10);
}

async function main() {
  const targetDate = process.argv[2] || getTodayDate();

  console.log(`\nINGEST DATE: ${targetDate}\n`);

  const fixtures = await fetchFixturesByDate(targetDate);

  if (!fixtures.length) {
    console.log('No fixtures fetched → STOP');
    return;
  }

  const client = await db.getClient();

  try {
    await client.query('BEGIN');

    for (const f of fixtures) {
      await client.query(
        `
        INSERT INTO fixtures (
          source_name,
          external_id,
          kickoff_utc,
          home_team_id,
          away_team_id,
          competition_id,
          country_id,
          status,
          compare_url,
          fixture_date
        )
        VALUES ($1,$2,$3,1,2,1,1,'scheduled',$4,$5)
        ON CONFLICT (source_name, external_id)
        DO UPDATE SET
          kickoff_utc = EXCLUDED.kickoff_utc,
          compare_url = EXCLUDED.compare_url,
          fixture_date = EXCLUDED.fixture_date,
          scraped_at = NOW()
        `,
        [
          f.source_name,
          f.external_id,
          f.kickoff_utc,
          f.compare_url,
          targetDate
        ]
      );
    }

    await client.query('COMMIT');

    console.log(`Saved ${fixtures.length} fixtures`);
  } catch (err) {
    await client.query('ROLLBACK');
    console.error(err);
  } finally {
    client.release();
  }
}

main();
