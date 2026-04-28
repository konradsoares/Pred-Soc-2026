const db = require('../db/connection');
const { fetchFixturesByDate } = require('../scrapers/statareaFixtures');

function yesterdayDateISO() {
  const d = new Date();
  d.setDate(d.getDate() - 1);
  return d.toISOString().slice(0, 10);
}

function hasFinalScore(fixture) {
  return (
    fixture.home_goals !== null &&
    fixture.home_goals !== undefined &&
    fixture.away_goals !== null &&
    fixture.away_goals !== undefined
  );
}

async function findFixtureId(client, fixture, targetDate) {
  const byExternalId = await client.query(
    `
    SELECT id
    FROM fixtures
    WHERE source_name = $1
      AND external_id = $2
    LIMIT 1
    `,
    [fixture.source_name, fixture.external_id]
  );

  if (byExternalId.rows.length) {
    return byExternalId.rows[0].id;
  }

  const fallback = await client.query(
    `
    SELECT f.id
    FROM fixtures f
    JOIN teams ht ON ht.id = f.home_team_id
    JOIN teams at ON at.id = f.away_team_id
    WHERE f.source_name = $1
      AND COALESCE(f.fixture_date, f.kickoff_utc::date) = $2::date
      AND ht.name = $3
      AND at.name = $4
    LIMIT 1
    `,
    [
      fixture.source_name,
      targetDate,
      fixture.home_team,
      fixture.away_team
    ]
  );

  return fallback.rows[0]?.id || null;
}

async function upsertFixtureScore(client, fixtureId, fixture) {
  await client.query(
    `
    INSERT INTO fixture_scores (
      fixture_id,
      home_goals,
      away_goals,
      fulltime_home,
      fulltime_away,
      updated_at
    )
    VALUES ($1,$2,$3,$2,$3,NOW())
    ON CONFLICT (fixture_id)
    DO UPDATE SET
      home_goals = EXCLUDED.home_goals,
      away_goals = EXCLUDED.away_goals,
      fulltime_home = EXCLUDED.fulltime_home,
      fulltime_away = EXCLUDED.fulltime_away,
      updated_at = NOW()
    `,
    [
      fixtureId,
      fixture.home_goals,
      fixture.away_goals
    ]
  );

  await client.query(
    `
    UPDATE fixtures
    SET status = 'finished'
    WHERE id = $1
    `,
    [fixtureId]
  );
}

async function main() {
  const targetDate = process.argv[2] || yesterdayDateISO();

  console.log(`Updating results for ${targetDate}`);

  const scrapedFixtures = await fetchFixturesByDate(targetDate);
  const completedFixtures = scrapedFixtures.filter(hasFinalScore);

  console.log(`Scraped fixtures: ${scrapedFixtures.length}`);
  console.log(`Completed fixtures with scores: ${completedFixtures.length}`);

  const client = await db.getClient();

  try {
    await client.query('BEGIN');

    let updated = 0;
    let skipped = 0;

    for (const fixture of completedFixtures) {
      const fixtureId = await findFixtureId(client, fixture, targetDate);

      if (!fixtureId) {
        skipped += 1;
        console.log(`Skipped, fixture not found: ${fixture.home_team} vs ${fixture.away_team}`);
        continue;
      }

      await upsertFixtureScore(client, fixtureId, fixture);
      updated += 1;

      console.log(
        `Updated: ${fixture.home_team} ${fixture.home_goals}-${fixture.away_goals} ${fixture.away_team}`
      );
    }

    await client.query('COMMIT');

    console.log(`Results update finished. Updated: ${updated}, skipped: ${skipped}`);
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Results update failed:', err.message);
    process.exitCode = 1;
  } finally {
    client.release();
    await db.pool.end();
  }
}

main();
