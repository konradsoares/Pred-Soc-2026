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

function normalizeUrl(value) {
  return String(value || '')
    .replace(/^https?:\/\/www\./i, 'http://www.')
    .replace(/\+/g, '%20')
    .trim();
}

async function findFixtureId(client, fixture, targetDate) {
  if (fixture.compare_url) {
    const byCompareUrl = await client.query(
      `
      SELECT id
      FROM fixtures
      WHERE fixture_date = $1::date
        AND compare_url IS NOT NULL
        AND replace(compare_url, '+', '%20') = $2
      LIMIT 1
      `,
      [targetDate, normalizeUrl(fixture.compare_url)]
    );

    if (byCompareUrl.rows.length) {
      return byCompareUrl.rows[0].id;
    }
  }

  return null;
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
