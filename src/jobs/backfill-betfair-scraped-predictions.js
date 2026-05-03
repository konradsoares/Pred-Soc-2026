const db = require('../db/connection');
const { fetchFixturesByDate } = require('../scrapers/statareaFixtures');

const SOURCE_NAME = 'betfair';

function todayDateISO() {
  return new Date().toISOString().slice(0, 10);
}

function normalizeUrl(value) {
  return String(value || '')
    .replace(/^https?:\/\/www\./i, 'http://www.')
    .replace(/\+/g, '%20')
    .trim();
}

async function loadMatchedBetfairFixtures(client, targetDate) {
  const result = await client.query(
    `
    SELECT
      f.id AS fixture_id,
      f.compare_url,
      ht.name AS home_team,
      at.name AS away_team
    FROM fixtures f
    JOIN teams ht ON ht.id = f.home_team_id
    JOIN teams at ON at.id = f.away_team_id
    LEFT JOIN scraped_predictions sp
      ON sp.fixture_id = f.id
     AND sp.source_name = f.source_name
    WHERE f.source_name = $1
      AND f.fixture_date = $2::date
      AND f.compare_url IS NOT NULL
      AND (
        sp.id IS NULL
        OR sp.prob_home IS NULL
        OR sp.prob_draw IS NULL
        OR sp.prob_away IS NULL
      )
    ORDER BY f.kickoff_utc ASC, f.id ASC
    `,
    [SOURCE_NAME, targetDate]
  );

  return result.rows;
}

async function upsertScrapedPrediction(client, fixtureId, statareaFixture) {
  await client.query(
    `
    INSERT INTO scraped_predictions (
      fixture_id,
      source_name,
      tip,
      prob_home,
      prob_draw,
      prob_away,
      prob_over_25,
      prob_under_25,
      raw_payload
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
    ON CONFLICT (fixture_id, source_name)
    DO UPDATE SET
      tip = EXCLUDED.tip,
      prob_home = EXCLUDED.prob_home,
      prob_draw = EXCLUDED.prob_draw,
      prob_away = EXCLUDED.prob_away,
      prob_over_25 = EXCLUDED.prob_over_25,
      prob_under_25 = EXCLUDED.prob_under_25,
      raw_payload = EXCLUDED.raw_payload
    `,
    [
      fixtureId,
      SOURCE_NAME,
      statareaFixture.tip || null,
      statareaFixture.prob_home || null,
      statareaFixture.prob_draw || null,
      statareaFixture.prob_away || null,
      statareaFixture.prob_over_25 || null,
      statareaFixture.prob_under_25 || null,
      JSON.stringify(statareaFixture)
    ]
  );
}

async function main() {
  const targetDate = process.argv[2] || todayDateISO();

  console.log(`Backfilling Betfair scraped_predictions for ${targetDate}`);

  console.log('Scraping Statarea fixtures...');
  const statareaFixtures = await fetchFixturesByDate(targetDate);

  const byCompareUrl = new Map();

  for (const fixture of statareaFixtures) {
    if (!fixture.compare_url) continue;
    byCompareUrl.set(normalizeUrl(fixture.compare_url), fixture);
  }

  console.log(`Statarea fixtures indexed by compare_url: ${byCompareUrl.size}`);

  const client = await db.getClient();

  try {
    await client.query('BEGIN');

    const fixtures = await loadMatchedBetfairFixtures(client, targetDate);

    console.log(`Matched Betfair fixtures needing predictions: ${fixtures.length}`);

    let updated = 0;
    let skipped = 0;

    for (const fixture of fixtures) {
      const statareaFixture = byCompareUrl.get(normalizeUrl(fixture.compare_url));

      if (!statareaFixture) {
        skipped += 1;
        console.log(`Skipped, compare_url not found in Statarea scrape: ${fixture.home_team} v ${fixture.away_team}`);
        continue;
      }

      await upsertScrapedPrediction(client, fixture.fixture_id, statareaFixture);

      updated += 1;

      console.log(
        `Updated predictions: ${fixture.home_team} v ${fixture.away_team} ` +
        `H=${statareaFixture.prob_home} D=${statareaFixture.prob_draw} A=${statareaFixture.prob_away} ` +
        `O2.5=${statareaFixture.prob_over_25} U2.5=${statareaFixture.prob_under_25}`
      );
    }

    await client.query('COMMIT');

    console.log(`Backfill finished. Updated: ${updated}, skipped: ${skipped}`);
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Backfill failed:', err.message);
    process.exitCode = 1;
  } finally {
    client.release();
    await db.pool.end();
  }
}

main();
