const db = require('../db/connection');
const { fetchFixturesByDate } = require('../scrapers/statareaFixtures');

function todayDateISO() {
  return new Date().toISOString().slice(0, 10);
}

async function getOrCreateCountry(client, name = 'Unknown') {
  const existing = await client.query(
    `SELECT id FROM countries WHERE name = $1`,
    [name]
  );

  if (existing.rows.length) return existing.rows[0].id;

  const inserted = await client.query(
    `INSERT INTO countries (name) VALUES ($1) RETURNING id`,
    [name]
  );

  return inserted.rows[0].id;
}

async function getOrCreateCompetition(
  client,
  countryId,
  name = 'Unknown Competition',
  type = 'league',
  sourceName = 'statarea'
) {
  const externalId = `${countryId}:${name}`;

  const existing = await client.query(
    `SELECT id
     FROM competitions
     WHERE source_name = $1
       AND external_id = $2`,
    [sourceName, externalId]
  );

  if (existing.rows.length) return existing.rows[0].id;

  const inserted = await client.query(
    `INSERT INTO competitions (source_name, external_id, country_id, name, type)
     VALUES ($1, $2, $3, $4, $5)
     RETURNING id`,
    [sourceName, externalId, countryId, name, type]
  );

  return inserted.rows[0].id;
}

async function getOrCreateSeason(client, competitionId, seasonLabel) {
  const existing = await client.query(
    `SELECT id
     FROM seasons
     WHERE competition_id = $1
       AND season_label = $2`,
    [competitionId, seasonLabel]
  );

  if (existing.rows.length) return existing.rows[0].id;

  const inserted = await client.query(
    `INSERT INTO seasons (competition_id, season_label)
     VALUES ($1, $2)
     RETURNING id`,
    [competitionId, seasonLabel]
  );

  return inserted.rows[0].id;
}

async function getOrCreateTeam(client, teamName, countryId, sourceName = 'statarea') {
  const externalId = `${countryId}:${teamName}`;

  const existing = await client.query(
    `SELECT id
     FROM teams
     WHERE source_name = $1
       AND external_id = $2`,
    [sourceName, externalId]
  );

  if (existing.rows.length) return existing.rows[0].id;

  const inserted = await client.query(
    `INSERT INTO teams (source_name, external_id, country_id, name)
     VALUES ($1, $2, $3, $4)
     RETURNING id`,
    [sourceName, externalId, countryId, teamName]
  );

  return inserted.rows[0].id;
}

async function upsertFixture(client, fixture, targetDate) {
  const fixtureDate = fixture.fixture_date || targetDate;
  const seasonLabel = String(new Date(fixture.kickoff_utc).getUTCFullYear());

  const countryId = await getOrCreateCountry(client, fixture.country || 'Unknown');

  const competitionId = await getOrCreateCompetition(
    client,
    countryId,
    fixture.competition || 'Unknown Competition',
    fixture.competition_type || 'league',
    fixture.source_name
  );

  const seasonId = await getOrCreateSeason(client, competitionId, seasonLabel);

  const homeTeamId = await getOrCreateTeam(
    client,
    fixture.home_team,
    countryId,
    fixture.source_name
  );

  const awayTeamId = await getOrCreateTeam(
    client,
    fixture.away_team,
    countryId,
    fixture.source_name
  );

  const inserted = await client.query(
    `INSERT INTO fixtures (
      source_name,
      external_id,
      competition_id,
      season_id,
      country_id,
      home_team_id,
      away_team_id,
      kickoff_utc,
      status,
      is_friendly,
      compare_url,
      fixture_date
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
    ON CONFLICT (source_name, home_team_id, away_team_id, fixture_date)
    DO UPDATE SET
      external_id = EXCLUDED.external_id,
      competition_id = EXCLUDED.competition_id,
      season_id = EXCLUDED.season_id,
      country_id = EXCLUDED.country_id,
      kickoff_utc = EXCLUDED.kickoff_utc,
      status = EXCLUDED.status,
      is_friendly = EXCLUDED.is_friendly,
      compare_url = EXCLUDED.compare_url,
      scraped_at = NOW()
    RETURNING id`,
    [
      fixture.source_name,
      fixture.external_id,
      competitionId,
      seasonId,
      countryId,
      homeTeamId,
      awayTeamId,
      fixture.kickoff_utc,
      'scheduled',
      fixture.is_friendly,
      fixture.compare_url || null,
      fixtureDate
    ]
  );

  const fixtureId = inserted.rows[0].id;

  await client.query(
    `INSERT INTO scraped_predictions (
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
      raw_payload = EXCLUDED.raw_payload,
      scraped_at = NOW()`,
    [
      fixtureId,
      fixture.source_name,
      fixture.tip || null,
      fixture.prob_home,
      fixture.prob_draw,
      fixture.prob_away,
      fixture.prob_over_25,
      fixture.prob_under_25,
      JSON.stringify(fixture.raw_payload || {})
    ]
  );

  return fixtureId;
}

async function main() {
  const targetDate = process.argv[2] || todayDateISO();

  console.log(`\nINGEST DATE: ${targetDate}\n`);

  const fixtures = await fetchFixturesByDate(targetDate);
  console.log(`Fetched ${fixtures.length} fixtures`);

  if (!fixtures.length) {
    console.log('No fixtures fetched.');
    return;
  }

  const client = await db.getClient();

  try {
    await client.query('BEGIN');

    for (const fixture of fixtures) {
      await upsertFixture(client, fixture, targetDate);
      console.log(`Saved: ${fixture.home_team} vs ${fixture.away_team}`);
    }

    await client.query('COMMIT');
    console.log('Done');
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Failed:', err.message);
    process.exitCode = 1;
    throw err;
  } finally {
    client.release();
    await db.pool.end();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
