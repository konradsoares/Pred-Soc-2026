const db = require('../db/connection');

const SOURCE_NAME = 'betfair';

function todayDateISO() {
  return new Date().toISOString().slice(0, 10);
}

function normalizeText(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function detectCompetitionType(name) {
  const n = normalizeText(name).toLowerCase();

  if (!n) return 'league';
  if (n.includes('friendly')) return 'friendly';
  if (n.includes('cup')) return 'cup';
  if (
    n.includes('playoff') ||
    n.includes('play-offs') ||
    n.includes('tournament') ||
    n.includes('super cup') ||
    n.includes('supercup')
  ) {
    return 'tournament';
  }

  return 'league';
}

function parseBetfairTeams(eventName) {
  const name = normalizeText(eventName);

  const separators = [
    ' v ',
    ' vs ',
    ' - '
  ];

  for (const sep of separators) {
    if (name.includes(sep)) {
      const parts = name.split(sep).map(normalizeText).filter(Boolean);

      if (parts.length === 2) {
        return {
          home_team: parts[0],
          away_team: parts[1]
        };
      }
    }
  }

  return null;
}

async function getOrCreateCountry(client, countryCode) {
  const name = countryCode || 'Unknown';

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

async function getOrCreateCompetition(client, countryId, event) {
  const name = event.competition_name || 'Unknown Competition';
  const externalId = event.competition_id || `${countryId}:${name}`;
  const type = detectCompetitionType(name);

  const existing = await client.query(
    `
    SELECT id
    FROM competitions
    WHERE source_name = $1
      AND external_id = $2
    `,
    [SOURCE_NAME, externalId]
  );

  if (existing.rows.length) return existing.rows[0].id;

  const inserted = await client.query(
    `
    INSERT INTO competitions (
      source_name,
      external_id,
      country_id,
      name,
      type
    )
    VALUES ($1,$2,$3,$4,$5)
    RETURNING id
    `,
    [SOURCE_NAME, externalId, countryId, name, type]
  );

  return inserted.rows[0].id;
}

async function getOrCreateSeason(client, competitionId, kickoffUtc) {
  const seasonLabel = String(new Date(kickoffUtc).getUTCFullYear());

  const existing = await client.query(
    `
    SELECT id
    FROM seasons
    WHERE competition_id = $1
      AND season_label = $2
    `,
    [competitionId, seasonLabel]
  );

  if (existing.rows.length) return existing.rows[0].id;

  const inserted = await client.query(
    `
    INSERT INTO seasons (
      competition_id,
      season_label
    )
    VALUES ($1,$2)
    RETURNING id
    `,
    [competitionId, seasonLabel]
  );

  return inserted.rows[0].id;
}

async function getOrCreateTeam(client, teamName, countryId) {
  const externalId = `${countryId}:${teamName}`;

  const existing = await client.query(
    `
    SELECT id
    FROM teams
    WHERE source_name = $1
      AND external_id = $2
    `,
    [SOURCE_NAME, externalId]
  );

  if (existing.rows.length) return existing.rows[0].id;

  const inserted = await client.query(
    `
    INSERT INTO teams (
      source_name,
      external_id,
      country_id,
      name
    )
    VALUES ($1,$2,$3,$4)
    RETURNING id
    `,
    [SOURCE_NAME, externalId, countryId, teamName]
  );

  return inserted.rows[0].id;
}

async function loadBetfairEvents(client, targetDate) {
  const result = await client.query(
    `
    SELECT
      betfair_event_id,
      event_name,
      country_code,
      timezone,
      open_date,
      competition_id,
      competition_name,
      raw
    FROM betfair_events
    WHERE open_date::date = $1::date
    ORDER BY open_date ASC, event_name ASC
    `,
    [targetDate]
  );

  return result.rows;
}

async function upsertFixture(client, event, parsedTeams) {
  const countryId = await getOrCreateCountry(client, event.country_code || 'Unknown');
  const competitionId = await getOrCreateCompetition(client, countryId, event);
  const seasonId = await getOrCreateSeason(client, competitionId, event.open_date);

  const homeTeamId = await getOrCreateTeam(
    client,
    parsedTeams.home_team,
    countryId
  );

  const awayTeamId = await getOrCreateTeam(
    client,
    parsedTeams.away_team,
    countryId
  );

  const fixtureDate = new Date(event.open_date).toISOString().slice(0, 10);

  const result = await client.query(
    `
    INSERT INTO fixtures (
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
      scraped_at = NOW()
    RETURNING id
    `,
    [
      SOURCE_NAME,
      event.betfair_event_id,
      competitionId,
      seasonId,
      countryId,
      homeTeamId,
      awayTeamId,
      event.open_date,
      'scheduled',
      detectCompetitionType(event.competition_name) === 'friendly',
      null,
      fixtureDate
    ]
  );

  return result.rows[0].id;
}

async function main() {
  const targetDate = process.argv[2] || todayDateISO();

  console.log(`Syncing Betfair events to fixtures for ${targetDate}`);

  const client = await db.getClient();

  try {
    await client.query('BEGIN');

    const events = await loadBetfairEvents(client, targetDate);

    console.log(`Betfair events found: ${events.length}`);

    let synced = 0;
    let skipped = 0;

    for (const event of events) {
      const parsedTeams = parseBetfairTeams(event.event_name);

      if (!parsedTeams) {
        skipped += 1;
        console.log(`Skipped, cannot parse teams: ${event.event_name}`);
        continue;
      }

      const fixtureId = await upsertFixture(client, event, parsedTeams);

      synced += 1;

      console.log(
        `Synced fixture ${fixtureId}: ${parsedTeams.home_team} v ${parsedTeams.away_team}`
      );
    }

    await client.query('COMMIT');

    console.log(`Sync finished. Synced: ${synced}, skipped: ${skipped}`);
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Sync failed:', err.message);
    process.exitCode = 1;
  } finally {
    client.release();
    await db.pool.end();
  }
}

main();
