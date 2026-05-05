const db = require('../db/connection');

function parseTeamsFromBetfairEventName(eventName) {
  if (!eventName || typeof eventName !== 'string') {
    return null;
  }

  const separators = [' v ', ' vs ', ' - '];

  for (const separator of separators) {
    if (eventName.includes(separator)) {
      const parts = eventName.split(separator).map((v) => v.trim());

      if (parts.length === 2 && parts[0] && parts[1]) {
        return {
          homeTeam: parts[0],
          awayTeam: parts[1],
        };
      }
    }
  }

  return null;
}

async function findFixtureForBetfairEvent(eventId, eventName) {
  const existing = await db.query(
    `
    SELECT
      f.id AS fixture_id,
      ht.name AS home_team,
      at.name AS away_team,
      f.compare_url
    FROM betfair_events be
    JOIN fixtures f ON f.id = be.fixture_id
    LEFT JOIN teams ht ON ht.id = f.home_team_id
    LEFT JOIN teams at ON at.id = f.away_team_id
    WHERE be.betfair_event_id = $1
    LIMIT 1
    `,
    [eventId]
  );

  if (existing.rows.length) {
    return {
      fixtureId: existing.rows[0].fixture_id,
      homeTeam: existing.rows[0].home_team,
      awayTeam: existing.rows[0].away_team,
      compareUrl: existing.rows[0].compare_url,
      mappingConfidence: 'high',
      mappingMethod: 'betfair_events.fixture_id',
    };
  }

  const parsed = parseTeamsFromBetfairEventName(eventName);

  if (!parsed) {
    return {
      fixtureId: null,
      homeTeam: null,
      awayTeam: null,
      compareUrl: null,
      mappingConfidence: 'none',
      mappingMethod: 'failed_parse',
    };
  }

  const byNames = await db.query(
    `
    SELECT
      f.id AS fixture_id,
      ht.name AS home_team,
      at.name AS away_team,
      f.compare_url
    FROM fixtures f
    JOIN teams ht ON ht.id = f.home_team_id
    JOIN teams at ON at.id = f.away_team_id
    WHERE LOWER(ht.name) = LOWER($1)
      AND LOWER(at.name) = LOWER($2)
    ORDER BY f.id DESC
    LIMIT 1
    `,
    [parsed.homeTeam, parsed.awayTeam]
  );

  if (byNames.rows.length) {
    return {
      fixtureId: byNames.rows[0].fixture_id,
      homeTeam: byNames.rows[0].home_team,
      awayTeam: byNames.rows[0].away_team,
      compareUrl: byNames.rows[0].compare_url,
      mappingConfidence: 'high',
      mappingMethod: 'parsed_name_exact_match',
    };
  }

  return {
    fixtureId: null,
    homeTeam: parsed.homeTeam,
    awayTeam: parsed.awayTeam,
    compareUrl: null,
    mappingConfidence: 'low',
    mappingMethod: 'parsed_only',
  };
}

async function getRunnerNameFromCatalogue(marketCatalogue, selectionId) {
  if (!marketCatalogue || !Array.isArray(marketCatalogue.runners)) {
    return null;
  }

  const runner = marketCatalogue.runners.find(
    (r) => String(r.selectionId) === String(selectionId)
  );

  return runner ? runner.runnerName : null;
}

module.exports = {
  parseTeamsFromBetfairEventName,
  findFixtureForBetfairEvent,
  getRunnerNameFromCatalogue,
};
