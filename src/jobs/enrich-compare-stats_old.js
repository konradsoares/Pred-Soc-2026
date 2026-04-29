const db = require('../db/connection');
const { fetchCompareStats } = require('../scrapers/statareaCompare');

const REQUEST_TIMEOUT_MS = 30000;
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 2500;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function todayDateISO() {
  return new Date().toISOString().slice(0, 10);
}

async function fetchCompareStatsWithRetry(compareUrl, maxRetries = MAX_RETRIES) {
  let lastError = null;

  for (let attempt = 1; attempt <= maxRetries; attempt += 1) {
    try {
      return await fetchCompareStats(compareUrl, { timeoutMs: REQUEST_TIMEOUT_MS });
    } catch (err) {
      lastError = err;
      const isLastAttempt = attempt === maxRetries;

      console.error(
        `Compare scrape failed for ${compareUrl} (attempt ${attempt}/${maxRetries}): ${err.message}`
      );

      if (!isLastAttempt) await sleep(RETRY_DELAY_MS * attempt);
    }
  }

  throw lastError;
}

async function loadFixturesToEnrich(client, targetDate) {
  const result = await client.query(
    `
    SELECT
      f.id AS fixture_id,
      f.compare_url,
      f.home_team_id,
      f.away_team_id,
      ht.name AS home_team_name,
      at.name AS away_team_name
    FROM fixtures f
    JOIN teams ht ON ht.id = f.home_team_id
    JOIN teams at ON at.id = f.away_team_id
    LEFT JOIN head_to_head_stats h2h
      ON h2h.fixture_id = f.id
     AND h2h.home_team_id = f.home_team_id
     AND h2h.away_team_id = f.away_team_id
     AND h2h.matches_considered = 10
    LEFT JOIN team_recent_stats trs_home
      ON trs_home.fixture_id = f.id
     AND trs_home.team_id = f.home_team_id
     AND trs_home.matches_considered = 10
    LEFT JOIN team_recent_stats trs_away
      ON trs_away.fixture_id = f.id
     AND trs_away.team_id = f.away_team_id
     AND trs_away.matches_considered = 10
    WHERE f.compare_url IS NOT NULL
      AND COALESCE(f.fixture_date, f.kickoff_utc::date) = $1::date
      AND (
        h2h.id IS NULL
        OR trs_home.id IS NULL
        OR trs_away.id IS NULL
      )
    ORDER BY f.kickoff_utc ASC, f.id ASC
    `,
    [targetDate]
  );

  return result.rows;
}

function pctToCount(value, base = 10) {
  if (value === null || value === undefined) return null;
  return Math.round(Number(value) / 100 * base);
}

function buildTeamRecentRow(fixtureId, teamId, stats) {
  const matchesConsidered = 10;

  return {
    fixture_id: fixtureId,
    team_id: teamId,
    matches_considered: matchesConsidered,
    wins: pctToCount(stats?.general_match_facts?.win_pct, matchesConsidered),
    draws: pctToCount(stats?.general_match_facts?.draw_pct, matchesConsidered),
    losses: pctToCount(stats?.general_match_facts?.opponent_win_pct, matchesConsidered),
    goals_for: null,
    goals_against: null,
    clean_sheets: null,
    failed_to_score: pctToCount(stats?.first_goal?.team_without_goal, matchesConsidered),
    btts: pctToCount(stats?.goal_characteristics?.both_score_pct, matchesConsidered),
    over_25: pctToCount(stats?.over_under?.all_goals_over_25, matchesConsidered)
  };
}

function buildH2HRow(fixtureId, homeTeamId, awayTeamId, h2h) {
  return {
    fixture_id: fixtureId,
    home_team_id: homeTeamId,
    away_team_id: awayTeamId,
    matches_considered: h2h?.matches_considered ?? 0,
    home_wins: h2h?.home_wins ?? 0,
    draws: h2h?.draws ?? 0,
    away_wins: h2h?.away_wins ?? 0,
    home_goals: h2h?.home_goals ?? 0,
    away_goals: h2h?.away_goals ?? 0
  };
}

async function upsertTeamRecentStats(client, row) {
  await client.query(
    `
    INSERT INTO team_recent_stats (
      fixture_id,
      team_id,
      matches_considered,
      wins,
      draws,
      losses,
      goals_for,
      goals_against,
      clean_sheets,
      failed_to_score,
      btts,
      over_25
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
    ON CONFLICT (fixture_id, team_id, matches_considered)
    DO UPDATE SET
      wins = EXCLUDED.wins,
      draws = EXCLUDED.draws,
      losses = EXCLUDED.losses,
      goals_for = EXCLUDED.goals_for,
      goals_against = EXCLUDED.goals_against,
      clean_sheets = EXCLUDED.clean_sheets,
      failed_to_score = EXCLUDED.failed_to_score,
      btts = EXCLUDED.btts,
      over_25 = EXCLUDED.over_25
    `,
    [
      row.fixture_id,
      row.team_id,
      row.matches_considered,
      row.wins,
      row.draws,
      row.losses,
      row.goals_for,
      row.goals_against,
      row.clean_sheets,
      row.failed_to_score,
      row.btts,
      row.over_25
    ]
  );
}

async function upsertHeadToHeadStats(client, row) {
  await client.query(
    `
    INSERT INTO head_to_head_stats (
      fixture_id,
      home_team_id,
      away_team_id,
      matches_considered,
      home_wins,
      draws,
      away_wins,
      home_goals,
      away_goals
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
    ON CONFLICT (fixture_id, home_team_id, away_team_id, matches_considered)
    DO UPDATE SET
      home_wins = EXCLUDED.home_wins,
      draws = EXCLUDED.draws,
      away_wins = EXCLUDED.away_wins,
      home_goals = EXCLUDED.home_goals,
      away_goals = EXCLUDED.away_goals
    `,
    [
      row.fixture_id,
      row.home_team_id,
      row.away_team_id,
      row.matches_considered,
      row.home_wins,
      row.draws,
      row.away_wins,
      row.home_goals,
      row.away_goals
    ]
  );
}

async function main() {
  const targetDate = process.argv[2] || todayDateISO();
  console.log(`Enrichment target date: ${targetDate}`);

  const client = await db.getClient();

  try {
    const fixtures = await loadFixturesToEnrich(client, targetDate);
    console.log(`Fixtures to enrich for ${targetDate}: ${fixtures.length}`);

    for (const fixture of fixtures) {
      console.log(`Enriching fixture ${fixture.fixture_id}: ${fixture.home_team_name} vs ${fixture.away_team_name}`);

      let compareStats;
      try {
        compareStats = await fetchCompareStatsWithRetry(fixture.compare_url);
      } catch (err) {
        console.error(`Skipping fixture ${fixture.fixture_id} after retries: ${err.message}`);
        continue;
      }

      const homeRecent = buildTeamRecentRow(
        fixture.fixture_id,
        fixture.home_team_id,
        compareStats.recent_form?.home
      );

      const awayRecent = buildTeamRecentRow(
        fixture.fixture_id,
        fixture.away_team_id,
        compareStats.recent_form?.away
      );

      const h2hRow = buildH2HRow(
        fixture.fixture_id,
        fixture.home_team_id,
        fixture.away_team_id,
        compareStats.h2h
      );

      try {
        await client.query('BEGIN');
        await upsertTeamRecentStats(client, homeRecent);
        await upsertTeamRecentStats(client, awayRecent);
        await upsertHeadToHeadStats(client, h2hRow);
        await client.query('COMMIT');
      } catch (err) {
        await client.query('ROLLBACK');
        console.error(`DB upsert failed for fixture ${fixture.fixture_id}: ${err.message}`);
      }

      await sleep(500);
    }
  } catch (err) {
    console.error(err);
    process.exitCode = 1;
  } finally {
    client.release();
    await db.pool.end();
  }
}

main();
