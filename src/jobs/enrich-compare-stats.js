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

function num(value) {
  if (value === null || value === undefined) return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function pctToCount(value, base = 10) {
  const n = num(value);
  if (n === null) return null;
  return Math.round((n / 100) * base);
}

function firstNumber(...values) {
  for (const value of values) {
    const n = num(value);
    if (n !== null) return n;
  }
  return null;
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
        OR trs_home.raw_payload IS NULL
        OR trs_away.raw_payload IS NULL
        OR trs_home.avg_goals_for IS NULL
        OR trs_away.avg_goals_for IS NULL
        OR trs_home.under_25 IS NULL
        OR trs_away.under_25 IS NULL
      )
    ORDER BY f.kickoff_utc ASC, f.id ASC
    `,
    [targetDate]
  );

  return result.rows;
}

function buildTeamRecentRow(fixtureId, teamId, stats) {
  const matchesConsidered = 10;
  const overUnder = stats?.over_under || {};
  const general = stats?.general_match_facts || {};
  const goals = stats?.goal_characteristics || {};
  const firstGoal = stats?.first_goal || {};

  const allOver25 = firstNumber(
    overUnder.all_goals_over_25,
    overUnder.over_25,
    stats?.over_25_pct
  );

  const allUnder25 = firstNumber(
    overUnder.all_goals_under_25,
    overUnder.under_25,
    stats?.under_25_pct
  );

  return {
    fixture_id: fixtureId,
    team_id: teamId,
    matches_considered: matchesConsidered,

    wins: pctToCount(general.win_pct, matchesConsidered),
    draws: pctToCount(general.draw_pct, matchesConsidered),
    losses: pctToCount(general.opponent_win_pct, matchesConsidered),

    goals_for: firstNumber(stats?.goals_for, stats?.total_goals_for),
    goals_against: firstNumber(stats?.goals_against, stats?.total_goals_against),
    clean_sheets: firstNumber(stats?.clean_sheets),
    failed_to_score: pctToCount(firstGoal.team_without_goal, matchesConsidered),
    btts: pctToCount(goals.both_score_pct, matchesConsidered),

    over_25: pctToCount(allOver25, matchesConsidered),
    under_25: pctToCount(allUnder25, matchesConsidered),

    avg_goals_for: firstNumber(stats?.avg_goals_for, stats?.average_scored_goals_per_match),
    avg_goals_against: firstNumber(stats?.avg_goals_against, stats?.average_conceded_goals_per_match),
    chance_score_next_pct: firstNumber(stats?.chance_score_next_pct, stats?.chance_to_score_goal_next_match),
    chance_concede_next_pct: firstNumber(stats?.chance_concede_next_pct, stats?.chance_to_concede_goal_next_match),

    over_15_matches: pctToCount(overUnder.all_goals_over_15, matchesConsidered),
    under_15_matches: pctToCount(overUnder.all_goals_under_15, matchesConsidered),
    over_35_matches: pctToCount(overUnder.all_goals_over_35, matchesConsidered),
    under_35_matches: pctToCount(overUnder.all_goals_under_35, matchesConsidered),

    time_without_scored_goal_min: firstNumber(stats?.time_without_scored_goal_min),
    time_without_conceded_goal_min: firstNumber(stats?.time_without_conceded_goal_min),

    raw_payload: stats || {}
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
      over_25,
      avg_goals_for,
      avg_goals_against,
      chance_score_next_pct,
      chance_concede_next_pct,
      under_25,
      over_15_matches,
      under_15_matches,
      over_35_matches,
      under_35_matches,
      time_without_scored_goal_min,
      time_without_conceded_goal_min,
      raw_payload
    )
    VALUES (
      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,
      $13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24
    )
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
      over_25 = EXCLUDED.over_25,
      avg_goals_for = EXCLUDED.avg_goals_for,
      avg_goals_against = EXCLUDED.avg_goals_against,
      chance_score_next_pct = EXCLUDED.chance_score_next_pct,
      chance_concede_next_pct = EXCLUDED.chance_concede_next_pct,
      under_25 = EXCLUDED.under_25,
      over_15_matches = EXCLUDED.over_15_matches,
      under_15_matches = EXCLUDED.under_15_matches,
      over_35_matches = EXCLUDED.over_35_matches,
      under_35_matches = EXCLUDED.under_35_matches,
      time_without_scored_goal_min = EXCLUDED.time_without_scored_goal_min,
      time_without_conceded_goal_min = EXCLUDED.time_without_conceded_goal_min,
      raw_payload = EXCLUDED.raw_payload
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
      row.over_25,
      row.avg_goals_for,
      row.avg_goals_against,
      row.chance_score_next_pct,
      row.chance_concede_next_pct,
      row.under_25,
      row.over_15_matches,
      row.under_15_matches,
      row.over_35_matches,
      row.under_35_matches,
      row.time_without_scored_goal_min,
      row.time_without_conceded_goal_min,
      JSON.stringify(row.raw_payload || {})
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
