const fs = require('fs');
const path = require('path');
const OpenAI = require('openai');
const db = require('../db/connection');
const env = require('../config/env');
const { buildMarkets } = require('../lib/odds');

const ROOT_DIR = path.resolve(__dirname, '../..');
const CONFIG_PATH = path.join(ROOT_DIR, 'config', 'app.config.json');
const OUTPUT_DIR = path.join(ROOT_DIR, 'output');

const WINDOW = process.argv[2] || process.env.PREDICTION_WINDOW || 'morning';

const WINDOWS = {
  morning:   ['00:00', '11:59'],
  noon:      ['12:00', '15:59'],
  afternoon: ['16:00', '19:59'],
  evening:   ['20:00', '23:59']
};

if (!WINDOWS[WINDOW]) {
  throw new Error(`Invalid window: ${WINDOW}`);
}

const [WINDOW_START, WINDOW_END] = WINDOWS[WINDOW];

function loadConfig() {
  return JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
}

function ensureOutputDir() {
  fs.mkdirSync(OUTPUT_DIR, { recursive: true });
}

function todayDateISO() {
  const d = new Date();
  d.setDate(d.getDate());
  return d.toISOString().slice(0, 10);
}

function toNumber(value) {
  if (value === null || value === undefined) return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function uniqueByFixtureAndMarket(markets) {
  const map = new Map();
  for (const market of markets) {
    const key = `${market.market}|${market.pick}`;
    if (!map.has(key)) map.set(key, market);
  }
  return [...map.values()];
}

function filterMarkets(markets, config) {
  const minOdds = config.prediction.min_single_pick_odds;
  const maxOdds = config.prediction.max_single_pick_odds;
  const minConfidence = config.prediction.min_model_confidence * 100;

  return markets.filter((m) => {
    if (!m.odds || !m.prob) return false;
    if (m.odds < minOdds || m.odds > maxOdds) return false;
    if (m.prob < minConfidence) return false;
    return true;
  });
}

// function buildAccumulatorCandidates(fixtures, config) {
//   const maxLegs = config.prediction.max_acca_legs || 3;
//   const minTotalOdds = config.prediction.min_acca_total_odds || 3;
//   const maxTotalOdds = config.prediction.max_acca_total_odds || 5;

//   const picks = [];

//   for (const fixture of fixtures) {
//     for (const market of fixture.available_markets || []) {
//       picks.push({
//         fixture_id: fixture.fixture_id,
//         home_team: fixture.home_team,
//         away_team: fixture.away_team,
//         country: fixture.country,
//         competition: fixture.competition,
//         market: market.market,
//         pick: market.pick,
//         prob: market.prob,
//         odds: market.odds
//       });
//     }
//   }

//   const candidates = [];

//   for (let i = 0; i < picks.length; i += 1) {
//     for (let j = i + 1; j < picks.length; j += 1) {
//       if (picks[i].fixture_id === picks[j].fixture_id) continue;

//       const totalOdds2 = Number((picks[i].odds * picks[j].odds).toFixed(2));
//       if (totalOdds2 >= minTotalOdds && totalOdds2 <= maxTotalOdds) {
//         candidates.push({
//           name: `acca_2_${picks[i].fixture_id}_${picks[j].fixture_id}`,
//           legs: [picks[i], picks[j]],
//           total_odds: totalOdds2
//         });
//       }

//       if (maxLegs >= 3) {
//         for (let k = j + 1; k < picks.length; k += 1) {
//           if (
//             picks[i].fixture_id === picks[k].fixture_id ||
//             picks[j].fixture_id === picks[k].fixture_id
//           ) {
//             continue;
//           }

//           const totalOdds3 = Number((picks[i].odds * picks[j].odds * picks[k].odds).toFixed(2));
//           if (totalOdds3 >= minTotalOdds && totalOdds3 <= maxTotalOdds) {
//             candidates.push({
//               name: `acca_3_${picks[i].fixture_id}_${picks[j].fixture_id}_${picks[k].fixture_id}`,
//               legs: [picks[i], picks[j], picks[k]],
//               total_odds: totalOdds3
//             });
//           }
//         }
//       }
//     }
//   }

//   return candidates.slice(0, 150);
// }
function marketRiskScore(market) {
  let risk = 0;

  if (market.market === '1X2') risk += 3;
  if (market.market === 'goals' && String(market.pick).startsWith('over_2_5')) risk += 3;
  if (market.market === 'goals' && String(market.pick).startsWith('under_2_5')) risk += 3;
  if (market.market === 'double_chance') risk += 1;
  if (market.market === 'goals' && String(market.pick).includes('over_1_5')) risk += 1;

  if (Number(market.prob || 0) < 65) risk += 2;
  if (Number(market.odds || 0) > 1.75) risk += 2;

  return risk;
}

function isLowVarianceMarket(market) {
  return (
    market.market === 'double_chance' ||
    (market.market === 'goals' && ['over_1_5', 'under_3_5'].includes(market.pick))
  );
}

function comboHasBadCorrelation(legs) {
  const competitions = new Set(legs.map((l) => l.competition));
  const countries = new Set(legs.map((l) => l.country));

  if (legs.length >= 3 && competitions.size === 1) return true;
  if (legs.length >= 3 && countries.size === 1) return true;

  const highRiskCount = legs.filter((l) => marketRiskScore(l) >= 4).length;
  if (highRiskCount >= 2) return true;

  const lowVarianceCount = legs.filter(isLowVarianceMarket).length;
  if (lowVarianceCount === 0) return true;

  return false;
}

function scoreAccumulator(legs, totalOdds) {
  const avgProb =
    legs.reduce((sum, l) => sum + Number(l.prob || 0), 0) / legs.length;

  const totalRisk = legs.reduce((sum, l) => sum + marketRiskScore(l), 0);

  return Number((avgProb - totalRisk * 3 + totalOdds).toFixed(2));
}

function buildAccumulatorCandidates(fixtures, config) {
  const maxLegs = config.prediction.max_acca_legs || 3;
  const minTotalOdds = config.prediction.min_acca_total_odds || 3;
  const maxTotalOdds = config.prediction.max_acca_total_odds || 5;

  const picks = [];

  for (const fixture of fixtures) {
    for (const market of fixture.available_markets || []) {
      if (Number(market.prob || 0) < 64) continue;

      picks.push({
        fixture_id: fixture.fixture_id,
        home_team: fixture.home_team,
        away_team: fixture.away_team,
        country: fixture.country,
        competition: fixture.competition,
        market: market.market,
        pick: market.pick,
        prob: market.prob,
        odds: market.odds,
        reason_tags: market.reason_tags || []
      });
    }
  }

  const candidates = [];

  for (let i = 0; i < picks.length; i += 1) {
    for (let j = i + 1; j < picks.length; j += 1) {
      if (picks[i].fixture_id === picks[j].fixture_id) continue;

      const legs2 = [picks[i], picks[j]];
      const totalOdds2 = Number((picks[i].odds * picks[j].odds).toFixed(2));

      if (
        totalOdds2 >= minTotalOdds &&
        totalOdds2 <= maxTotalOdds &&
        !comboHasBadCorrelation(legs2)
      ) {
        candidates.push({
          name: `smart_double_${picks[i].fixture_id}_${picks[j].fixture_id}`,
          legs: legs2,
          total_odds: totalOdds2,
          model_score: scoreAccumulator(legs2, totalOdds2)
        });
      }

      if (maxLegs >= 3) {
        for (let k = j + 1; k < picks.length; k += 1) {
          if (
            picks[i].fixture_id === picks[k].fixture_id ||
            picks[j].fixture_id === picks[k].fixture_id
          ) {
            continue;
          }

          const legs3 = [picks[i], picks[j], picks[k]];
          const totalOdds3 = Number((picks[i].odds * picks[j].odds * picks[k].odds).toFixed(2));

          if (
            totalOdds3 >= minTotalOdds &&
            totalOdds3 <= maxTotalOdds &&
            !comboHasBadCorrelation(legs3)
          ) {
            candidates.push({
              name: `smart_treble_${picks[i].fixture_id}_${picks[j].fixture_id}_${picks[k].fixture_id}`,
              legs: legs3,
              total_odds: totalOdds3,
              model_score: scoreAccumulator(legs3, totalOdds3)
            });
          }
        }
      }
    }
  }

  return candidates
    .sort((a, b) => b.model_score - a.model_score)
    .slice(0, 50);
}
function applyFootballRules(fixture, markets) {
  const home = fixture.recent_form?.home || {};
  const away = fixture.recent_form?.away || {};

  const strongHomeScoring =
    Number(home.avg_goals_for || 0) >= 2 &&
    Number(home.wins || 0) >= 6 &&
    Number(home.chance_score_next_pct || 0) >= 80;

  const awayInconsistent =
    Number(away.draws || 0) >= 3 &&
    Number(away.losses || 0) >= 2 &&
    Number(away.avg_goals_for || 0) < 2;

  const homeSignal = strongHomeScoring && awayInconsistent;

  return markets
    .map((m) => {
      const market = { ...m };

      if (homeSignal) {
        if (market.market === '1X2' && market.pick === '1') {
          market.prob = Math.min(95, Number(market.prob || 0) + 8);
          market.reason_tags = [
            ...(market.reason_tags || []),
            'strong_home_scoring',
            'away_inconsistency'
          ];
        }

        if (market.market === 'double_chance' && market.pick === '1X') {
          market.prob = Math.min(95, Number(market.prob || 0) + 5);
          market.reason_tags = [
            ...(market.reason_tags || []),
            'strong_home_scoring',
            'away_inconsistency'
          ];
        }

        if (market.market === 'double_chance' && market.pick === 'X2') {
          market.prob = Math.max(1, Number(market.prob || 0) - 12);
          market.reason_tags = [
            ...(market.reason_tags || []),
            'penalized_away_inconsistent'
          ];
        }
      }

      return market;
    })
    .filter((market) => {
      const hasStrongSignal =
        (market.reason_tags || []).includes('strong_home_scoring') ||
        (market.reason_tags || []).includes('away_inconsistency');

      const isLazyDoubleChance =
        market.market === 'double_chance' &&
        Number(market.prob || 0) < 65 &&
        !hasStrongSignal;

      return !isLazyDoubleChance;
    });
}

async function loadTodayDataset(client, targetDate) {
  const result = await client.query(
    `
    SELECT
      f.id AS fixture_id,
      f.kickoff_utc,
      f.compare_url,
      c.name AS competition,
      co.name AS country,
      ht.name AS home_team,
      at.name AS away_team,
      sp.tip AS source_tip,
      sp.prob_home,
      sp.prob_draw,
      sp.prob_away,
      sp.prob_over_25,
      sp.prob_under_25,
      h2h.matches_considered AS h2h_matches_considered,
      h2h.home_wins AS h2h_home_wins,
      h2h.draws AS h2h_draws,
      h2h.away_wins AS h2h_away_wins,
      h2h.home_goals AS h2h_home_goals,
      h2h.away_goals AS h2h_away_goals,
      trs_home.wins AS home_recent_wins,
      trs_home.draws AS home_recent_draws,
      trs_home.losses AS home_recent_losses,
      trs_home.failed_to_score AS home_recent_failed_to_score,
      trs_home.btts AS home_recent_btts,
      trs_home.over_25 AS home_recent_over_25,
      trs_home.under_25 AS home_recent_under_25,
      trs_home.goals_for AS home_recent_goals_for,
      trs_home.goals_against AS home_recent_goals_against,
      trs_home.clean_sheets AS home_recent_clean_sheets,
      trs_home.avg_goals_for AS home_avg_goals_for,
      trs_home.avg_goals_against AS home_avg_goals_against,
      trs_home.chance_score_next_pct AS home_chance_score_next_pct,
      trs_home.chance_concede_next_pct AS home_chance_concede_next_pct,
      trs_home.over_15_matches AS home_recent_over_15,
      trs_home.under_15_matches AS home_recent_under_15,
      trs_home.over_35_matches AS home_recent_over_35,
      trs_home.under_35_matches AS home_recent_under_35,
      trs_home.time_without_scored_goal_min AS home_time_without_scored_goal_min,
      trs_home.time_without_conceded_goal_min AS home_time_without_conceded_goal_min,
      trs_away.wins AS away_recent_wins,
      trs_away.draws AS away_recent_draws,
      trs_away.losses AS away_recent_losses,
      trs_away.failed_to_score AS away_recent_failed_to_score,
      trs_away.btts AS away_recent_btts,
      trs_away.over_25 AS away_recent_over_25,
      trs_away.over_25 AS away_recent_over_25,
      trs_away.under_25 AS away_recent_under_25,
      trs_away.goals_for AS away_recent_goals_for,
      trs_away.goals_against AS away_recent_goals_against,
      trs_away.clean_sheets AS away_recent_clean_sheets,
      trs_away.avg_goals_for AS away_avg_goals_for,
      trs_away.avg_goals_against AS away_avg_goals_against,
      trs_away.chance_score_next_pct AS away_chance_score_next_pct,
      trs_away.chance_concede_next_pct AS away_chance_concede_next_pct,
      trs_away.over_15_matches AS away_recent_over_15,
      trs_away.under_15_matches AS away_recent_under_15,
      trs_away.over_35_matches AS away_recent_over_35,
      trs_away.under_35_matches AS away_recent_under_35,
      trs_away.time_without_scored_goal_min AS away_time_without_scored_goal_min,
      trs_away.time_without_conceded_goal_min AS away_time_without_conceded_goal_min
    FROM fixtures f
    JOIN teams ht ON ht.id = f.home_team_id
    JOIN teams at ON at.id = f.away_team_id
    LEFT JOIN competitions c ON c.id = f.competition_id
    LEFT JOIN countries co ON co.id = f.country_id
    LEFT JOIN scraped_predictions sp
      ON sp.fixture_id = f.id
     AND sp.source_name = f.source_name
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
    WHERE f.kickoff_utc >= (($1::date + $2::time) AT TIME ZONE 'UTC') AND f.kickoff_utc <= (($1::date + $3::time) AT TIME ZONE 'UTC')
    ORDER BY f.kickoff_utc ASC, f.id ASC
    `,
    [targetDate, WINDOW_START, WINDOW_END]
  );

  return result.rows.map((row) => {
    const base = {
      fixture_id: row.fixture_id,
      kickoff_utc: row.kickoff_utc,
      compare_url: row.compare_url,
      country: row.country,
      competition: row.competition,
      home_team: row.home_team,
      away_team: row.away_team,
      source_tip: row.source_tip,
      prob_home: toNumber(row.prob_home),
      prob_draw: toNumber(row.prob_draw),
      prob_away: toNumber(row.prob_away),
      prob_over_25: toNumber(row.prob_over_25),
      prob_under_25: toNumber(row.prob_under_25),
      h2h: {
        matches_considered: toNumber(row.h2h_matches_considered),
        home_wins: toNumber(row.h2h_home_wins),
        draws: toNumber(row.h2h_draws),
        away_wins: toNumber(row.h2h_away_wins),
        home_goals: toNumber(row.h2h_home_goals),
        away_goals: toNumber(row.h2h_away_goals)
      },
      recent_form: {
        home: {
          wins: toNumber(row.home_recent_wins),
          draws: toNumber(row.home_recent_draws),
          losses: toNumber(row.home_recent_losses),
          goals_for: toNumber(row.home_recent_goals_for),
          goals_against: toNumber(row.home_recent_goals_against),
          clean_sheets: toNumber(row.home_recent_clean_sheets),
          failed_to_score: toNumber(row.home_recent_failed_to_score),
          btts: toNumber(row.home_recent_btts),
          over_15: toNumber(row.home_recent_over_15),
          under_15: toNumber(row.home_recent_under_15),
          over_25: toNumber(row.home_recent_over_25),
          under_25: toNumber(row.home_recent_under_25),
          over_35: toNumber(row.home_recent_over_35),
          under_35: toNumber(row.home_recent_under_35),
          avg_goals_for: toNumber(row.home_avg_goals_for),
          avg_goals_against: toNumber(row.home_avg_goals_against),
          chance_score_next_pct: toNumber(row.home_chance_score_next_pct),
          chance_concede_next_pct: toNumber(row.home_chance_concede_next_pct),
          time_without_scored_goal_min: toNumber(row.home_time_without_scored_goal_min),
          time_without_conceded_goal_min: toNumber(row.home_time_without_conceded_goal_min)
        },
        away: {
          wins: toNumber(row.away_recent_wins),
          draws: toNumber(row.away_recent_draws),
          losses: toNumber(row.away_recent_losses),
          goals_for: toNumber(row.away_recent_goals_for),
          goals_against: toNumber(row.away_recent_goals_against),
          clean_sheets: toNumber(row.away_recent_clean_sheets),
          failed_to_score: toNumber(row.away_recent_failed_to_score),
          btts: toNumber(row.away_recent_btts),
          over_15: toNumber(row.away_recent_over_15),
          under_15: toNumber(row.away_recent_under_15),
          over_25: toNumber(row.away_recent_over_25),
          under_25: toNumber(row.away_recent_under_25),
          over_35: toNumber(row.away_recent_over_35),
          under_35: toNumber(row.away_recent_under_35),
          avg_goals_for: toNumber(row.away_avg_goals_for),
          avg_goals_against: toNumber(row.away_avg_goals_against),
          chance_score_next_pct: toNumber(row.away_chance_score_next_pct),
          chance_concede_next_pct: toNumber(row.away_chance_concede_next_pct),
          time_without_scored_goal_min: toNumber(row.away_time_without_scored_goal_min),
          time_without_conceded_goal_min: toNumber(row.away_time_without_conceded_goal_min)
        }
      }
    };

    // const markets = uniqueByFixtureAndMarket(buildMarkets(base));
    const markets = uniqueByFixtureAndMarket(
      applyFootballRules(base, buildMarkets(base))
    );
    return {
      ...base,
      available_markets: markets
    };
  });
}

function prepareFixturesForAI(fixtures, config) {
  const prepared = fixtures
    .map((fixture) => ({
      ...fixture,
      available_markets: filterMarkets(fixture.available_markets, config)
    }))
    .filter((fixture) => fixture.available_markets.length > 0);

  return prepared.slice(0, config.prediction.max_daily_picks || prepared.length);
}

function buildPromptPayload(fixtures, accumulatorCandidates, config, targetDate) {
  return {
    date: targetDate,
    rules: {
      min_confidence: config.prediction.min_model_confidence,
      min_acca_total_odds: config.prediction.min_acca_total_odds,
      max_acca_total_odds: config.prediction.max_acca_total_odds,
      max_acca_legs: config.prediction.max_acca_legs,
      allow_system_bets: config.prediction.allow_system_bets
    },
    staking: config.staking || {
      daily_bankroll: 10,
      currency: 'EUR',
      singles_pct: 70,
      accumulators_pct: 20,
      systems_pct: 10
    },
    notes: {
      synthetic_odds: true,
      synthetic_odds_basis: 'Odds are approximated from probabilities with margin-adjusted implied probability.',
      staking_warning: 'Staking plan distributes risk but does not guarantee profit.'
    },
    fixtures,
    accumulator_candidates: accumulatorCandidates
  };
}

async function callOpenAIForTips(config, payload) {
  const apiKey = env[config.ai.api_key_env];
  if (!apiKey) {
    throw new Error(`Missing API key in env var ${config.ai.api_key_env}`);
  }

  const client = new OpenAI({ apiKey });

  const schema = {
    name: 'betting_tips_response',
    strict: true,
    schema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        staking_plan: {
          type: 'object',
          additionalProperties: false,
          properties: {
            daily_bankroll: { type: 'number' },
            currency: { type: 'string' },
            singles_total: { type: 'number' },
            accumulators_total: { type: 'number' },
            systems_total: { type: 'number' },
            notes: { type: 'string' }
          },
          required: [
            'daily_bankroll',
            'currency',
            'singles_total',
            'accumulators_total',
            'systems_total',
            'notes'
          ]
        },
        singles: {
          type: 'array',
          items: {
            type: 'object',
            additionalProperties: false,
            properties: {
              fixture_id: { type: 'integer' },
              market: { type: 'string' },
              pick: { type: 'string' },
              odds: { type: 'number' },
              stake: { type: 'number' },
              confidence: { type: 'number' },
              reason: { type: 'string' }
            },
            required: ['fixture_id', 'market', 'pick', 'odds', 'stake', 'confidence', 'reason']
          }
        },
        accumulators: {
          type: 'array',
          items: {
            type: 'object',
            additionalProperties: false,
            properties: {
              name: { type: 'string' },
              total_odds: { type: 'number' },
              stake: { type: 'number' },
              confidence: { type: 'number' },
              reason: { type: 'string' },
              legs: {
                type: 'array',
                items: {
                  type: 'object',
                  additionalProperties: false,
                  properties: {
                    fixture_id: { type: 'integer' },
                    market: { type: 'string' },
                    pick: { type: 'string' },
                    odds: { type: 'number' }
                  },
                  required: ['fixture_id', 'market', 'pick', 'odds']
                }
              }
            },
            required: ['name', 'total_odds', 'stake', 'confidence', 'reason', 'legs']
          }
        },
        system_bets: {
          type: 'array',
          items: {
            type: 'object',
            additionalProperties: false,
            properties: {
              type: { type: 'string' },
              stake: { type: 'number' },
              reason: { type: 'string' },
              legs: {
                type: 'array',
                items: {
                  type: 'object',
                  additionalProperties: false,
                  properties: {
                    fixture_id: { type: 'integer' },
                    market: { type: 'string' },
                    pick: { type: 'string' },
                    odds: { type: 'number' }
                  },
                  required: ['fixture_id', 'market', 'pick', 'odds']
                }
              }
            },
            required: ['type', 'stake', 'reason', 'legs']
          }
        },
        excluded_fixtures: {
          type: 'array',
          items: {
            type: 'object',
            additionalProperties: false,
            properties: {
              fixture_id: { type: 'integer' },
              reason: { type: 'string' }
            },
            required: ['fixture_id', 'reason']
          }
        }
      },
      required: ['staking_plan', 'singles', 'accumulators', 'system_bets', 'excluded_fixtures']
    }
  };

  const response = await client.responses.create({
    model: config.ai.model,
    input: [
      {
        role: 'system',
        content: [
          {
            type: 'input_text',
            text:
                'You are a football betting analysis assistant. Use only the provided dataset. Do not invent fixtures, markets, odds, or results. Analyze all available data: source probabilities, H2H, recent form, average goals for/against, score/concede chances, clean sheets, failed-to-score, BTTS, over/under 1.5/2.5/3.5 counts, and time since scored/conceded. Avoid picks where signals conflict strongly. Prefer lower-variance picks, but do not force a pick if the data is weak. Explain each pick using specific stats from the payload. Create a practical staking plan using the provided bankroll config. Singles should carry most of the bankroll. Accumulators and systems should be smaller speculative positions. You are not allowed to blindly follow probabilities. You must: - Detect contextual mismatches (home advantage, travel, altitude, competition type) - Prioritize recent form over generic probabilities - Penalize away teams in difficult environments (South America, altitude, hostile venues) - Avoid generic "balanced → double chance" reasoning. If a pick is weak or unclear → DO NOT INCLUDE IT. You must justify every pick using: - recent form stats - scoring/conceding metrics - match context (competition, location) - risk level. You are NOT allowed to default to conservative picks without clear statistical support. Market reason_tags are pre-AI rule signals. Treat them as important. If a market is penalized, do not select it unless other data strongly overrides it. Accumulator candidates are pre-filtered for lower correlation and lower variance. Prefer candidates with higher model_score. Do not create accumulator legs outside the provided accumulator_candidates list. Return only schema-valid JSON.'
          }
        ]
      },
      {
        role: 'user',
        content: [
          {
            type: 'input_text',
            text: JSON.stringify(payload)
          }
        ]
      }
    ],
    temperature: config.ai.temperature,
    max_output_tokens: config.ai.max_output_tokens,
    text: {
      format: {
        type: 'json_schema',
        name: schema.name,
        strict: true,
        schema: schema.schema
      }
    }
  });

  return JSON.parse(response.output_text);
}

function writeOutputFile(targetDate, data) {
  ensureOutputDir();
  const filename = path.join(OUTPUT_DIR, `tips-${targetDate}-${WINDOW}.json`);
  fs.writeFileSync(filename, JSON.stringify(data, null, 2), 'utf8');
  return filename;
}

async function deleteExistingBatchForDate(client, targetDate) {
  await client.query(
    `DELETE FROM sent_tip_batches
     WHERE tip_date = $1::date
       AND tip_window = $2`,
    [targetDate, WINDOW]
  );
}

async function insertTipBatch(client, targetDate, tipsFile, config) {
  const staking = tipsFile.ai_tips?.staking_plan || config.staking || {};

  const result = await client.query(
    `
    INSERT INTO sent_tip_batches (
      tip_date,
      tip_window,
      bankroll,
      currency,
      raw_payload
    )
    VALUES ($1, $2, $3, $4, $5)
    RETURNING id
    `,
    [
      targetDate,
      WINDOW,
      staking.daily_bankroll || null,
      staking.currency || 'EUR',
      JSON.stringify(tipsFile)
    ]
  );

  return result.rows[0].id;
}

async function insertSentTip(client, batchId, tip) {
  const result = await client.query(
    `
    INSERT INTO sent_tips (
      batch_id,
      fixture_id,
      bet_type,
      market,
      pick,
      odds,
      stake,
      confidence,
      status,
      profit_loss,
      raw_payload
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'pending',NULL,$9)
    RETURNING id
    `,
    [
      batchId,
      tip.fixture_id || null,
      tip.bet_type,
      tip.market || null,
      tip.pick || null,
      tip.odds || null,
      tip.stake || null,
      tip.confidence || null,
      JSON.stringify(tip.raw_payload || tip)
    ]
  );

  return result.rows[0].id;
}

async function insertSentTipLeg(client, sentTipId, leg) {
  await client.query(
    `
    INSERT INTO sent_tip_legs (
      sent_tip_id,
      fixture_id,
      market,
      pick,
      odds,
      status,
      raw_payload
    )
    VALUES ($1,$2,$3,$4,$5,'pending',$6)
    `,
    [
      sentTipId,
      leg.fixture_id || null,
      leg.market,
      leg.pick,
      leg.odds || null,
      JSON.stringify(leg)
    ]
  );
}

async function storeTipsInDb(client, targetDate, tipsFile, config) {
  const aiTips = tipsFile.ai_tips || {};
  const singles = aiTips.singles || [];
  const accumulators = aiTips.accumulators || [];
  const systemBets = aiTips.system_bets || [];

  await client.query('BEGIN');

  try {
    await deleteExistingBatchForDate(client, targetDate);

    const batchId = await insertTipBatch(client, targetDate, tipsFile, config);

    for (const single of singles) {
      await insertSentTip(client, batchId, {
        ...single,
        bet_type: 'single',
        raw_payload: single
      });
    }

    for (const acca of accumulators) {
      const sentTipId = await insertSentTip(client, batchId, {
        fixture_id: null,
        bet_type: 'accumulator',
        market: 'accumulator',
        pick: acca.name,
        odds: acca.total_odds,
        stake: acca.stake,
        confidence: acca.confidence,
        raw_payload: acca
      });

      for (const leg of acca.legs || []) {
        await insertSentTipLeg(client, sentTipId, leg);
      }
    }

    for (const systemBet of systemBets) {
      const sentTipId = await insertSentTip(client, batchId, {
        fixture_id: null,
        bet_type: 'system',
        market: 'system',
        pick: systemBet.type,
        odds: null,
        stake: systemBet.stake,
        confidence: null,
        raw_payload: systemBet
      });

      for (const leg of systemBet.legs || []) {
        await insertSentTipLeg(client, sentTipId, leg);
      }
    }

    await client.query('COMMIT');
    console.log(`Stored tips batch ${batchId} for ${targetDate}`);
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  }
}

async function main() {
  const config = loadConfig();
  const client = await db.getClient();

  try {
    const targetDate = todayDateISO();
    const dataset = await loadTodayDataset(client, targetDate);
    const preparedFixtures = prepareFixturesForAI(dataset, config);
    const accumulatorCandidates = buildAccumulatorCandidates(preparedFixtures, config);

    if (!preparedFixtures.length) {
      console.log('No fixtures with usable markets found.');
      return;
    }

    const payload = buildPromptPayload(preparedFixtures, accumulatorCandidates, config, targetDate);

    if (!config.ai.enabled) {
      const tipsFile = {
        date: targetDate,
        window: WINDOW,
        window_start: WINDOW_START,
        window_end: WINDOW_END,
        payload,
        ai_tips: {
          staking_plan: config.staking || {},
          singles: [],
          accumulators: [],
          system_bets: [],
          excluded_fixtures: []
        }
      };

      const file = writeOutputFile(targetDate, tipsFile);
      await storeTipsInDb(client, targetDate, tipsFile, config);
      console.log(`AI disabled. Wrote dataset to ${file}`);
      return;
    }

    const aiTips = await callOpenAIForTips(config, payload);

    const tipsFile = {
      date: targetDate,
      window: WINDOW,
      window_start: WINDOW_START,
      window_end: WINDOW_END,
      payload,
      ai_tips: aiTips
    };

    const file = writeOutputFile(targetDate, tipsFile);
    await storeTipsInDb(client, targetDate, tipsFile, config);

    console.log(`Tips written to ${file}`);
  } finally {
    client.release();
    await db.pool.end();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
