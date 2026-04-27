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

function buildAccumulatorCandidates(fixtures, config) {
  const maxLegs = config.prediction.max_acca_legs || 3;
  const minTotalOdds = config.prediction.min_acca_total_odds || 3;
  const maxTotalOdds = config.prediction.max_acca_total_odds || 5;

  const picks = [];

  for (const fixture of fixtures) {
    for (const market of fixture.available_markets || []) {
      picks.push({
        fixture_id: fixture.fixture_id,
        home_team: fixture.home_team,
        away_team: fixture.away_team,
        country: fixture.country,
        competition: fixture.competition,
        market: market.market,
        pick: market.pick,
        prob: market.prob,
        odds: market.odds
      });
    }
  }

  const candidates = [];

  for (let i = 0; i < picks.length; i += 1) {
    for (let j = i + 1; j < picks.length; j += 1) {
      if (picks[i].fixture_id === picks[j].fixture_id) continue;

      const totalOdds2 = Number((picks[i].odds * picks[j].odds).toFixed(2));
      if (totalOdds2 >= minTotalOdds && totalOdds2 <= maxTotalOdds) {
        candidates.push({
          name: `acca_2_${picks[i].fixture_id}_${picks[j].fixture_id}`,
          legs: [picks[i], picks[j]],
          total_odds: totalOdds2
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

          const totalOdds3 = Number((picks[i].odds * picks[j].odds * picks[k].odds).toFixed(2));
          if (totalOdds3 >= minTotalOdds && totalOdds3 <= maxTotalOdds) {
            candidates.push({
              name: `acca_3_${picks[i].fixture_id}_${picks[j].fixture_id}_${picks[k].fixture_id}`,
              legs: [picks[i], picks[j], picks[k]],
              total_odds: totalOdds3
            });
          }
        }
      }
    }
  }

  return candidates.slice(0, 150);
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
      trs_away.wins AS away_recent_wins,
      trs_away.draws AS away_recent_draws,
      trs_away.losses AS away_recent_losses,
      trs_away.failed_to_score AS away_recent_failed_to_score,
      trs_away.btts AS away_recent_btts,
      trs_away.over_25 AS away_recent_over_25
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
    WHERE f.kickoff_utc >= NOW() AND f.kickoff_utc >= ($1::date + $2::time) AND f.kickoff_utc <= ($1::date + $3::time)
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
          failed_to_score: toNumber(row.home_recent_failed_to_score),
          btts: toNumber(row.home_recent_btts),
          over_25: toNumber(row.home_recent_over_25)
        },
        away: {
          wins: toNumber(row.away_recent_wins),
          draws: toNumber(row.away_recent_draws),
          losses: toNumber(row.away_recent_losses),
          failed_to_score: toNumber(row.away_recent_failed_to_score),
          btts: toNumber(row.away_recent_btts),
          over_25: toNumber(row.away_recent_over_25)
        }
      }
    };

    const markets = uniqueByFixtureAndMarket(buildMarkets(base));

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
              'You are a football betting analysis assistant. Use only the provided dataset. Do not invent fixtures, markets, odds, or results. Favor lower variance picks. Create a practical staking plan using the provided bankroll config. Singles should carry most of the bankroll. Accumulators and systems should be smaller speculative positions. Return only schema-valid JSON.'
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
