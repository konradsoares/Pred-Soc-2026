const db = require('../db/connection');

function clampProbability(value) {
  const n = Number(value);

  if (!Number.isFinite(n)) {
    return null;
  }

  if (n < 0.01) {
    return 0.01;
  }

  if (n > 0.99) {
    return 0.99;
  }

  return n;
}

function pctToProbability(value) {
  if (value === null || value === undefined) {
    return null;
  }

  const n = Number(value);

  if (!Number.isFinite(n)) {
    return null;
  }

  if (n > 1) {
    return clampProbability(n / 100);
  }

  return clampProbability(n);
}

async function getFixtureStats(fixtureId) {
  if (!fixtureId) {
    return {
      prediction: null,
      recentStats: [],
      h2h: [],
      marketPerformance: [],
    };
  }

  const predictionResult = await db.query(
    `
    SELECT *
    FROM scraped_predictions
    WHERE fixture_id = $1
    ORDER BY id DESC
    LIMIT 1
    `,
    [fixtureId]
  );

  const recentStatsResult = await db.query(
    `
    SELECT *
    FROM team_recent_stats
    WHERE fixture_id = $1
    ORDER BY id DESC
    `,
    [fixtureId]
  );

  const h2hResult = await db.query(
    `
    SELECT *
    FROM head_to_head_stats
    WHERE fixture_id = $1
    ORDER BY id DESC
    LIMIT 10
    `,
    [fixtureId]
  );

  const marketPerformanceResult = await db.query(
    `
    SELECT *
    FROM paper_market_performance
    ORDER BY total_bets DESC NULLS LAST
    LIMIT 100
    `
  );

  return {
    prediction: predictionResult.rows[0] || null,
    recentStats: recentStatsResult.rows,
    h2h: h2hResult.rows,
    marketPerformance: marketPerformanceResult.rows,
  };
}

function getHistoricalMarketAdjustment(marketType, marketPerformance) {
  if (!Array.isArray(marketPerformance)) {
    return {
      adjustment: 0,
      unstable: false,
      reason: 'No historical market performance available',
    };
  }

  const row = marketPerformance.find((item) => {
    const marketName =
      item.market_type ||
      item.market_name ||
      item.betfair_market_type ||
      '';

    return String(marketName).toUpperCase() === String(marketType).toUpperCase();
  });

  if (!row) {
    return {
      adjustment: 0,
      unstable: false,
      reason: 'No historical data for this market type',
    };
  }

  const roi = Number(row.roi || row.profit_roi || row.yield || 0);
  const strikeRate = Number(row.strike_rate || row.win_rate || 0);
  const totalBets = Number(row.total_bets || row.count || 0);

  if (totalBets >= 10 && roi < 0 && strikeRate < 0.5) {
    return {
      adjustment: -0.04,
      unstable: true,
      reason: 'Historically weak market performance',
    };
  }

  if (totalBets >= 10 && roi > 0 && strikeRate >= 0.55) {
    return {
      adjustment: 0.02,
      unstable: false,
      reason: 'Historically positive market performance',
    };
  }

  return {
    adjustment: 0,
    unstable: false,
    reason: 'Neutral historical market performance',
  };
}

function estimateFromPrediction(marketType, runnerName, prediction) {
  if (!prediction) {
    return null;
  }

  const pick = String(runnerName || '').toLowerCase();
  const market = String(marketType || '').toUpperCase();

  const homeProb =
    pctToProbability(prediction.home_probability) ||
    pctToProbability(prediction.home_prob) ||
    pctToProbability(prediction.prob_home);

  const drawProb =
    pctToProbability(prediction.draw_probability) ||
    pctToProbability(prediction.draw_prob) ||
    pctToProbability(prediction.prob_draw);

  const awayProb =
    pctToProbability(prediction.away_probability) ||
    pctToProbability(prediction.away_prob) ||
    pctToProbability(prediction.prob_away);

  if (market === 'MATCH_ODDS') {
    if (pick.includes('draw')) return drawProb;
    if (homeProb && prediction.home_team && pick.includes(String(prediction.home_team).toLowerCase())) return homeProb;
    if (awayProb && prediction.away_team && pick.includes(String(prediction.away_team).toLowerCase())) return awayProb;

    return null;
  }

  if (market === 'DOUBLE_CHANCE') {
    if (pick.includes('1x') || pick.includes('home or draw')) {
      return homeProb && drawProb ? clampProbability(homeProb + drawProb) : null;
    }

    if (pick.includes('x2') || pick.includes('draw or away')) {
      return awayProb && drawProb ? clampProbability(awayProb + drawProb) : null;
    }

    if (pick.includes('12') || pick.includes('home or away')) {
      return homeProb && awayProb ? clampProbability(homeProb + awayProb) : null;
    }
  }

  return null;
}

function estimateFromStats(marketType, runnerName, recentStats, h2h) {
  const market = String(marketType || '').toUpperCase();
  const pick = String(runnerName || '').toLowerCase();

  let probability = null;
  const reasons = [];

  if (Array.isArray(recentStats) && recentStats.length) {
    const avgGoalsFor = recentStats
      .map((r) => Number(r.avg_goals_for))
      .filter(Number.isFinite);

    const avgGoalsAgainst = recentStats
      .map((r) => Number(r.avg_goals_against))
      .filter(Number.isFinite);

    const avgFor =
      avgGoalsFor.length > 0
        ? avgGoalsFor.reduce((a, b) => a + b, 0) / avgGoalsFor.length
        : null;

    const avgAgainst =
      avgGoalsAgainst.length > 0
        ? avgGoalsAgainst.reduce((a, b) => a + b, 0) / avgGoalsAgainst.length
        : null;

    if (market === 'OVER_UNDER_15') {
      if (pick.includes('over') && avgFor !== null && avgAgainst !== null) {
        probability = avgFor + avgAgainst >= 2.0 ? 0.72 : 0.58;
        reasons.push('Recent goals trend supports Over/Under 1.5 estimate');
      }

      if (pick.includes('under') && avgFor !== null && avgAgainst !== null) {
        probability = avgFor + avgAgainst < 2.0 ? 0.62 : 0.42;
        reasons.push('Recent goals trend used for Under 1.5 estimate');
      }
    }

    if (market === 'OVER_UNDER_25') {
      if (pick.includes('over') && avgFor !== null && avgAgainst !== null) {
        probability = avgFor + avgAgainst >= 2.8 ? 0.66 : 0.48;
        reasons.push('Recent goals trend used for Over 2.5 estimate');
      }

      if (pick.includes('under') && avgFor !== null && avgAgainst !== null) {
        probability = avgFor + avgAgainst < 2.8 ? 0.60 : 0.44;
        reasons.push('Recent goals trend used for Under 2.5 estimate');
      }
    }
  }

  if (Array.isArray(h2h) && h2h.length) {
    reasons.push(`H2H sample available: ${h2h.length} rows`);
  }

  return {
    probability: clampProbability(probability),
    reasons,
  };
}

async function calculateModelProbability(payload) {
  const {
    fixtureId,
    marketType,
    runnerName,
  } = payload;

  const stats = await getFixtureStats(fixtureId);

  const predictionProb = estimateFromPrediction(
    marketType,
    runnerName,
    stats.prediction
  );

  const statsEstimate = estimateFromStats(
    marketType,
    runnerName,
    stats.recentStats,
    stats.h2h
  );

  const historical = getHistoricalMarketAdjustment(
    marketType,
    stats.marketPerformance
  );

  let baseProbability = predictionProb || statsEstimate.probability;

  if (!baseProbability) {
    return {
      modelProbability: null,
      riskLevel: 'reject',
      reason: 'No usable probability source found',
      statsSummary: {
        hasPrediction: Boolean(stats.prediction),
        recentStatsRows: stats.recentStats.length,
        h2hRows: stats.h2h.length,
        historicalReason: historical.reason,
      },
    };
  }

  const adjustedProbability = clampProbability(
    baseProbability + historical.adjustment
  );

  let riskLevel = 'medium';

  if (historical.unstable) {
    riskLevel = 'high';
  }

  if (predictionProb && statsEstimate.probability) {
    const diff = Math.abs(predictionProb - statsEstimate.probability);

    if (diff >= 0.18) {
      return {
        modelProbability: null,
        riskLevel: 'reject',
        reason: 'Prediction and recent stats conflict heavily',
        statsSummary: {
          predictionProbability: predictionProb,
          statsProbability: statsEstimate.probability,
          probabilityDifference: diff,
          historicalReason: historical.reason,
        },
      };
    }

    if (diff < 0.08 && !historical.unstable) {
      riskLevel = 'low';
    }
  }

  return {
    modelProbability: adjustedProbability,
    riskLevel,
    reason: [
      predictionProb ? 'Statarea probability used' : 'Recent stats probability used',
      ...statsEstimate.reasons,
      historical.reason,
    ].join('. '),
    statsSummary: {
      hasPrediction: Boolean(stats.prediction),
      predictionProbability: predictionProb,
      statsProbability: statsEstimate.probability,
      recentStatsRows: stats.recentStats.length,
      h2hRows: stats.h2h.length,
      historicalAdjustment: historical.adjustment,
      historicalUnstable: historical.unstable,
      historicalReason: historical.reason,
    },
  };
}

module.exports = {
  calculateModelProbability,
};
