const BetfairClient = require('../betfair/client');
const db = require('../db/connection');

const MARKET_TYPES = [
  'MATCH_ODDS',
  'DOUBLE_CHANCE',
  'OVER_UNDER_15',
  'OVER_UNDER_25'
];

const MIN_BACK_ODD = 1.20;
const MIN_EDGE = 0.01;
const MAX_OPPORTUNITIES_PER_EVENT = 1;

function addRejection(rejections, item) {
  rejections.push({
    time: new Date().toISOString(),
    ...item
  });
}

function getBestBackOdd(runner) {
  const prices = runner?.ex?.availableToBack || [];
  if (!prices.length) return null;
  return Number(prices[0].price);
}

function impliedProbabilityFromOdd(odd) {
  return Number((1 / odd).toFixed(6));
}

function getMarketType(catalogue) {
  return catalogue?.description?.marketType || null;
}

function getRunnerName(catalogue, selectionId) {
  const runner = catalogue.runners.find(
    r => String(r.selectionId) === String(selectionId)
  );

  return runner ? runner.runnerName : null;
}

function parseTeamsFromEventName(eventName) {
  if (!eventName) return null;

  const separators = [' v ', ' vs ', ' - '];

  for (const separator of separators) {
    if (eventName.includes(separator)) {
      const [homeTeam, awayTeam] = eventName.split(separator).map(v => v.trim());

      if (homeTeam && awayTeam) {
        return { homeTeam, awayTeam };
      }
    }
  }

  return null;
}

function getLiveContext() {
  return {
    minute: null,
    score: null,
    source: 'not_available_from_current_betfair_json_rpc_calls'
  };
}

function calculateConfidenceScore({
  edge,
  riskLevel,
  mappingConfidence,
  hasPrediction,
  recentStatsRows,
  compareUrl,
  marketType
}) {
  let score = 50;

  if (edge >= 0.15) score += 25;
  else if (edge >= 0.10) score += 18;
  else if (edge >= 0.05) score += 10;
  else if (edge >= 0.02) score += 4;

  if (riskLevel === 'low') score += 15;
  if (riskLevel === 'medium') score += 5;
  if (riskLevel === 'high') score -= 10;

  if (mappingConfidence === 'high') score += 10;
  else score -= 25;

  if (hasPrediction) score += 8;
  if (recentStatsRows >= 2) score += 7;
  if (compareUrl) score += 5;

  if (marketType === 'DOUBLE_CHANCE') score -= 5;
  if (marketType === 'OVER_UNDER_15') score += 5;

  if (score > 100) score = 100;
  if (score < 0) score = 0;

  return score;
}

function shouldTriggerAlert(opportunity) {
  return (
    opportunity.edge >= 0.10 &&
    opportunity.riskLevel === 'low' &&
    opportunity.confidenceScore >= 80 &&
    opportunity.modelProbability >= 0.60
  );
}

async function findFixture(eventId, eventName) {
  const mapped = await db.query(
    `
    SELECT
      f.id AS fixture_id,
      ht.name AS home_team,
      at.name AS away_team,
      f.compare_url
    FROM betfair_tip_mappings btm
    JOIN fixtures f ON f.id = btm.fixture_id
    LEFT JOIN teams ht ON ht.id = f.home_team_id
    LEFT JOIN teams at ON at.id = f.away_team_id
    WHERE btm.betfair_event_id = $1
    ORDER BY btm.updated_at DESC NULLS LAST, btm.id DESC
    LIMIT 1
    `,
    [String(eventId)]
  );

  if (mapped.rows.length) {
    return {
      fixtureId: mapped.rows[0].fixture_id,
      homeTeam: mapped.rows[0].home_team,
      awayTeam: mapped.rows[0].away_team,
      compareUrl: mapped.rows[0].compare_url,
      confidence: 'high',
      method: 'betfair_tip_mappings'
    };
  }

  const parsed = parseTeamsFromEventName(eventName);

  if (!parsed) {
    return {
      fixtureId: null,
      homeTeam: null,
      awayTeam: null,
      compareUrl: null,
      confidence: 'none',
      method: 'parse_failed'
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
      confidence: 'high',
      method: 'team_name_exact'
    };
  }

  return {
    fixtureId: null,
    homeTeam: parsed.homeTeam,
    awayTeam: parsed.awayTeam,
    compareUrl: null,
    confidence: 'low',
    method: 'parsed_only'
  };
}

async function getBasicModelProbability({ fixtureId, marketType, runnerName }) {
  if (!fixtureId) return null;

  function toProb(value) {
    if (value === null || value === undefined || value === '') return null;

    const n = Number(value);

    if (!Number.isFinite(n)) return null;

    if (n > 1) return n / 100;

    return n;
  }

  function firstProb(row, keys) {
    if (!row) return null;

    for (const key of keys) {
      if (row[key] !== undefined && row[key] !== null) {
        const p = toProb(row[key]);
        if (p !== null && p > 0 && p < 1) return p;
      }
    }

    return null;
  }

  function poissonCdf(k, lambda) {
    let sum = 0;

    for (let i = 0; i <= k; i++) {
      sum += (Math.exp(-lambda) * Math.pow(lambda, i)) / factorial(i);
    }

    return sum;
  }

  function factorial(n) {
    if (n <= 1) return 1;

    let result = 1;

    for (let i = 2; i <= n; i++) {
      result *= i;
    }

    return result;
  }

  const prediction = await db.query(
    `
    SELECT *
    FROM scraped_predictions
    WHERE fixture_id = $1
    ORDER BY id DESC
    LIMIT 1
    `,
    [fixtureId]
  );

  const recentStats = await db.query(
    `
    SELECT *
    FROM team_recent_stats
    WHERE fixture_id = $1
    ORDER BY id DESC
    `,
    [fixtureId]
  );

  const predictionRow = prediction.rows[0] || null;
  const statsRows = recentStats.rows || [];

  const pick = String(runnerName || '').toLowerCase();
  const market = String(marketType || '').toUpperCase();

  let probability = null;
  const reasons = [];

  const isOver = pick.includes('over');
  const isUnder = pick.includes('under');

  if (predictionRow) {
    const homeProb = firstProb(predictionRow, [
      'home_probability',
      'home_prob',
      'prob_home',
      'home_win_probability',
      'home_win_prob'
    ]);

    const drawProb = firstProb(predictionRow, [
      'draw_probability',
      'draw_prob',
      'prob_draw'
    ]);

    const awayProb = firstProb(predictionRow, [
      'away_probability',
      'away_prob',
      'prob_away',
      'away_win_probability',
      'away_win_prob'
    ]);

    const over15 = firstProb(predictionRow, [
      'over_1_5',
      'over15',
      'over_15',
      'over_1_5_probability',
      'over15_probability',
      'prob_over_1_5',
      'prob_over15'
    ]);

    const over25 = firstProb(predictionRow, [
      'over_2_5',
      'over25',
      'over_25',
      'over_2_5_probability',
      'over25_probability',
      'prob_over_2_5',
      'prob_over25'
    ]);

    const over35 = firstProb(predictionRow, [
      'over_3_5',
      'over35',
      'over_35',
      'over_3_5_probability',
      'over35_probability',
      'prob_over_3_5',
      'prob_over35'
    ]);

    if (market === 'MATCH_ODDS') {
      if (pick.includes('draw')) {
        probability = drawProb;
      } else if (
        predictionRow.home_team &&
        pick.includes(String(predictionRow.home_team).toLowerCase())
      ) {
        probability = homeProb;
      } else if (
        predictionRow.away_team &&
        pick.includes(String(predictionRow.away_team).toLowerCase())
      ) {
        probability = awayProb;
      }

      if (probability !== null) {
        reasons.push('Statarea match result probability used');
      }
    }

    if (market === 'DOUBLE_CHANCE') {
      if ((pick.includes('1x') || pick.includes('home or draw')) && homeProb !== null && drawProb !== null) {
        probability = homeProb + drawProb;
      }

      if ((pick.includes('x2') || pick.includes('draw or away')) && drawProb !== null && awayProb !== null) {
        probability = drawProb + awayProb;
      }

      if ((pick.includes('12') || pick.includes('home or away')) && homeProb !== null && awayProb !== null) {
        probability = homeProb + awayProb;
      }

      if (probability !== null) {
        probability = Math.min(probability, 0.98);
        reasons.push('Statarea double chance probability used');
      }
    }

    if (market === 'OVER_UNDER_15' && over15 !== null) {
      probability = isOver ? over15 : isUnder ? 1 - over15 : null;
      reasons.push('Statarea Over/Under 1.5 distribution used');
    }

    if (market === 'OVER_UNDER_25' && over25 !== null) {
      probability = isOver ? over25 : isUnder ? 1 - over25 : null;
      reasons.push('Statarea Over/Under 2.5 distribution used');
    }

    if (market === 'OVER_UNDER_35' && over35 !== null) {
      probability = isOver ? over35 : isUnder ? 1 - over35 : null;
      reasons.push('Statarea Over/Under 3.5 distribution used');
    }
  }

  if (probability === null && statsRows.length) {
    const avgGoalsFor = statsRows
      .map(r => Number(r.avg_goals_for))
      .filter(Number.isFinite);

    const avgGoalsAgainst = statsRows
      .map(r => Number(r.avg_goals_against))
      .filter(Number.isFinite);

    const avgFor = avgGoalsFor.length
      ? avgGoalsFor.reduce((a, b) => a + b, 0) / avgGoalsFor.length
      : null;

    const avgAgainst = avgGoalsAgainst.length
      ? avgGoalsAgainst.reduce((a, b) => a + b, 0) / avgGoalsAgainst.length
      : null;

    const lambda =
      avgFor !== null && avgAgainst !== null
        ? Math.max(0.2, Math.min(5.0, avgFor + avgAgainst))
        : null;

    if (lambda !== null) {
      if (market === 'OVER_UNDER_15') {
        const under15 = poissonCdf(1, lambda);
        probability = isOver ? 1 - under15 : isUnder ? under15 : null;
      }

      if (market === 'OVER_UNDER_25') {
        const under25 = poissonCdf(2, lambda);
        probability = isOver ? 1 - under25 : isUnder ? under25 : null;
      }

      if (market === 'OVER_UNDER_35') {
        const under35 = poissonCdf(3, lambda);
        probability = isOver ? 1 - under35 : isUnder ? under35 : null;
      }

      if (probability !== null) {
        reasons.push('Poisson estimate from recent goals trend used');
      }
    }
  }

  if (probability === null || probability <= 0 || probability >= 1) {
    return null;
  }

  return {
    modelProbability: Number(probability.toFixed(6)),
    reason: reasons.join('. '),
    statsSummary: {
      hasPrediction: Boolean(predictionRow),
      recentStatsRows: statsRows.length,
      probabilitySource: reasons[0] || null
    }
  };
}

function getGoalDirection(opp) {
  const pick = String(opp.runnerName || '').toLowerCase();

  if (pick.includes('over')) return 'over';
  if (pick.includes('under')) return 'under';

  return 'other';
}

function isGoalsMarket(opp) {
  return String(opp.marketType || '').startsWith('OVER_UNDER');
}

function groupOpportunities(opportunities) {
  const byEvent = new Map();

  for (const opp of opportunities) {
    if (!byEvent.has(opp.betfairEventId)) {
      byEvent.set(opp.betfairEventId, []);
    }

    byEvent.get(opp.betfairEventId).push(opp);
  }

  const final = [];

  for (const eventOpps of byEvent.values()) {
    const goals = eventOpps.filter(isGoalsMarket);
    const nonGoals = eventOpps.filter(o => !isGoalsMarket(o));

    if (goals.length) {
      const bestGoal = goals.sort((a, b) => {
        if (b.confidenceScore !== a.confidenceScore) {
          return b.confidenceScore - a.confidenceScore;
        }

        if (b.modelProbability !== a.modelProbability) {
          return b.modelProbability - a.modelProbability;
        }

        return b.edge - a.edge;
      })[0];

      final.push(bestGoal);
    }

    if (nonGoals.length) {
      const bestNonGoal = nonGoals.sort((a, b) => {
        if (b.confidenceScore !== a.confidenceScore) {
          return b.confidenceScore - a.confidenceScore;
        }

        if (b.modelProbability !== a.modelProbability) {
          return b.modelProbability - a.modelProbability;
        }

        return b.edge - a.edge;
      })[0];

      final.push(bestNonGoal);
    }
  }

  return final.sort((a, b) => {
    if (b.confidenceScore !== a.confidenceScore) {
      return b.confidenceScore - a.confidenceScore;
    }

    if (b.modelProbability !== a.modelProbability) {
      return b.modelProbability - a.modelProbability;
    }

    return b.edge - a.edge;
  });
}

function chunkArray(items, size) {
  const chunks = [];

  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size));
  }

  return chunks;
}

async function listMarketBooksInChunks(betfair, marketIds) {
  const chunks = chunkArray(marketIds, 10);
  const allBooks = [];

  for (const chunk of chunks) {
    const books = await betfair.listMarketBook(chunk);
    allBooks.push(...books);
  }

  return allBooks;
}

async function scanInplayOpportunities(options = {}) {
  const debug = options.debug === true;
  const betfair = new BetfairClient();

  const rejections = [];

  const eventsResult = await betfair.listEvents({
    eventTypeIds: ['1'],
    inPlayOnly: true
  });

  const events = eventsResult.map(row => row.event).filter(Boolean);
  const eventIds = events.map(event => String(event.id));

  if (!eventIds.length) {
    return {
      totalEvents: 0,
      totalMarkets: 0,
      totalRawOpportunities: 0,
      totalOpportunities: 0,
      opportunities: [],
      rejections: debug ? rejections : undefined
    };
  }

  const catalogues = await betfair.listMarketCatalogue(
    {
      eventIds,
      marketTypeCodes: MARKET_TYPES,
      inPlayOnly: true
    },
    '200'
  );

  const marketIds = catalogues.map(m => m.marketId);

  if (!marketIds.length) {
    return {
      totalEvents: events.length,
      totalMarkets: 0,
      totalRawOpportunities: 0,
      totalOpportunities: 0,
      opportunities: [],
      rejections: debug ? rejections : undefined
    };
  }

  const marketBooks = await listMarketBooksInChunks(betfair, marketIds);

  const catalogueByMarketId = new Map(
    catalogues.map(catalogue => [catalogue.marketId, catalogue])
  );

  const rawOpportunities = [];

  for (const marketBook of marketBooks) {
    const catalogue = catalogueByMarketId.get(marketBook.marketId);

    if (!catalogue) {
      addRejection(rejections, {
        scope: 'market',
        marketId: marketBook.marketId,
        reason: 'missing_market_catalogue'
      });
      continue;
    }

    const marketType = getMarketType(catalogue);
    const eventId = String(catalogue.event.id);
    const eventName = catalogue.event.name;

    if (marketBook.status !== 'OPEN') {
      addRejection(rejections, {
        scope: 'market',
        eventName,
        marketId: marketBook.marketId,
        marketType,
        reason: 'market_not_open',
        status: marketBook.status
      });
      continue;
    }

    if (marketBook.inplay !== true) {
      addRejection(rejections, {
        scope: 'market',
        eventName,
        marketId: marketBook.marketId,
        marketType,
        reason: 'market_not_inplay'
      });
      continue;
    }

    if (!MARKET_TYPES.includes(marketType)) {
      addRejection(rejections, {
        scope: 'market',
        eventName,
        marketId: marketBook.marketId,
        marketType,
        reason: 'unsupported_market_type'
      });
      continue;
    }

    const fixtureMatch = await findFixture(eventId, eventName);

    if (fixtureMatch.confidence !== 'high') {
      addRejection(rejections, {
        scope: 'event',
        eventId,
        eventName,
        marketId: marketBook.marketId,
        marketType,
        reason: 'fixture_mapping_not_confident',
        mappingMethod: fixtureMatch.method,
        mappingConfidence: fixtureMatch.confidence
      });
      continue;
    }

    for (const runner of marketBook.runners || []) {
      if (runner.status !== 'ACTIVE') {
        addRejection(rejections, {
          scope: 'runner',
          eventId,
          eventName,
          marketId: marketBook.marketId,
          marketType,
          selectionId: runner.selectionId,
          reason: 'runner_not_active',
          status: runner.status
        });
        continue;
      }

      const runnerName = getRunnerName(catalogue, runner.selectionId);

      if (!runnerName) {
        addRejection(rejections, {
          scope: 'runner',
          eventId,
          eventName,
          marketId: marketBook.marketId,
          marketType,
          selectionId: runner.selectionId,
          reason: 'runner_name_missing'
        });
        continue;
      }

      const backOdd = getBestBackOdd(runner);

      if (!backOdd) {
        addRejection(rejections, {
          scope: 'runner',
          eventId,
          eventName,
          marketId: marketBook.marketId,
          marketType,
          runnerName,
          selectionId: runner.selectionId,
          reason: 'missing_back_odd'
        });
        continue;
      }

      if (backOdd < MIN_BACK_ODD) {
        addRejection(rejections, {
          scope: 'runner',
          eventId,
          eventName,
          marketId: marketBook.marketId,
          marketType,
          runnerName,
          selectionId: runner.selectionId,
          reason: 'odd_below_minimum',
          backOdd,
          minBackOdd: MIN_BACK_ODD
        });
        continue;
      }

      const impliedProbability = impliedProbabilityFromOdd(backOdd);

      const model = await getBasicModelProbability({
        fixtureId: fixtureMatch.fixtureId,
        marketType,
        runnerName
      });

      if (!model) {
        addRejection(rejections, {
          scope: 'runner',
          eventId,
          eventName,
          marketId: marketBook.marketId,
          marketType,
          runnerName,
          selectionId: runner.selectionId,
          reason: 'missing_model_probability'
        });
        continue;
      }

      const edge = Number(
        (model.modelProbability - impliedProbability).toFixed(6)
      );

      if (edge < MIN_EDGE) {
        addRejection(rejections, {
          scope: 'runner',
          eventId,
          eventName,
          marketId: marketBook.marketId,
          marketType,
          runnerName,
          selectionId: runner.selectionId,
          reason: 'edge_below_minimum',
          backOdd,
          impliedProbability,
          modelProbability: model.modelProbability,
          edge,
          minEdge: MIN_EDGE
        });
        continue;
      }

      const riskLevel =
        edge >= 0.08 ? 'low' :
        edge >= 0.04 ? 'medium' :
        'high';

      const confidenceScore = calculateConfidenceScore({
        edge,
        riskLevel,
        mappingConfidence: fixtureMatch.confidence,
        hasPrediction: model.statsSummary.hasPrediction,
        recentStatsRows: model.statsSummary.recentStatsRows,
        compareUrl: fixtureMatch.compareUrl,
        marketType
      });

      const opportunity = {
        betfairEventId: eventId,
        betfairEventName: eventName,
        fixtureId: fixtureMatch.fixtureId,
        homeTeam: fixtureMatch.homeTeam,
        awayTeam: fixtureMatch.awayTeam,

        liveContext: getLiveContext(),

        marketId: marketBook.marketId,
        marketType,
        marketStatus: marketBook.status,
        selectionId: runner.selectionId,
        runnerName,

        backOdd,
        impliedProbability,
        modelProbability: model.modelProbability,
        edge,
        riskLevel,
        confidenceScore,
        alert: false,

        reason: model.reason,
        statsSummary: {
          ...model.statsSummary,
          mappingMethod: fixtureMatch.method,
          compareUrl: fixtureMatch.compareUrl || null
        }
      };

      opportunity.alert = shouldTriggerAlert(opportunity);

      rawOpportunities.push(opportunity);
    }
  }

  const opportunities = groupOpportunities(rawOpportunities);

  return {
    totalEvents: events.length,
    totalMarkets: catalogues.length,
    totalRawOpportunities: rawOpportunities.length,
    totalOpportunities: opportunities.length,
    opportunities,
    rejections: debug ? rejections : undefined
  };
}

module.exports = {
  scanInplayOpportunities
};

// const BetfairClient = require('../betfair/client');
// const db = require('../db/connection');

// const MARKET_TYPES = [
//   'MATCH_ODDS',
//   'DOUBLE_CHANCE',
//   'OVER_UNDER_15',
//   'OVER_UNDER_25'
// ];

// const MIN_BACK_ODD = 1.20;

// function getBestBackOdd(runner) {
//   const prices = runner?.ex?.availableToBack || [];

//   if (!prices.length) {
//     return null;
//   }

//   return Number(prices[0].price);
// }

// function impliedProbabilityFromOdd(odd) {
//   return Number((1 / odd).toFixed(6));
// }

// function getMarketType(catalogue) {
//   return catalogue?.description?.marketType || null;
// }

// function getRunnerName(catalogue, selectionId) {
//   const runner = catalogue.runners.find(
//     (r) => String(r.selectionId) === String(selectionId)
//   );

//   return runner ? runner.runnerName : null;
// }

// function parseTeamsFromEventName(eventName) {
//   if (!eventName) return null;

//   const separators = [' v ', ' vs ', ' - '];

//   for (const separator of separators) {
//     if (eventName.includes(separator)) {
//       const [homeTeam, awayTeam] = eventName.split(separator).map(v => v.trim());

//       if (homeTeam && awayTeam) {
//         return { homeTeam, awayTeam };
//       }
//     }
//   }

//   return null;
// }

// async function findFixture(eventId, eventName) {
//   const mapped = await db.query(
//     `
//     SELECT
//       f.id AS fixture_id,
//       ht.name AS home_team,
//       at.name AS away_team,
//       f.compare_url
//     FROM betfair_tip_mappings btm
//     JOIN fixtures f ON f.id = btm.fixture_id
//     LEFT JOIN teams ht ON ht.id = f.home_team_id
//     LEFT JOIN teams at ON at.id = f.away_team_id
//     WHERE btm.betfair_event_id = $1
//     ORDER BY btm.updated_at DESC NULLS LAST, btm.id DESC
//     LIMIT 1
//     `,
//     [String(eventId)]
//   );

//   if (mapped.rows.length) {
//     return {
//       fixtureId: mapped.rows[0].fixture_id,
//       homeTeam: mapped.rows[0].home_team,
//       awayTeam: mapped.rows[0].away_team,
//       compareUrl: mapped.rows[0].compare_url,
//       confidence: 'high',
//       method: 'betfair_tip_mappings'
//     };
//   }

//   const parsed = parseTeamsFromEventName(eventName);

//   if (!parsed) {
//     return {
//       fixtureId: null,
//       homeTeam: null,
//       awayTeam: null,
//       compareUrl: null,
//       confidence: 'none',
//       method: 'parse_failed'
//     };
//   }

//   const byNames = await db.query(
//     `
//     SELECT
//       f.id AS fixture_id,
//       ht.name AS home_team,
//       at.name AS away_team,
//       f.compare_url
//     FROM fixtures f
//     JOIN teams ht ON ht.id = f.home_team_id
//     JOIN teams at ON at.id = f.away_team_id
//     WHERE LOWER(ht.name) = LOWER($1)
//       AND LOWER(at.name) = LOWER($2)
//     ORDER BY f.id DESC
//     LIMIT 1
//     `,
//     [parsed.homeTeam, parsed.awayTeam]
//   );

//   if (byNames.rows.length) {
//     return {
//       fixtureId: byNames.rows[0].fixture_id,
//       homeTeam: byNames.rows[0].home_team,
//       awayTeam: byNames.rows[0].away_team,
//       compareUrl: byNames.rows[0].compare_url,
//       confidence: 'high',
//       method: 'team_name_exact'
//     };
//   }

//   return {
//     fixtureId: null,
//     homeTeam: parsed.homeTeam,
//     awayTeam: parsed.awayTeam,
//     compareUrl: null,
//     confidence: 'low',
//     method: 'parsed_only'
//   };
// }
// async function getBasicModelProbability({ fixtureId, marketType, runnerName }) {
//   if (!fixtureId) {
//     return null;
//   }

//   const prediction = await db.query(
//     `
//     SELECT *
//     FROM scraped_predictions
//     WHERE fixture_id = $1
//     ORDER BY id DESC
//     LIMIT 1
//     `,
//     [fixtureId]
//   );

//   const recentStats = await db.query(
//     `
//     SELECT *
//     FROM team_recent_stats
//     WHERE fixture_id = $1
//     ORDER BY id DESC
//     `,
//     [fixtureId]
//   );

//   const predictionRow = prediction.rows[0] || null;
//   const statsRows = recentStats.rows || [];

//   const pick = String(runnerName || '').toLowerCase();
//   const market = String(marketType || '').toUpperCase();

//   let probability = null;
//   const reasons = [];

//   if (predictionRow) {
//     const homeProb =
//       Number(predictionRow.home_probability || predictionRow.home_prob || predictionRow.prob_home || 0);

//     const drawProb =
//       Number(predictionRow.draw_probability || predictionRow.draw_prob || predictionRow.prob_draw || 0);

//     const awayProb =
//       Number(predictionRow.away_probability || predictionRow.away_prob || predictionRow.prob_away || 0);

//     const hp = homeProb > 1 ? homeProb / 100 : homeProb;
//     const dp = drawProb > 1 ? drawProb / 100 : drawProb;
//     const ap = awayProb > 1 ? awayProb / 100 : awayProb;

//     if (market === 'MATCH_ODDS') {
//       if (pick.includes('draw')) probability = dp;
//       else if (predictionRow.home_team && pick.includes(String(predictionRow.home_team).toLowerCase())) probability = hp;
//       else if (predictionRow.away_team && pick.includes(String(predictionRow.away_team).toLowerCase())) probability = ap;
//     }

//     if (market === 'DOUBLE_CHANCE') {
//       if (pick.includes('1x') || pick.includes('home or draw')) probability = hp + dp;
//       if (pick.includes('x2') || pick.includes('draw or away')) probability = dp + ap;
//       if (pick.includes('12') || pick.includes('home or away')) probability = hp + ap;
//     }

//     if (probability) {
//       reasons.push('Statarea prediction probability used');
//     }
//   }

//   if (!probability && statsRows.length) {
//     const avgGoalsFor = statsRows
//       .map(r => Number(r.avg_goals_for))
//       .filter(Number.isFinite);

//     const avgGoalsAgainst = statsRows
//       .map(r => Number(r.avg_goals_against))
//       .filter(Number.isFinite);

//     const avgFor = avgGoalsFor.length
//       ? avgGoalsFor.reduce((a, b) => a + b, 0) / avgGoalsFor.length
//       : null;

//     const avgAgainst = avgGoalsAgainst.length
//       ? avgGoalsAgainst.reduce((a, b) => a + b, 0) / avgGoalsAgainst.length
//       : null;

//     const goalTrend =
//       avgFor !== null && avgAgainst !== null
//         ? avgFor + avgAgainst
//         : null;

//     if (market === 'OVER_UNDER_15' && goalTrend !== null) {
//       if (pick.includes('over')) probability = goalTrend >= 2.0 ? 0.72 : 0.56;
//       if (pick.includes('under')) probability = goalTrend < 2.0 ? 0.62 : 0.42;
//       reasons.push('Recent goals trend used');
//     }

//     if (market === 'OVER_UNDER_25' && goalTrend !== null) {
//       if (pick.includes('over')) probability = goalTrend >= 2.8 ? 0.66 : 0.48;
//       if (pick.includes('under')) probability = goalTrend < 2.8 ? 0.60 : 0.44;
//       reasons.push('Recent goals trend used');
//     }
//   }

//   if (!probability || probability <= 0 || probability >= 1) {
//     return null;
//   }

//   return {
//     modelProbability: Number(probability.toFixed(6)),
//     reason: reasons.join('. '),
//     statsSummary: {
//       hasPrediction: Boolean(predictionRow),
//       recentStatsRows: statsRows.length
//     }
//   };
// }

// async function scanInplayOpportunities() {
//   const betfair = new BetfairClient();

//   const eventsResult = await betfair.listEvents({
//     eventTypeIds: ['1'],
//     inPlayOnly: true
//   });

//   const events = eventsResult.map(row => row.event).filter(Boolean);
//   const eventIds = events.map(event => String(event.id));

//   if (!eventIds.length) {
//     return {
//       totalEvents: 0,
//       totalMarkets: 0,
//       totalOpportunities: 0,
//       opportunities: []
//     };
//   }

//   const catalogues = await betfair.listMarketCatalogue(
//     {
//       eventIds,
//       marketTypeCodes: MARKET_TYPES,
//       inPlayOnly: true
//     },
//     '200'
//   );

//   const marketIds = catalogues.map(m => m.marketId);

//   if (!marketIds.length) {
//     return {
//       totalEvents: events.length,
//       totalMarkets: 0,
//       totalOpportunities: 0,
//       opportunities: []
//     };
//   }

//   const marketBooks = await betfair.listMarketBook(marketIds);

//   const catalogueByMarketId = new Map(
//     catalogues.map(catalogue => [catalogue.marketId, catalogue])
//   );

//   const opportunities = [];

//   for (const marketBook of marketBooks) {
//     if (marketBook.status !== 'OPEN') continue;
//     if (marketBook.inplay !== true) continue;

//     const catalogue = catalogueByMarketId.get(marketBook.marketId);
//     if (!catalogue) continue;

//     const marketType = getMarketType(catalogue);
//     if (!MARKET_TYPES.includes(marketType)) continue;

//     const eventId = String(catalogue.event.id);
//     const eventName = catalogue.event.name;

//     const fixtureMatch = await findFixture(eventId, eventName);

//     if (fixtureMatch.confidence !== 'high') {
//       continue;
//     }

//     for (const runner of marketBook.runners || []) {
//       if (runner.status !== 'ACTIVE') continue;

//       const backOdd = getBestBackOdd(runner);
//       if (!backOdd || backOdd < MIN_BACK_ODD) continue;

//       const runnerName = getRunnerName(catalogue, runner.selectionId);
//       if (!runnerName) continue;

//       const impliedProbability = impliedProbabilityFromOdd(backOdd);

//       const model = await getBasicModelProbability({
//         fixtureId: fixtureMatch.fixtureId,
//         marketType,
//         runnerName
//       });

//       if (!model) continue;

//       const edge = Number(
//         (model.modelProbability - impliedProbability).toFixed(6)
//       );

//       if (edge <= 0) continue;

//       opportunities.push({
//         betfairEventId: eventId,
//         betfairEventName: eventName,
//         fixtureId: fixtureMatch.fixtureId,
//         homeTeam: fixtureMatch.homeTeam,
//         awayTeam: fixtureMatch.awayTeam,
//         marketId: marketBook.marketId,
//         marketType,
//         marketStatus: marketBook.status,
//         selectionId: runner.selectionId,
//         runnerName,
//         backOdd,
//         impliedProbability,
//         modelProbability: model.modelProbability,
//         edge,
//         riskLevel: edge >= 0.08 ? 'low' : edge >= 0.04 ? 'medium' : 'high',
//         reason: model.reason,
//         statsSummary: {
//           ...model.statsSummary,
//           mappingMethod: fixtureMatch.method,
//           compareUrl: fixtureMatch.compareUrl || null
//         }
//       });
//     }
//   }

//   return {
//     totalEvents: events.length,
//     totalMarkets: catalogues.length,
//     totalOpportunities: opportunities.length,
//     opportunities
//   };
// }

// module.exports = {
//   scanInplayOpportunities
// };
