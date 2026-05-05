const BetfairClient = require('../betfair/client');
const db = require('../db/connection');

const MARKET_TYPES = [
  'MATCH_ODDS',
  'DOUBLE_CHANCE',
  'OVER_UNDER_15',
  'OVER_UNDER_25'
];

const MIN_BACK_ODD = 1.20;

function getBestBackOdd(runner) {
  const prices = runner?.ex?.availableToBack || [];

  if (!prices.length) {
    return null;
  }

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
    (r) => String(r.selectionId) === String(selectionId)
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
  if (!fixtureId) {
    return null;
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

  if (predictionRow) {
    const homeProb =
      Number(predictionRow.home_probability || predictionRow.home_prob || predictionRow.prob_home || 0);

    const drawProb =
      Number(predictionRow.draw_probability || predictionRow.draw_prob || predictionRow.prob_draw || 0);

    const awayProb =
      Number(predictionRow.away_probability || predictionRow.away_prob || predictionRow.prob_away || 0);

    const hp = homeProb > 1 ? homeProb / 100 : homeProb;
    const dp = drawProb > 1 ? drawProb / 100 : drawProb;
    const ap = awayProb > 1 ? awayProb / 100 : awayProb;

    if (market === 'MATCH_ODDS') {
      if (pick.includes('draw')) probability = dp;
      else if (predictionRow.home_team && pick.includes(String(predictionRow.home_team).toLowerCase())) probability = hp;
      else if (predictionRow.away_team && pick.includes(String(predictionRow.away_team).toLowerCase())) probability = ap;
    }

    if (market === 'DOUBLE_CHANCE') {
      if (pick.includes('1x') || pick.includes('home or draw')) probability = hp + dp;
      if (pick.includes('x2') || pick.includes('draw or away')) probability = dp + ap;
      if (pick.includes('12') || pick.includes('home or away')) probability = hp + ap;
    }

    if (probability) {
      reasons.push('Statarea prediction probability used');
    }
  }

  if (!probability && statsRows.length) {
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

    const goalTrend =
      avgFor !== null && avgAgainst !== null
        ? avgFor + avgAgainst
        : null;

    if (market === 'OVER_UNDER_15' && goalTrend !== null) {
      if (pick.includes('over')) probability = goalTrend >= 2.0 ? 0.72 : 0.56;
      if (pick.includes('under')) probability = goalTrend < 2.0 ? 0.62 : 0.42;
      reasons.push('Recent goals trend used');
    }

    if (market === 'OVER_UNDER_25' && goalTrend !== null) {
      if (pick.includes('over')) probability = goalTrend >= 2.8 ? 0.66 : 0.48;
      if (pick.includes('under')) probability = goalTrend < 2.8 ? 0.60 : 0.44;
      reasons.push('Recent goals trend used');
    }
  }

  if (!probability || probability <= 0 || probability >= 1) {
    return null;
  }

  return {
    modelProbability: Number(probability.toFixed(6)),
    reason: reasons.join('. '),
    statsSummary: {
      hasPrediction: Boolean(predictionRow),
      recentStatsRows: statsRows.length
    }
  };
}

async function scanInplayOpportunities() {
  const betfair = new BetfairClient();

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
      totalOpportunities: 0,
      opportunities: []
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
      totalOpportunities: 0,
      opportunities: []
    };
  }

  const marketBooks = await betfair.listMarketBook(marketIds);

  const catalogueByMarketId = new Map(
    catalogues.map(catalogue => [catalogue.marketId, catalogue])
  );

  const opportunities = [];

  for (const marketBook of marketBooks) {
    if (marketBook.status !== 'OPEN') continue;
    if (marketBook.inplay !== true) continue;

    const catalogue = catalogueByMarketId.get(marketBook.marketId);
    if (!catalogue) continue;

    const marketType = getMarketType(catalogue);
    if (!MARKET_TYPES.includes(marketType)) continue;

    const eventId = String(catalogue.event.id);
    const eventName = catalogue.event.name;

    const fixtureMatch = await findFixture(eventId, eventName);

    if (fixtureMatch.confidence !== 'high') {
      continue;
    }

    for (const runner of marketBook.runners || []) {
      if (runner.status !== 'ACTIVE') continue;

      const backOdd = getBestBackOdd(runner);
      if (!backOdd || backOdd < MIN_BACK_ODD) continue;

      const runnerName = getRunnerName(catalogue, runner.selectionId);
      if (!runnerName) continue;

      const impliedProbability = impliedProbabilityFromOdd(backOdd);

      const model = await getBasicModelProbability({
        fixtureId: fixtureMatch.fixtureId,
        marketType,
        runnerName
      });

      if (!model) continue;

      const edge = Number(
        (model.modelProbability - impliedProbability).toFixed(6)
      );

      if (edge <= 0) continue;

      opportunities.push({
        betfairEventId: eventId,
        betfairEventName: eventName,
        fixtureId: fixtureMatch.fixtureId,
        homeTeam: fixtureMatch.homeTeam,
        awayTeam: fixtureMatch.awayTeam,
        marketId: marketBook.marketId,
        marketType,
        marketStatus: marketBook.status,
        selectionId: runner.selectionId,
        runnerName,
        backOdd,
        impliedProbability,
        modelProbability: model.modelProbability,
        edge,
        riskLevel: edge >= 0.08 ? 'low' : edge >= 0.04 ? 'medium' : 'high',
        reason: model.reason,
        statsSummary: {
          ...model.statsSummary,
          mappingMethod: fixtureMatch.method,
          compareUrl: fixtureMatch.compareUrl || null
        }
      });
    }
  }

  return {
    totalEvents: events.length,
    totalMarkets: catalogues.length,
    totalOpportunities: opportunities.length,
    opportunities
  };
}

module.exports = {
  scanInplayOpportunities
};
