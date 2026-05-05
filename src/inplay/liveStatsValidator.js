const axios = require('axios');
const env = require('../config/env');

const BETSAPI_BASE_URL = 'https://api.b365api.com/v1';

function getToken() {
  const token = env.BETSAPI_TOKEN || process.env.BETSAPI_TOKEN;

  if (!token) {
    throw new Error('BETSAPI_TOKEN is missing from docker/.env');
  }

  return token;
}

function normalizeTeamName(name) {
  return String(name || '')
    .toLowerCase()
    .replace(/\[[0-9]+\]/g, '')
    .replace(/\([^)]*\)/g, '')
    .replace(/\bfc\b/g, '')
    .replace(/\bafc\b/g, '')
    .replace(/\bsc\b/g, '')
    .replace(/\breserves\b/g, 'reserve')
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function tokenize(name) {
  return normalizeTeamName(name)
    .split(' ')
    .filter(t => t.length >= 3);
}

function similarity(a, b) {
  const aTokens = tokenize(a);
  const bTokens = tokenize(b);

  if (!aTokens.length || !bTokens.length) return 0;

  let matches = 0;

  for (const token of aTokens) {
    if (bTokens.includes(token)) {
      matches++;
    }
  }

  return matches / Math.max(aTokens.length, bTokens.length);
}

function parseScore(scoreText) {
  const match = String(scoreText || '').match(/(\d+)\s*-\s*(\d+)/);

  if (!match) {
    return {
      homeGoals: null,
      awayGoals: null,
      totalGoals: null,
      score: null
    };
  }

  const homeGoals = Number(match[1]);
  const awayGoals = Number(match[2]);

  return {
    homeGoals,
    awayGoals,
    totalGoals: homeGoals + awayGoals,
    score: `${homeGoals}-${awayGoals}`
  };
}

function pick(obj, paths) {
  for (const path of paths) {
    const parts = path.split('.');
    let current = obj;

    for (const part of parts) {
      if (current === null || current === undefined) {
        current = undefined;
        break;
      }

      current = current[part];
    }

    if (current !== undefined && current !== null && current !== '') {
      return current;
    }
  }

  return null;
}

async function betsApiGet(path, params = {}) {
  const response = await axios.get(`${BETSAPI_BASE_URL}${path}`, {
    timeout: 15000,
    params: {
      token: getToken(),
      ...params
    }
  });

  if (response.data && response.data.success === 0) {
    throw new Error(
      `BetsAPI error: ${response.data.error || JSON.stringify(response.data)}`
    );
  }

  return response.data;
}

function normalizeInplayGame(row) {
  const home =
    pick(row, [
      'home.name',
      'home',
      'home_team',
      'team_home',
      'event.home.name'
    ]) || '';

  const away =
    pick(row, [
      'away.name',
      'away',
      'away_team',
      'team_away',
      'event.away.name'
    ]) || '';

  const id =
    pick(row, [
      'id',
      'event_id',
      'betfair_event_id',
      'bf_event_id',
      'event.id'
    ]);

  const league =
    pick(row, [
      'league.name',
      'competition.name',
      'competition',
      'league',
      'event.competition.name'
    ]) || '';

  const score =
    pick(row, [
      'ss',
      'score',
      'scores',
      'result',
      'event.ss'
    ]) || '';

  const timer =
    pick(row, [
      'timer.tm',
      'timer',
      'time',
      'minute',
      'event.timer.tm',
      'event.openDate'
    ]);

  const eventName =
    pick(row, [
      'name',
      'event.name',
      'event_name'
    ]) || '';

  return {
    id: id ? String(id) : null,
    ourEventId: row.our_event_id ? String(row.our_event_id) : null,
    home: String(home),
    away: String(away),
    eventName: String(eventName),
    league: String(league),
    score: String(score || ''),
    minute: timer !== null && timer !== undefined ? String(timer) : null,
    raw: row
  };
}

async function getBetsApiInplayGames() {
  const data = await betsApiGet('/betfair/ex/inplay', {
    sport_id: 1
  });

  const results =
    data.results ||
    data.result ||
    data.events ||
    data.data ||
    [];

  if (!Array.isArray(results)) {
    throw new Error(
      `Unexpected BetsAPI ex/inplay response shape: ${JSON.stringify(data).slice(0, 800)}`
    );
  }

  return results.map(normalizeInplayGame);
}

function findBestBetsApiMatch(opportunity, games) {
  const home = opportunity.homeTeam || '';
  const away = opportunity.awayTeam || '';
  const eventName = opportunity.betfairEventName || '';

  let best = null;

  for (const game of games) {
    const homeScore =
      similarity(home, game.home) +
      similarity(away, game.away);

    const reversedScore =
      similarity(home, game.away) +
      similarity(away, game.home);

    const score = Math.max(homeScore, reversedScore) / 2;
    const reversed = reversedScore > homeScore;

    const eventBonus =
      eventName &&
      normalizeTeamName(eventName).includes(tokenize(game.home)[0] || '___')
        ? 0.05
        : 0;

    const finalScore = Math.min(1, score + eventBonus);

    if (!best || finalScore > best.matchScore) {
      best = {
        ...game,
        matchScore: Number(finalScore.toFixed(3)),
        reversed
      };
    }
  }

  if (!best || best.matchScore < 0.55) {
    return null;
  }

  return best;
}

function normalizeStatsFromEventResponse(data) {
  const source =
    data.results ||
    data.result ||
    data.event ||
    data.data ||
    data;

  const rawStats =
    pick(source, [
      'stats',
      'statistics',
      'extra.stats',
      'extra.statistics'
    ]) || [];

  const stats = {};

  if (Array.isArray(rawStats)) {
    for (const item of rawStats) {
      const label =
        item.type ||
        item.name ||
        item.label ||
        item.key;

      if (!label) continue;

      const key = String(label)
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '_')
        .replace(/^_+|_+$/g, '');

      stats[key] = {
        home: Number(item.home ?? item.home_value ?? item.value_home ?? item[0]),
        away: Number(item.away ?? item.away_value ?? item.value_away ?? item[1]),
        label
      };
    }
  }

  if (rawStats && typeof rawStats === 'object' && !Array.isArray(rawStats)) {
    for (const [label, value] of Object.entries(rawStats)) {
      const key = String(label)
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '_')
        .replace(/^_+|_+$/g, '');

      if (Array.isArray(value)) {
        stats[key] = {
          home: Number(value[0]),
          away: Number(value[1]),
          label
        };
      } else if (value && typeof value === 'object') {
        stats[key] = {
          home: Number(value.home ?? value.home_value ?? value.value_home),
          away: Number(value.away ?? value.away_value ?? value.value_away),
          label
        };
      }
    }
  }

  const score =
    pick(source, ['ss', 'score', 'scores', 'result']) ||
    pick(data, ['ss', 'score']);

  const minute =
    pick(source, ['timer.tm', 'timer', 'time', 'minute']) ||
    pick(data, ['timer.tm', 'timer', 'time', 'minute']);

  return {
    score: score ? String(score) : null,
    minute: minute !== null && minute !== undefined ? String(minute) : null,
    stats,
    raw: data
  };
}

async function getBetsApiMatchStats(eventId) {
  const data = await betsApiGet('/event/view', {
    event_id: eventId
  });

  return normalizeStatsFromEventResponse(data);
}

function getStat(stats, keys) {
  for (const key of keys) {
    if (stats[key]) return stats[key];
  }

  return { home: null, away: null };
}

function numberOrZero(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

function validateGoalsOpportunity(opportunity, liveStats) {
  const pickName = String(opportunity.runnerName || '').toLowerCase();
  const market = String(opportunity.marketType || '').toUpperCase();

  const isOver = pickName.includes('over');
  const isUnder = pickName.includes('under');

  const scoreData = parseScore(liveStats.score);

  const attacks = getStat(liveStats.stats, ['attacks', 'attack']);
  const dangerousAttacks = getStat(liveStats.stats, ['dangerous_attacks', 'dangerous_attack']);
  const onTarget = getStat(liveStats.stats, ['on_target', 'shots_on_target', 'shot_on_target']);
  const offTarget = getStat(liveStats.stats, ['off_target', 'shots_off_target', 'shot_off_target']);
  const corners = getStat(liveStats.stats, ['corners', 'corner']);

  const totalOnTarget = numberOrZero(onTarget.home) + numberOrZero(onTarget.away);
  const totalOffTarget = numberOrZero(offTarget.home) + numberOrZero(offTarget.away);
  const totalCorners = numberOrZero(corners.home) + numberOrZero(corners.away);
  const totalDangerousAttacks = numberOrZero(dangerousAttacks.home) + numberOrZero(dangerousAttacks.away);
  const totalAttacks = numberOrZero(attacks.home) + numberOrZero(attacks.away);

  let pressureScore = 0;

  pressureScore += Math.min(totalOnTarget * 12, 36);
  pressureScore += Math.min(totalOffTarget * 4, 20);
  pressureScore += Math.min(totalCorners * 6, 24);
  pressureScore += Math.min(totalDangerousAttacks / 2, 30);
  pressureScore += Math.min(totalAttacks / 8, 15);

  pressureScore = Math.min(100, Math.round(pressureScore));

  let validation = 'neutral';
  let riskAdjustment = 0;
  const reasons = [];

  reasons.push(
    `Live pressure: SOT ${totalOnTarget}, off target ${totalOffTarget}, corners ${totalCorners}, dangerous attacks ${totalDangerousAttacks}`
  );

  if (scoreData.score) {
    reasons.push(`Score ${scoreData.score}`);
  }

  if (market === 'OVER_UNDER_15') {
    if (isOver) {
      if (scoreData.totalGoals >= 2) {
        validation = 'supports';
        riskAdjustment = -2;
        reasons.push('Over 1.5 already landed by current score');
      } else if (pressureScore >= 55) {
        validation = 'supports';
        riskAdjustment = -1;
        reasons.push('Live pressure supports Over 1.5');
      } else if (pressureScore <= 25) {
        validation = 'contradicts';
        riskAdjustment = 2;
        reasons.push('Low live pressure contradicts Over 1.5');
      }
    }

    if (isUnder) {
      if (scoreData.totalGoals >= 2) {
        validation = 'contradicts';
        riskAdjustment = 3;
        reasons.push('Under 1.5 already lost by current score');
      } else if (pressureScore <= 30) {
        validation = 'supports';
        riskAdjustment = -1;
        reasons.push('Low live pressure supports Under 1.5');
      } else if (pressureScore >= 55) {
        validation = 'contradicts';
        riskAdjustment = 2;
        reasons.push('High live pressure contradicts Under 1.5');
      }
    }
  }

  if (market === 'OVER_UNDER_25') {
    if (isOver) {
      if (scoreData.totalGoals >= 3) {
        validation = 'supports';
        riskAdjustment = -2;
        reasons.push('Over 2.5 already landed by current score');
      } else if (pressureScore >= 65) {
        validation = 'supports';
        riskAdjustment = -1;
        reasons.push('Live pressure supports Over 2.5');
      } else if (pressureScore <= 35) {
        validation = 'contradicts';
        riskAdjustment = 2;
        reasons.push('Low live pressure contradicts Over 2.5');
      }
    }

    if (isUnder) {
      if (scoreData.totalGoals >= 3) {
        validation = 'contradicts';
        riskAdjustment = 3;
        reasons.push('Under 2.5 already lost by current score');
      } else if (pressureScore <= 40) {
        validation = 'supports';
        riskAdjustment = -1;
        reasons.push('Low live pressure supports Under 2.5');
      } else if (pressureScore >= 70) {
        validation = 'contradicts';
        riskAdjustment = 2;
        reasons.push('High live pressure contradicts Under 2.5');
      }
    }
  }

  return {
    liveStatsFound: true,
    validation,
    livePressureScore: pressureScore,
    riskAdjustment,
    reason: reasons.join('. '),
    score: scoreData.score,
    minute: liveStats.minute,
    stats: {
      attacks,
      dangerousAttacks,
      onTarget,
      offTarget,
      corners,
      totals: {
        onTarget: totalOnTarget,
        offTarget: totalOffTarget,
        corners: totalCorners,
        dangerousAttacks: totalDangerousAttacks,
        attacks: totalAttacks
      }
    }
  };
}

async function validateLiveStats(opportunity) {
  try {
    const games = await getBetsApiInplayGames();
    const match = findBestBetsApiMatch(opportunity, games);

    if (!match) {
      return {
        liveStatsFound: false,
        validation: 'unknown',
        livePressureScore: null,
        riskAdjustment: 0,
        reason: 'No confident BetsAPI in-play match found',
        match: null
      };
    }

    const statsEventId = match.ourEventId || match.id;
    const liveStats = await getBetsApiMatchStats(statsEventId);

    const market = String(opportunity.marketType || '').toUpperCase();

    const validation = market.startsWith('OVER_UNDER')
      ? validateGoalsOpportunity(opportunity, liveStats)
      : {
          liveStatsFound: true,
          validation: 'neutral',
          livePressureScore: null,
          riskAdjustment: 0,
          reason: 'Live result-market validation not implemented yet',
          score: liveStats.score,
          minute: liveStats.minute,
          stats: liveStats.stats
        };

    return {
      ...validation,
      source: 'betsapi_api',
      betsapiEventId: match.id,
      matchScore: match.matchScore,
      matchedHome: match.home,
      matchedAway: match.away,
      reversed: match.reversed,
      betsapiStatsEventId: statsEventId
    };
  } catch (error) {
    return {
      liveStatsFound: false,
      validation: 'error',
      livePressureScore: null,
      riskAdjustment: 0,
      reason: `BetsAPI API validation failed: ${error.message}`,
      match: null
    };
  }
}

module.exports = {
  validateLiveStats,
  getBetsApiInplayGames,
  getBetsApiMatchStats,
  findBestBetsApiMatch,
  normalizeTeamName,
  similarity
};
