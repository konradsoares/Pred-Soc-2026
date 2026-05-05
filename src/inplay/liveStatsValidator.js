const axios = require('axios');
const cheerio = require('cheerio');

const BETSAPI_INPLAY_URL = 'https://betsapi.com/cip/soccer';
const BETSAPI_BASE_URL = 'https://betsapi.com';

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

function cleanText(value) {
  return String(value || '')
    .replace(/\s+/g, ' ')
    .trim();
}

async function fetchHtml(url) {
  const response = await axios.get(url, {
    timeout: 15000,
    headers: {
      'User-Agent': 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.7727.114 Mobile Safari/537.36',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
    }
  });

  return response.data;
}

async function getBetsApiInplayGames() {
  const html = await fetchHtml(BETSAPI_INPLAY_URL);
  const $ = cheerio.load(html);

  const games = [];

  $('tr[id^="r_"]').each((_, row) => {
    const $row = $(row);

    const rowId = $row.attr('id') || '';
    const betsapiId = rowId.replace('r_', '');

    const link = $row.find('a[href^="/soccer/r/"]').first();
    const href = link.attr('href');

    if (!href) return;

    const cells = $row.find('td').map((i, td) => cleanText($(td).text())).get();

    /*
      BetsAPI table normally gives:
      League | Time | Home | Score | Away | 1 | X | 2
      But there may be hidden/empty cells, so we also rely on ids.
    */

    const home =
      cleanText($row.find(`td[id$="_home"]`).text()) ||
      cells[2] ||
      '';

    const score =
      cleanText($row.find(`td[id$="_ss"]`).text()) ||
      cells[3] ||
      '';

    const away =
      cleanText($row.find(`td[id$="_away"]`).text()) ||
      cells[4] ||
      '';

    const league =
      cleanText($row.find('td').first().text()) ||
      cells[0] ||
      '';

    games.push({
      betsapiId,
      league,
      home,
      away,
      score,
      url: `${BETSAPI_BASE_URL}${href}`,
      rawCells: cells
    });
  });

  return games;
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

    const eventNameBonus =
      eventName &&
      normalizeTeamName(eventName).includes(normalizeTeamName(game.home).split(' ')[0])
        ? 0.05
        : 0;

    const finalScore = Math.min(1, score + eventNameBonus);

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

function parseNumber(value) {
  const match = String(value || '').match(/-?\d+(\.\d+)?/);
  return match ? Number(match[0]) : null;
}

async function getBetsApiMatchStats(matchUrl) {
  const html = await fetchHtml(matchUrl);
  const $ = cheerio.load(html);

  const stats = {};

  /*
    Match Stats table structure seen from your screenshot:
    left value | stat name | right value

    Example:
    66 | Attacks | 85
    45 | Dangerous Attacks | 49
    1 | On Target | 2
  */

  $('table tr').each((_, row) => {
    const cells = $(row).find('td').map((i, td) => cleanText($(td).text())).get();

    if (cells.length < 3) return;

    const left = cells[0];
    const label = cells[1];
    const right = cells[2];

    const normalizedLabel = label
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '_')
      .replace(/^_+|_+$/g, '');

    if (!normalizedLabel) return;

    stats[normalizedLabel] = {
      home: parseNumber(left),
      away: parseNumber(right),
      label
    };
  });

  return stats;
}

function getStat(stats, key) {
  return stats[key] || { home: null, away: null };
}

function validateGoalsOpportunity(opportunity, liveStats) {
  const pick = String(opportunity.runnerName || '').toLowerCase();
  const market = String(opportunity.marketType || '').toUpperCase();

  const isOver = pick.includes('over');
  const isUnder = pick.includes('under');

  const scoreData = parseScore(liveStats.score);

  const attacks = getStat(liveStats.stats, 'attacks');
  const dangerousAttacks = getStat(liveStats.stats, 'dangerous_attacks');
  const onTarget = getStat(liveStats.stats, 'on_target');
  const offTarget = getStat(liveStats.stats, 'off_target');
  const corners = getStat(liveStats.stats, 'corners');

  const totalOnTarget = (onTarget.home || 0) + (onTarget.away || 0);
  const totalOffTarget = (offTarget.home || 0) + (offTarget.away || 0);
  const totalCorners = (corners.home || 0) + (corners.away || 0);
  const totalDangerousAttacks = (dangerousAttacks.home || 0) + (dangerousAttacks.away || 0);
  const totalAttacks = (attacks.home || 0) + (attacks.away || 0);

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

function validateResultOpportunity(opportunity, liveStats) {
  return {
    liveStatsFound: true,
    validation: 'neutral',
    livePressureScore: null,
    riskAdjustment: 0,
    reason: 'Live result-market validation not implemented yet',
    score: parseScore(liveStats.score).score,
    stats: liveStats.stats
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

    const stats = await getBetsApiMatchStats(match.url);

    const liveStats = {
      source: 'betsapi',
      matchUrl: match.url,
      matchScore: match.matchScore,
      reversed: match.reversed,
      home: match.home,
      away: match.away,
      score: match.score,
      stats
    };

    const market = String(opportunity.marketType || '').toUpperCase();

    const validation = market.startsWith('OVER_UNDER')
      ? validateGoalsOpportunity(opportunity, liveStats)
      : validateResultOpportunity(opportunity, liveStats);

    return {
      ...validation,
      source: 'betsapi',
      matchUrl: match.url,
      matchScore: match.matchScore,
      matchedHome: match.home,
      matchedAway: match.away,
      reversed: match.reversed
    };
  } catch (error) {
    return {
      liveStatsFound: false,
      validation: 'error',
      livePressureScore: null,
      riskAdjustment: 0,
      reason: `BetsAPI live stats validation failed: ${error.message}`,
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
