const env = require('../config/env');
const axios = require('axios');
const cheerio = require('cheerio');

function normalizeText(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function escapeRegex(value) {
  return String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function parseIntSafe(value) {
  if (value === null || value === undefined) return null;
  const cleaned = String(value).replace(/[^\d-]/g, '').trim();
  if (!cleaned) return null;
  const num = parseInt(cleaned, 10);
  return Number.isNaN(num) ? null : num;
}

function parseFloatSafe(value) {
  if (value === null || value === undefined) return null;
  const cleaned = String(value).replace(',', '.').replace(/[^\d.-]/g, '').trim();
  if (!cleaned) return null;
  const num = parseFloat(cleaned);
  return Number.isNaN(num) ? null : num;
}

function parsePercentSafe(value) {
  return parseFloatSafe(value);
}

function absoluteUrl(url) {
  if (!url) return null;
  if (/^https?:\/\//i.test(url)) return url;
  return `${env.SCRAPER_BASE_URL}${url.startsWith('/') ? '' : '/'}${url}`;
}

function teamNameFromCompareUrl(compareUrl, side = 'home') {
  const decoded = decodeURIComponent(compareUrl.replace(/\+/g, ' '));
  const m = decoded.match(/\/compare\/teams\/([^/]+)\/([^/]+)$/i);
  if (!m) return null;

  const raw = side === 'home' ? m[1] : m[2];
  return normalizeText(raw.replace(/\s*\([^)]+\)\s*$/, ''));
}

function countryFromCompareUrl(compareUrl, side = 'home') {
  const decoded = decodeURIComponent(compareUrl.replace(/\+/g, ' '));
  const m = decoded.match(/\/compare\/teams\/([^/]+)\/([^/]+)$/i);
  if (!m) return null;

  const raw = side === 'home' ? m[1] : m[2];
  const c = raw.match(/\(([^)]+)\)\s*$/);
  return c ? normalizeText(c[1]) : null;
}

function sectionSlice(fullText, startLabel, endLabels = []) {
  const text = normalizeText(fullText);
  const startIdx = text.indexOf(startLabel);
  if (startIdx < 0) return '';

  const afterStart = text.slice(startIdx + startLabel.length);
  let endIdx = afterStart.length;

  for (const label of endLabels) {
    const idx = afterStart.indexOf(label);
    if (idx >= 0 && idx < endIdx) endIdx = idx;
  }

  return normalizeText(afterStart.slice(0, endIdx));
}

function extractSectionText(bodyText, title) {
  const allTitles = [
    'Teams information',
    'Next match',
    'Matches between teams',
    'Statistics for last 10 matches',
    'Statistic facts of last 10 matches',
    'last 10 matches',
    'statistic facts',
    'team standings',
    'bet statistics',
    'Statistics of the season',
    'show all matches'
  ];

  return sectionSlice(bodyText, title, allTitles.filter((x) => x !== title));
}

function extractWorldRank(teamInfoText, teamName) {
  if (!teamInfoText || !teamName) return null;

  const rx = new RegExp(`${escapeRegex(teamName)}[\\s\\S]*?world rank\\s*(\\d+)`, 'i');
  const m = teamInfoText.match(rx);
  return m ? parseIntSafe(m[1]) : null;
}

function readValueAfterLabel(text, label) {
  const rx = new RegExp(`${escapeRegex(label)}\\s*([\\d.,]+)\\s*(?:%|min\\.)?`, 'i');
  const m = text.match(rx);
  return m ? parseFloatSafe(m[1]) : null;
}

function readPercentAfterLabel(text, label) {
  const rx = new RegExp(`${escapeRegex(label)}\\s*([\\d.,]+)%`, 'i');
  const m = text.match(rx);
  return m ? parsePercentSafe(m[1]) : null;
}

function parseH2HMatches(matchesText, homeName, awayName) {
  if (!matchesText) return [];

  const parts = matchesText.split(/(?=\d{4}-\d{2}-\d{2}\s+details\s+)/g);
  const matches = [];

  for (const chunk of parts) {
    const dateMatch = chunk.match(/(\d{4}-\d{2}-\d{2})\s+details\s+/);
    if (!dateMatch) continue;

    const date = dateMatch[1];
    const beforeDate = normalizeText(chunk.slice(0, dateMatch.index));
    const afterDate = chunk.slice(dateMatch.index + dateMatch[0].length);

    const scoreTeams = afterDate.match(
      /(\d+)\s*([A-Za-z0-9.\- '&/]+?)\s*(\d+)\s*([A-Za-z0-9.\- '&/]+?)\s*(\d{2})\s*half time result/i
    );

    if (!scoreTeams) continue;

    const homeGoals = parseIntSafe(scoreTeams[1]);
    const teamA = normalizeText(scoreTeams[2]);
    const awayGoals = parseIntSafe(scoreTeams[3]);
    const teamB = normalizeText(scoreTeams[4]);
    const htPair = String(scoreTeams[5] || '');
    const halftimeHome = parseIntSafe(htPair[0]);
    const halftimeAway = parseIntSafe(htPair[1]);

    matches.push({
      competition: beforeDate || null,
      date,
      home_team: teamA,
      away_team: teamB,
      home_goals: homeGoals,
      away_goals: awayGoals,
      halftime_home: halftimeHome,
      halftime_away: halftimeAway,
      is_target_pair: [teamA, teamB].includes(homeName) && [teamA, teamB].includes(awayName),
      raw_text: normalizeText(chunk)
    });
  }

  return matches;
}

function summarizeH2H(h2hMatches, homeName, awayName, limit = 10) {
  const filtered = h2hMatches.filter((m) => m.is_target_pair).slice(0, limit);

  let homeWins = 0;
  let draws = 0;
  let awayWins = 0;
  let homeGoals = 0;
  let awayGoals = 0;

  for (const m of filtered) {
    let targetHomeGoals;
    let targetAwayGoals;

    if (m.home_team === homeName && m.away_team === awayName) {
      targetHomeGoals = m.home_goals;
      targetAwayGoals = m.away_goals;
    } else if (m.home_team === awayName && m.away_team === homeName) {
      targetHomeGoals = m.away_goals;
      targetAwayGoals = m.home_goals;
    } else {
      continue;
    }

    homeGoals += targetHomeGoals;
    awayGoals += targetAwayGoals;

    if (targetHomeGoals > targetAwayGoals) homeWins += 1;
    else if (targetHomeGoals < targetAwayGoals) awayWins += 1;
    else draws += 1;
  }

  return {
    matches_considered: filtered.length,
    home_wins: homeWins,
    draws,
    away_wins: awayWins,
    home_goals: homeGoals,
    away_goals: awayGoals,
    recent_matches: filtered
  };
}

function parseLabeledPercents(text, labelMap) {
  const out = {};
  for (const [key, label] of Object.entries(labelMap)) {
    const rx = new RegExp(`${escapeRegex(label)}\\s*(\\d+)%`, 'i');
    const m = text.match(rx);
    out[key] = m ? parsePercentSafe(m[1]) : null;
  }
  return out;
}

function parseOverUnder(text, teamName) {
  function pct(rx) {
    const m = text.match(rx);
    return m ? parsePercentSafe(m[1]) : null;
  }

  return {
    all_goals_over_15: pct(/Over\/under 1\.5 for all goals in matches\s*over\s*(\d+)%/i),
    all_goals_under_15: pct(/Over\/under 1\.5 for all goals in matches[\s\S]*?under\s*(\d+)%/i),
    team_goals_over_15: pct(new RegExp(`Over\\/under 1\\.5 goals for ${escapeRegex(teamName)} only\\s*over\\s*(\\d+)%`, 'i')),
    team_goals_under_15: pct(new RegExp(`Over\\/under 1\\.5 goals for ${escapeRegex(teamName)} only[\\s\\S]*?under\\s*(\\d+)%`, 'i')),

    all_goals_over_25: pct(/Over\/under 2\.5 for all goals in matches\s*over\s*(\d+)%/i),
    all_goals_under_25: pct(/Over\/under 2\.5 for all goals in matches[\s\S]*?under\s*(\d+)%/i),
    team_goals_over_25: pct(new RegExp(`Over\\/under 2\\.5 goals for ${escapeRegex(teamName)} only\\s*over\\s*(\\d+)%`, 'i')),
    team_goals_under_25: pct(new RegExp(`Over\\/under 2\\.5 goals for ${escapeRegex(teamName)} only[\\s\\S]*?under\\s*(\\d+)%`, 'i')),

    all_goals_over_35: pct(/Over\/under 3\.5 for all goals in matches\s*over\s*(\d+)%/i),
    all_goals_under_35: pct(/Over\/under 3\.5 for all goals in matches[\s\S]*?under\s*(\d+)%/i),
    team_goals_over_35: pct(new RegExp(`Over\\/under 3\\.5 goals for ${escapeRegex(teamName)} only\\s*over\\s*(\\d+)%`, 'i')),
    team_goals_under_35: pct(new RegExp(`Over\\/under 3\\.5 goals for ${escapeRegex(teamName)} only[\\s\\S]*?under\\s*(\\d+)%`, 'i'))
  };
}

function parseStatsBlock(blockText, teamName) {
  const text = normalizeText(blockText);

  if (!text) {
    return {
      team_name: teamName,
      general_match_facts: {},
      halftime_1x2: {},
      second_half_1x2: {},
      over_under: {},
      goal_characteristics: {},
      goals_by_minutes: {},
      first_goal: {},
      winners_after_minutes: {},
      goal_difference: {},
      raw_text: ''
    };
  }

  const generalMatchFacts = parseLabeledPercents(text, {
    win_pct: teamName,
    draw_pct: 'draw',
    opponent_win_pct: 'opponent'
  });

  const overUnder = parseOverUnder(text, teamName);

  const statisticFacts = {
    avg_goals_for: readValueAfterLabel(text, 'Average scored goals per match'),
    avg_goals_against: readValueAfterLabel(text, 'Average conceded goals per match'),
    chance_score_next_pct: readPercentAfterLabel(text, 'Chance to score goal next match'),
    chance_concede_next_pct: readPercentAfterLabel(text, 'Chance to conceded goal next match'),
    clean_sheets_count: readValueAfterLabel(text, 'Number of clean sheet matches'),
    failed_to_score_count: readValueAfterLabel(text, 'Failure to score matches'),
    over_25_matches_count: readValueAfterLabel(text, 'Matches over 2.5 goals in'),
    under_25_matches_count: readValueAfterLabel(text, 'Matches under 2.5 goals in'),
    time_without_scored_goal_min: readValueAfterLabel(text, 'Time without scored goal'),
    time_without_conceded_goal_min: readValueAfterLabel(text, 'Time without conceded goal')
  };

  const halftime1x2 = (() => {
    const m = text.match(new RegExp(`Halftime 1 X 2\\s*${escapeRegex(teamName)}\\s*(\\d+)%\\s*draw\\s*(\\d+)%\\s*opponent\\s*(\\d+)%`, 'i'));
    return {
      win_pct: m ? parsePercentSafe(m[1]) : null,
      draw_pct: m ? parsePercentSafe(m[2]) : null,
      opponent_win_pct: m ? parsePercentSafe(m[3]) : null
    };
  })();

  const secondHalf1x2 = (() => {
    const m = text.match(new RegExp(`Second half 1 X 2\\s*${escapeRegex(teamName)}\\s*(\\d+)%\\s*draw\\s*(\\d+)%\\s*opponent\\s*(\\d+)%`, 'i'));
    return {
      win_pct: m ? parsePercentSafe(m[1]) : null,
      draw_pct: m ? parsePercentSafe(m[2]) : null,
      opponent_win_pct: m ? parsePercentSafe(m[3]) : null
    };
  })();

  const goalCharacteristics = {
    goal_bands_0_1: (text.match(/Goal bands\s*0-1\s*(\d+)%/i) || [])[1] ? parsePercentSafe(text.match(/Goal bands\s*0-1\s*(\d+)%/i)[1]) : null,
    goal_bands_2_3: (text.match(/Goal bands[\s\S]*?2-3\s*(\d+)%/i) || [])[1] ? parsePercentSafe(text.match(/Goal bands[\s\S]*?2-3\s*(\d+)%/i)[1]) : null,
    goal_bands_4_plus: (text.match(/Goal bands[\s\S]*?4\+\s*(\d+)%/i) || [])[1] ? parsePercentSafe(text.match(/Goal bands[\s\S]*?4\+\s*(\d+)%/i)[1]) : null,
    both_score_pct: (text.match(/Team to score\s*both\s*(\d+)%/i) || [])[1] ? parsePercentSafe(text.match(/Team to score\s*both\s*(\d+)%/i)[1]) : null,
    only_one_scores_pct: (text.match(/Team to score[\s\S]*?only one\s*(\d+)%/i) || [])[1] ? parsePercentSafe(text.match(/Team to score[\s\S]*?only one\s*(\d+)%/i)[1]) : null,
    neither_scores_pct: (text.match(/Team to score[\s\S]*?neither\s*(\d+)%/i) || [])[1] ? parsePercentSafe(text.match(/Team to score[\s\S]*?neither\s*(\d+)%/i)[1]) : null,
    odd_goals_pct: (text.match(/Odd\/even goals in matches\s*odd\s*(\d+)%/i) || [])[1] ? parsePercentSafe(text.match(/Odd\/even goals in matches\s*odd\s*(\d+)%/i)[1]) : null,
    even_goals_pct: (text.match(/Odd\/even goals in matches[\s\S]*?even\s*(\d+)%/i) || [])[1] ? parsePercentSafe(text.match(/Odd\/even goals in matches[\s\S]*?even\s*(\d+)%/i)[1]) : null
  };

  const firstGoal = {
    team_without_goal: statisticFacts.failed_to_score_count !== null ? statisticFacts.failed_to_score_count * 10 : null,
    match_without_goal: readPercentAfterLabel(text, 'without goal'),
    team_scores_first_pct: (text.match(new RegExp(`First goal in matches\\s*${escapeRegex(teamName)}\\s*(\\d+)%`, 'i')) || [])[1] ? parsePercentSafe(text.match(new RegExp(`First goal in matches\\s*${escapeRegex(teamName)}\\s*(\\d+)%`, 'i'))[1]) : null,
    opponent_scores_first_pct: (text.match(/First goal in matches[\s\S]*?opponent\s*(\d+)%/i) || [])[1] ? parsePercentSafe(text.match(/First goal in matches[\s\S]*?opponent\s*(\d+)%/i)[1]) : null
  };

  return {
    team_name: teamName,
    general_match_facts: generalMatchFacts,
    halftime_1x2: halftime1x2,
    second_half_1x2: secondHalf1x2,
    over_under: overUnder,
    statistic_facts: statisticFacts,
    avg_goals_for: statisticFacts.avg_goals_for,
    avg_goals_against: statisticFacts.avg_goals_against,
    chance_score_next_pct: statisticFacts.chance_score_next_pct,
    chance_concede_next_pct: statisticFacts.chance_concede_next_pct,
    clean_sheets_count: statisticFacts.clean_sheets_count,
    failed_to_score_count: statisticFacts.failed_to_score_count,
    over_25_matches_count: statisticFacts.over_25_matches_count,
    under_25_matches_count: statisticFacts.under_25_matches_count,
    time_without_scored_goal_min: statisticFacts.time_without_scored_goal_min,
    time_without_conceded_goal_min: statisticFacts.time_without_conceded_goal_min,
    goal_characteristics: goalCharacteristics,
    goals_by_minutes: {},
    first_goal: firstGoal,
    winners_after_minutes: {},
    goal_difference: {},
    raw_text: text
  };
}

function extractLast10TeamBlock(statsText, teamName, nextTeamName) {
  if (!statsText || !teamName) return '';

  const startRx = new RegExp(`${escapeRegex(teamName)}\\s+(?:General match facts|Number of ${escapeRegex(teamName)} wins)`, 'i');
  const start = statsText.search(startRx);
  if (start < 0) return '';

  const tail = statsText.slice(start);

  if (nextTeamName) {
    const endRx = new RegExp(`${escapeRegex(nextTeamName)}\\s+(?:General match facts|Number of ${escapeRegex(nextTeamName)} wins)`, 'i');
    const end = tail.search(endRx);
    if (end >= 0) return normalizeText(tail.slice(0, end));
  }

  return normalizeText(tail);
}

async function fetchCompareStats(compareUrl) {
  const finalUrl = absoluteUrl(compareUrl);

  const response = await axios.get(finalUrl, {
    timeout: Number(env.SCRAPER_TIMEOUT_MS || 30000),
    headers: {
      'User-Agent': 'Mozilla/5.0 PredSoc/1.0'
    }
  });

  const $ = cheerio.load(response.data);
  const bodyText = normalizeText($('body').text());

  const pageTitle = normalizeText($('h1').first().text());
  const homeName = teamNameFromCompareUrl(finalUrl, 'home');
  const awayName = teamNameFromCompareUrl(finalUrl, 'away');
  const homeCountry = countryFromCompareUrl(finalUrl, 'home');
  const awayCountry = countryFromCompareUrl(finalUrl, 'away');

  const teamsInfoText = extractSectionText(bodyText, 'Teams information');
  const matchesBetweenText = extractSectionText(bodyText, 'Matches between teams');
  const statsLast10Text =
    extractSectionText(bodyText, 'Statistic facts of last 10 matches') ||
    extractSectionText(bodyText, 'Statistics for last 10 matches') ||
    extractSectionText(bodyText, 'last 10 matches');

  const homeWorldRank = extractWorldRank(teamsInfoText, homeName);
  const awayWorldRank = extractWorldRank(teamsInfoText, awayName);

  const h2hMatches = parseH2HMatches(matchesBetweenText, homeName, awayName);
  const h2hSummary = summarizeH2H(h2hMatches, homeName, awayName, 10);

  const homeStatsBlock = extractLast10TeamBlock(statsLast10Text, homeName, awayName);
  const awayStatsBlock = extractLast10TeamBlock(statsLast10Text, awayName, null);

  return {
    compare_url: finalUrl,
    page_title: pageTitle,
    home_team: {
      name: homeName,
      country: homeCountry,
      world_rank: homeWorldRank
    },
    away_team: {
      name: awayName,
      country: awayCountry,
      world_rank: awayWorldRank
    },
    h2h: h2hSummary,
    recent_form: {
      home: parseStatsBlock(homeStatsBlock, homeName),
      away: parseStatsBlock(awayStatsBlock, awayName)
    },
    raw_payload: {
      teams_information_text: teamsInfoText,
      matches_between_teams_text: matchesBetweenText,
      statistics_last_10_matches_text: statsLast10Text,
      home_stats_block: homeStatsBlock,
      away_stats_block: awayStatsBlock,
      h2h_matches: h2hMatches
    }
  };
}

module.exports = {
  fetchCompareStats
};
