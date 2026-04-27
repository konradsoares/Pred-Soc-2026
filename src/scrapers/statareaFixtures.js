const env = require('../config/env');
const axios = require('axios');
const cheerio = require('cheerio');

function formatTargetDate(offsetDays = 0) {
  const d = new Date();
  d.setDate(d.getDate() + offsetDays);

  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');

  return `${yyyy}-${mm}-${dd}`;
}

function slugify(value) {
  return String(value || '')
    .toLowerCase()
    .normalize('NFKD')
    .replace(/[^\w\s-]/g, '')
    .trim()
    .replace(/\s+/g, '-');
}

function normalizeText(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function parseNumber(value) {
  if (!value) return null;
  const cleaned = String(value)
    .replace(',', '.')
    .replace(/[^\d.%-]/g, '')
    .trim();

  if (!cleaned) return null;

  const num = parseFloat(cleaned.replace('%', ''));
  return Number.isNaN(num) ? null : num;
}

function detectCompetitionType(name) {
  const n = normalizeText(name).toLowerCase();

  if (!n) return 'league';
  if (n.includes('friendly')) return 'friendly';
  if (n.includes('cup')) return 'cup';

  return 'league';
}

function extractKickoffTime(match, date) {
  const raw = normalizeText(match.find('.date, .time').first().text());
  const m = raw.match(/\b(\d{1,2}):(\d{2})\b/);

  if (!m) return new Date(`${date}T12:00:00Z`).toISOString();

  const hh = m[1].padStart(2, '0');
  const mm = m[2];

  return new Date(`${date}T${hh}:${mm}:00Z`).toISOString();
}

function extractCountryAndCompetition(match) {
  const container = match.closest('.competition');
  const header = normalizeText(container.find('.header').first().text());

  if (!header) {
    return { country: 'Unknown', competition: 'Unknown Competition' };
  }

  const cleaned = header.split('your prediction').pop().trim();
  const parts = cleaned.split(/\s+-\s+/);

  if (parts.length >= 2) {
    return {
      country: normalizeText(parts[0]),
      competition: normalizeText(parts.slice(1).join(' - '))
    };
  }

  return {
    country: 'Unknown',
    competition: cleaned
  };
}

function extractCompareUrl(match, home, away) {
  const href = match.find('a[href*="/compare/teams/"]').first().attr('href');

  if (href) {
    if (href.startsWith('http')) return href;
    return `${env.SCRAPER_BASE_URL}${href}`;
  }

  return `${env.SCRAPER_BASE_URL}/compare/teams/${encodeURIComponent(home)}/${encodeURIComponent(away)}`;
}

// 🔥 MAIN FIX → explicit date param
async function fetchFixturesByDate(date) {
  const url = `${env.SCRAPER_BASE_URL}/predictions/date/${date}/`;

  console.log(`Scraping: ${url}`);

  const res = await axios.get(url, {
    timeout: 30000,
    headers: { 'User-Agent': 'Mozilla/5.0' }
  });

  const $ = cheerio.load(res.data);
  const results = [];

  $('.match').each((_, el) => {
    const match = $(el);

    const home = normalizeText(match.find('.hostteam .name a').text());
    const away = normalizeText(match.find('.guestteam .name a').text());
    const tip = normalizeText(match.find('.tip .value div').text());

    if (!home || !away) return;

    const predictions = [];
    match.find('.coefbox .value').each((_, p) => {
      const v = parseNumber($(p).text());
      if (v !== null) predictions.push(v);
    });

    const { country, competition } = extractCountryAndCompetition(match);

    results.push({
      external_id: `statarea:${date}:${slugify(home)}:${slugify(away)}`,
      kickoff_utc: extractKickoffTime(match, date),
      country,
      competition,
      competition_type: detectCompetitionType(competition),
      is_friendly: false,
      home_team: home,
      away_team: away,
      compare_url: extractCompareUrl(match, home, away),
      source_name: 'statarea',
      tip: tip || null,
      prob_home: predictions[0] ?? null,
      prob_draw: predictions[1] ?? null,
      prob_away: predictions[2] ?? null,
      prob_over_25: predictions[6] ?? null,
      prob_under_25: predictions[7] ?? null,
      raw_payload: { predictions }
    });
  });

  console.log(`Parsed ${results.length} fixtures`);
  return results;
}

async function fetchTodayFixtures() {
  return fetchFixturesByDate(formatTargetDate(0));
}

module.exports = {
  fetchTodayFixtures,
  fetchFixturesByDate
};
