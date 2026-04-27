const env = require('../config/env');
const axios = require('axios');
const cheerio = require('cheerio');

function formatTargetDate() {
  const d = new Date();

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
  if (
    n.includes('playoff') ||
    n.includes('play-offs') ||
    n.includes('tournament') ||
    n.includes('super cup') ||
    n.includes('supercup')
  ) {
    return 'tournament';
  }

  return 'league';
}

function extractKickoffTime(match, date) {
  const raw = normalizeText(match.find('.date, .time').first().text());
  const timeMatch = raw.match(/\b(\d{1,2}):(\d{2})\b/);

  if (timeMatch) {
    const hh = timeMatch[1].padStart(2, '0');
    const mm = timeMatch[2];
    return new Date(`${date}T${hh}:${mm}:00Z`).toISOString();
  }

  const ownHeader = normalizeText(match.find('.teams .ownheader').first().text());
  const ownHeaderMatch = ownHeader.match(/\b(\d{4}-\d{2}-\d{2})\s+(\d{1,2}):(\d{2})\b/);

  if (ownHeaderMatch) {
    const hh = ownHeaderMatch[2].padStart(2, '0');
    const mm = ownHeaderMatch[3];
    return new Date(`${date}T${hh}:${mm}:00Z`).toISOString();
  }

  return new Date(`${date}T12:00:00Z`).toISOString();
}

function cleanHeaderText(raw) {
  let text = normalizeText(raw);

  if (!text) return '';

  const marker = 'your prediction';
  const idx = text.toLowerCase().indexOf(marker);
  if (idx >= 0) {
    text = normalizeText(text.slice(idx + marker.length));
  }

  text = text.replace(/^[-: ]+/, '').trim();
  return text;
}

function extractCountryAndCompetition(match) {
  const competitionContainer = match.closest('.competition');
  let headerText = '';

  if (competitionContainer.length) {
    headerText = cleanHeaderText(competitionContainer.find('.header').first().text());
  }

  if (!headerText) {
    const prevCompetition = match.prevAll('.competition').first();
    if (prevCompetition.length) {
      headerText = cleanHeaderText(prevCompetition.find('.header').first().text());
    }
  }

  if (!headerText) {
    const rawHeader = cleanHeaderText(match.prevAll('.header').first().text());
    if (rawHeader) {
      headerText = rawHeader;
    }
  }

  if (!headerText) {
    return {
      country: 'Unknown',
      competition: 'Unknown Competition'
    };
  }

  const parts = headerText.split(/\s+-\s+/);
  if (parts.length >= 2) {
    return {
      country: normalizeText(parts[0]),
      competition: normalizeText(parts.slice(1).join(' - '))
    };
  }

  return {
    country: 'Unknown',
    competition: headerText
  };
}

function extractCountryFromCompareUrl(compareUrl) {
  if (!compareUrl) return null;

  const decoded = decodeURIComponent(compareUrl.replace(/\+/g, ' '));
  const match = decoded.match(/\/compare\/teams\/[^/]+\(([^)]+)\)\/[^/]+\(([^)]+)\)/i);

  if (!match) return null;

  const country1 = normalizeText(match[1]);
  const country2 = normalizeText(match[2]);

  if (country1 && country1 === country2) return country1;
  return country1 || country2 || null;
}

function extractCompareUrl(match, home, away) {
  const href = match.find('.actions .info a[href*="/compare/teams/"], a[href*="/compare/teams/"]').first().attr('href');

  if (href) {
    if (/^https?:\/\//i.test(href)) return href;
    return `${env.SCRAPER_BASE_URL}${href.startsWith('/') ? '' : '/'}${href}`;
  }

  return `${env.SCRAPER_BASE_URL}/compare/teams/${encodeURIComponent(home)}/${encodeURIComponent(away)}`;
}

async function fetchTodayFixtures() {
  const date = formatTargetDate();
  const url = `${env.SCRAPER_BASE_URL}/predictions/date/${date}/competition`;	

  const res = await axios.get(url, {
    timeout: Number(env.SCRAPER_TIMEOUT_MS || 30000),
    headers: { 'User-Agent': 'Mozilla/5.0' }
  });

  const $ = cheerio.load(res.data);
  const results = [];

  $('.match').each((_, el) => {
    const match = $(el);

    const home = normalizeText(match.find('.hostteam .name a').first().text());
    const away = normalizeText(match.find('.guestteam .name a').first().text());
    const tip = normalizeText(match.find('.tip .value div').first().text());

    if (!home || !away) return;

    const predictions = [];
    match.find('.coefrow > .coefbox .value, .coefbox .value').each((_, p) => {
      const value = parseNumber($(p).text());
      if (value !== null) predictions.push(value);
    });

    const compareUrl = extractCompareUrl(match, home, away);
    const parsed = extractCountryAndCompetition(match);
    const fallbackCountry = extractCountryFromCompareUrl(compareUrl);

    const country = parsed.country !== 'Unknown' ? parsed.country : (fallbackCountry || 'Unknown');
    const competition = parsed.competition || 'Unknown Competition';
    const kickoffUtc = extractKickoffTime(match, date);
    const competitionType = detectCompetitionType(competition);

    results.push({
      external_id: `statarea:${date}:${slugify(home)}:${slugify(away)}`,
      kickoff_utc: kickoffUtc,
      country,
      competition,
      competition_type: competitionType,
      is_friendly: competitionType === 'friendly',
      home_team: home,
      away_team: away,
      compare_url: compareUrl,
      source_name: 'statarea',
      tip: tip || null,
      prob_home: predictions[0] ?? null,
      prob_draw: predictions[1] ?? null,
      prob_away: predictions[2] ?? null,
      prob_over_25: predictions[6] ?? null,
      prob_under_25: predictions[7] ?? null,
      raw_payload: {
        scraped_date: date,
        country,
        competition,
        compare_url: compareUrl,
        tip,
        predictions
      }
    });
  });

  const unique = new Map();
  for (const item of results) {
    const key = `${item.external_id}|${item.kickoff_utc}`;
    if (!unique.has(key)) {
      unique.set(key, item);
    }
  }

  return [...unique.values()];
}

module.exports = { fetchTodayFixtures };
