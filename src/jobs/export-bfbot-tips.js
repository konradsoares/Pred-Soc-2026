const fs = require('fs');
const path = require('path');
const db = require('../db/connection');
const env = require('../config/env');

const ROOT_DIR = path.resolve(__dirname, '../..');
const OUTPUT_DIR = path.join(ROOT_DIR, 'output', 'bfbot');

const WINDOW = process.argv[2] || process.env.PREDICTION_WINDOW || 'morning';
const TARGET_DATE = process.argv[3] || new Date().toISOString().slice(0, 10);

const BF_URL = 'https://api.betfair.com/exchange/betting/json-rpc/v1';
const APP_KEY = env.BETFAIR_APP_KEY;
const SESSION_TOKEN = env.BETFAIR_SESSION_TOKEN;

const EVENT_TYPE_SOCCER = '1';

function ensureOutputDir() {
  fs.mkdirSync(OUTPUT_DIR, { recursive: true });
}

function normalizeName(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/\(w\)/g, ' women ')
    .replace(/\bu20\b/g, ' under20 ')
    .replace(/\bu21\b/g, ' under21 ')
    .replace(/\bu23\b/g, ' under23 ')
    .replace(/fc\b/g, '')
    .replace(/cf\b/g, '')
    .replace(/afc\b/g, '')
    .replace(/[^a-z0-9]+/g, '')
    .trim();
}

function csvEscape(value) {
  if (value === null || value === undefined) return '';
  const s = String(value);
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

async function betfairRpc(method, params) {
  if (!APP_KEY || !SESSION_TOKEN) {
    throw new Error('Missing BETFAIR_APP_KEY or BETFAIR_SESSION_TOKEN in docker/.env');
  }

  const res = await fetch(BF_URL, {
    method: 'POST',
    headers: {
      'X-Application': APP_KEY,
      'X-Authentication': SESSION_TOKEN,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      jsonrpc: '2.0',
      method: `SportsAPING/v1.0/${method}`,
      params,
      id: Date.now()
    })
  });

  const json = await res.json();

  if (json.error) {
    throw new Error(`${method} failed: ${JSON.stringify(json.error)}`);
  }

  return json.result || [];
}

function marketConfig(tip) {
  const market = String(tip.market || '').toLowerCase();
  const pick = String(tip.pick || '').toLowerCase();

  if (market === '1x2' || market === 'match_odds') {
    return {
      marketType: 'MATCH_ODDS',
      handicap: '',
      kind: 'match_odds'
    };
  }

  if (market === 'double_chance') {
    return {
      marketType: 'DOUBLE_CHANCE',
      handicap: '',
      kind: 'double_chance'
    };
  }

  if (market === 'btts' || market === 'both_teams_to_score') {
    return {
      marketType: 'BOTH_TEAMS_TO_SCORE',
      handicap: '',
      kind: 'btts'
    };
  }

  if (market === 'goals') {
    const m = pick.match(/(over|under)_(\d+)_(\d+)/);
    if (!m) return null;

    const line = `${m[2]}.${m[3]}`;
    const typeCode = `OVER_UNDER_${m[2]}${m[3]}`;

    return {
      marketType: typeCode,
      handicap: line,
      kind: 'goals'
    };
  }

  return null;
}

function scoreEventName(eventName, homeTeam, awayTeam) {
  const ev = normalizeName(eventName);
  const home = normalizeName(homeTeam);
  const away = normalizeName(awayTeam);

  let score = 0;
  if (ev.includes(home)) score += 50;
  if (ev.includes(away)) score += 50;

  const parts = ev.split('v');
  if (parts.length >= 2) {
    if (parts[0].includes(home)) score += 10;
    if (parts.slice(1).join('').includes(away)) score += 10;
  }

  return score;
}

async function findBetfairMarket(tip, cfg) {
  const queries = [
    `${tip.home_team} ${tip.away_team}`,
    `${tip.home_team} v ${tip.away_team}`,
    tip.home_team,
    tip.away_team
  ];

  const from = `${TARGET_DATE}T00:00:00Z`;
  const to = `${TARGET_DATE}T23:59:59Z`;

  let best = null;

  for (const textQuery of queries) {
    const markets = await betfairRpc('listMarketCatalogue', {
      filter: {
        eventTypeIds: [EVENT_TYPE_SOCCER],
        textQuery,
        marketTypeCodes: [cfg.marketType],
        marketStartTime: {
          from,
          to
        }
      },
      marketProjection: ['EVENT', 'RUNNER_DESCRIPTION', 'MARKET_START_TIME'],
      maxResults: '50',
      sort: 'FIRST_TO_START'
    });

    for (const m of markets) {
      const eventName = m.event?.name || '';
      const score = scoreEventName(eventName, tip.home_team, tip.away_team);

      if (score < 70) continue;

      if (!best || score > best.score) {
        best = {
          score,
          market: m
        };
      }
    }

    if (best) break;
  }

  return best?.market || null;
}

function pickRunnerByName(runners, wanted) {
  const target = normalizeName(wanted);

  let best = null;

  for (const r of runners || []) {
    const rn = normalizeName(r.runnerName);

    let score = 0;
    if (rn === target) score += 100;
    else if (rn.includes(target) || target.includes(rn)) score += 80;

    if (!best || score > best.score) {
      best = { score, runner: r };
    }
  }

  return best && best.score >= 70 ? best.runner : null;
}

function findSelection(tip, market, cfg) {
  const runners = market.runners || [];
  const pick = String(tip.pick || '').toLowerCase();

  if (cfg.kind === 'match_odds') {
    if (pick === 'x') {
      return runners.find((r) => /draw/i.test(r.runnerName));
    }

    if (pick === '1') {
      return pickRunnerByName(runners, tip.home_team);
    }

    if (pick === '2') {
      return pickRunnerByName(runners, tip.away_team);
    }
  }

  if (cfg.kind === 'double_chance') {
    const home = normalizeName(tip.home_team);
    const away = normalizeName(tip.away_team);

    for (const r of runners) {
      const rn = normalizeName(r.runnerName);
      const hasHome = rn.includes(home) || /home/.test(rn);
      const hasAway = rn.includes(away) || /away/.test(rn);
      const hasDraw = /draw|tie|x/.test(rn);

      if (pick === '1x' && hasHome && hasDraw) return r;
      if (pick === 'x2' && hasAway && hasDraw) return r;
      if (pick === '12' && hasHome && hasAway) return r;
    }

    // fallback by common BF runner names
    for (const r of runners) {
      const name = String(r.runnerName || '').toLowerCase();
      if (pick === '1x' && /(home.*draw|draw.*home|1x)/i.test(name)) return r;
      if (pick === 'x2' && /(away.*draw|draw.*away|x2)/i.test(name)) return r;
      if (pick === '12' && /(home.*away|away.*home|12)/i.test(name)) return r;
    }
  }

  if (cfg.kind === 'goals') {
    const wanted = pick.startsWith('over') ? 'over' : 'under';
    return runners.find((r) => String(r.runnerName || '').toLowerCase().includes(wanted));
  }

  if (cfg.kind === 'btts') {
    if (['yes', 'y', 'btts_yes'].includes(pick)) {
      return runners.find((r) => /^yes$/i.test(r.runnerName));
    }

    if (['no', 'n', 'btts_no'].includes(pick)) {
      return runners.find((r) => /^no$/i.test(r.runnerName));
    }
  }

  return null;
}

async function loadTips(client) {
  const result = await client.query(
    `
    SELECT
      st.id AS sent_tip_id,
      st.fixture_id,
      st.market,
      st.pick,
      st.odds,
      st.stake,
      st.confidence,
      b.tip_date,
      b.tip_window,
      f.kickoff_utc,
      ht.name AS home_team,
      at.name AS away_team
    FROM sent_tips st
    JOIN sent_tip_batches b ON b.id = st.batch_id
    JOIN fixtures f ON f.id = st.fixture_id
    JOIN teams ht ON ht.id = f.home_team_id
    JOIN teams at ON at.id = f.away_team_id
    WHERE b.tip_date = $1::date
      AND b.tip_window = $2
      AND st.status = 'pending'
      AND st.bet_type = 'single'
      AND st.fixture_id IS NOT NULL
    ORDER BY f.kickoff_utc ASC, st.id ASC
    `,
    [TARGET_DATE, WINDOW]
  );

  return result.rows;
}

function bfbotRow({ provider, eventId, marketId, selectionId, marketType, handicap, betType }) {
  return [
    provider,
    eventId,
    marketId,
    selectionId,
    marketType,
    handicap || '',
    betType
  ].map(csvEscape).join(',');
}

function fileSafeMarketType(marketType) {
  return marketType.toLowerCase().replace(/[^a-z0-9]+/g, '-');
}

async function main() {
  ensureOutputDir();

  const client = await db.getClient();

  const grouped = new Map();
  const skipped = [];

  try {
    const tips = await loadTips(client);

    console.log(`Loaded ${tips.length} pending single tips for ${TARGET_DATE} / ${WINDOW}`);

    for (const tip of tips) {
      const cfg = marketConfig(tip);

      if (!cfg) {
        skipped.push({
          sent_tip_id: tip.sent_tip_id,
          fixture_id: tip.fixture_id,
          reason: `Unsupported market ${tip.market}/${tip.pick}`
        });
        continue;
      }

      try {
        const market = await findBetfairMarket(tip, cfg);

        if (!market) {
          skipped.push({
            sent_tip_id: tip.sent_tip_id,
            fixture_id: tip.fixture_id,
            market: tip.market,
            pick: tip.pick,
            reason: 'Betfair market not found'
          });
          continue;
        }

        const runner = findSelection(tip, market, cfg);

        if (!runner) {
          skipped.push({
            sent_tip_id: tip.sent_tip_id,
            fixture_id: tip.fixture_id,
            market_id: market.marketId,
            market_type: cfg.marketType,
            market: tip.market,
            pick: tip.pick,
            runners: (market.runners || []).map((r) => r.runnerName),
            reason: 'Selection not found in Betfair market'
          });
          continue;
        }

        const row = bfbotRow({
          provider: `PredSoc-${TARGET_DATE}-${WINDOW}`,
          eventId: market.event.id,
          marketId: market.marketId,
          selectionId: runner.selectionId,
          marketType: cfg.marketType,
          handicap: cfg.handicap,
          betType: 'BACK'
        });

        if (!grouped.has(cfg.marketType)) grouped.set(cfg.marketType, []);
        grouped.get(cfg.marketType).push(row);

        console.log(
          `OK ${tip.sent_tip_id}: ${tip.home_team} vs ${tip.away_team} | ${cfg.marketType} | ${runner.runnerName}`
        );
      } catch (err) {
        skipped.push({
          sent_tip_id: tip.sent_tip_id,
          fixture_id: tip.fixture_id,
          market: tip.market,
          pick: tip.pick,
          reason: err.message
        });
      }
    }

    const header = 'Provider,EventId,MarketId,SelectionId,MarketType,Handicap,BetType';

    for (const [marketType, rows] of grouped.entries()) {
      const filename = path.join(
        OUTPUT_DIR,
        `bfbot-${TARGET_DATE}-${WINDOW}-${fileSafeMarketType(marketType)}.csv`
      );

      fs.writeFileSync(filename, [header, ...rows].join('\n') + '\n', 'utf8');
      console.log(`Wrote ${rows.length} rows: ${filename}`);
    }

    const allRows = [...grouped.values()].flat();
    const allFile = path.join(OUTPUT_DIR, `bfbot-${TARGET_DATE}-${WINDOW}-all.csv`);
    fs.writeFileSync(allFile, [header, ...allRows].join('\n') + '\n', 'utf8');
    console.log(`Wrote ${allRows.length} rows: ${allFile}`);

    const skippedFile = path.join(OUTPUT_DIR, `bfbot-${TARGET_DATE}-${WINDOW}-skipped.json`);
    fs.writeFileSync(skippedFile, JSON.stringify(skipped, null, 2), 'utf8');
    console.log(`Skipped ${skipped.length} tips: ${skippedFile}`);
  } finally {
    client.release();
    await db.pool.end();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
