const fs = require('fs');
const path = require('path');
const db = require('../db/connection');
const env = require('../config/env');

const ROOT_DIR = path.resolve(__dirname, '../..');
const OUTPUT_DIR = path.join(ROOT_DIR, 'output', 'bfbot');

function isDateArg(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(value || ''));
}

function resolveArgs() {
  const a = process.argv[2] || process.env.PREDICTION_WINDOW || 'daily';
  const b = process.argv[3] || new Date().toISOString().slice(0, 10);

  // Supports:
  // node export-bfbot-tips.js evening 2026-05-07
  // node export-bfbot-tips.js 2026-05-07
  // node export-bfbot-tips.js daily 2026-05-07
  if (isDateArg(a) && !isDateArg(b)) {
    return { window: 'daily', targetDate: a };
  }

  if (isDateArg(a) && isDateArg(b)) {
    return { window: 'daily', targetDate: a };
  }

  return { window: a, targetDate: b };
}

const { window: WINDOW, targetDate: TARGET_DATE } = resolveArgs();

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
    .replace(/\bfc\b/g, '')
    .replace(/\bcf\b/g, '')
    .replace(/\bafc\b/g, '')
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

function safeFile(value) {
  return String(value || 'unknown').toLowerCase().replace(/[^a-z0-9]+/g, '-');
}

function firstValue(row, names) {
  for (const name of names) {
    if (row[name] !== undefined && row[name] !== null && row[name] !== '') return row[name];
  }
  return null;
}

function providerForTip(tip) {
  const market = String(tip.market || '').toLowerCase();
  const pick = String(tip.pick || '').toUpperCase();

  if (market === '1x2' || market === 'match_odds') {
    if (pick === '1') return 'match_odds_h';
    if (pick === 'X') return 'match_odds_d';
    if (pick === '2') return 'match_odds_a';
  }

  if (market === 'double_chance') {
    if (pick === '1X') return 'doublechance_hd';
    if (pick === 'X2') return 'doublechance_dw';
    if (pick === '12') return 'doublechance_ha';
  }

  if (market === 'goals') {
    if (pick === 'OVER_1_5') return 'goals_over_15';
    if (pick === 'UNDER_1_5') return 'goals_under_15';
    if (pick === 'OVER_2_5') return 'goals_over_25';
    if (pick === 'UNDER_2_5') return 'goals_under_25';
    if (pick === 'OVER_3_5') return 'goals_over_35';
    if (pick === 'UNDER_3_5') return 'goals_under_35';
  }

  if (market === 'btts' || market === 'both_teams_to_score') {
    if (pick === 'YES' || pick === 'BTTS_YES') return 'btts_yes';
    if (pick === 'NO' || pick === 'BTTS_NO') return 'btts_no';
  }

  return `predsoc_${safeFile(market)}_${safeFile(pick)}`;
}

function marketConfig(tip) {
  const market = String(tip.market || '').toLowerCase();
  const pick = String(tip.pick || '').toLowerCase();

  if (market === '1x2' || market === 'match_odds') {
    return { marketType: 'MATCH_ODDS', handicap: 0, kind: 'match_odds' };
  }

  if (market === 'double_chance') {
    return { marketType: 'DOUBLE_CHANCE', handicap: 0, kind: 'double_chance' };
  }

  if (market === 'btts' || market === 'both_teams_to_score') {
    return { marketType: 'BOTH_TEAMS_TO_SCORE', handicap: 0, kind: 'btts' };
  }

  if (market === 'goals') {
    const m = pick.match(/(over|under)_(\d+)_(\d+)/);
    if (!m) return null;

    return {
      marketType: `OVER_UNDER_${m[2]}${m[3]}`,
      handicap: 0,
      kind: 'goals'
    };
  }

  return null;
}

function bfbotRow({ provider, eventId, marketId, selectionId, marketType, handicap }) {
  return [
    provider,
    eventId,
    marketId,
    selectionId,
    marketType,
    handicap ?? 0,
    'UNKNOWN'
  ].map(csvEscape).join(',');
}

async function tableExists(client, tableName) {
  const r = await client.query(
    `
    SELECT EXISTS (
      SELECT 1
      FROM information_schema.tables
      WHERE table_schema = 'public'
        AND table_name = $1
    ) AS exists
    `,
    [tableName]
  );
  return r.rows[0].exists;
}

async function columnsForTable(client, tableName) {
  const r = await client.query(
    `
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = $1
    `,
    [tableName]
  );
  return new Set(r.rows.map((x) => x.column_name));
}

async function loadMappedSelections(client) {
  const hasMappings = await tableExists(client, 'betfair_tip_mappings');
  if (!hasMappings) return [];

  const cols = await columnsForTable(client, 'betfair_tip_mappings');

  const hasTipId = cols.has('sent_tip_id');
  const hasLegId = cols.has('sent_tip_leg_id');

  const eventCol = cols.has('betfair_event_id')
    ? 'm.betfair_event_id'
    : cols.has('event_id')
      ? 'm.event_id'
      : 'NULL';

  const marketCol = cols.has('betfair_market_id')
    ? 'm.betfair_market_id'
    : cols.has('market_id')
      ? 'm.market_id'
      : 'NULL';

  const selectionCol = cols.has('betfair_selection_id')
    ? 'm.betfair_selection_id'
    : cols.has('selection_id')
      ? 'm.selection_id'
      : 'NULL';

  const marketTypeCol = cols.has('market_type_code')
    ? 'm.market_type_code'
    : cols.has('betfair_market_type')
      ? 'm.betfair_market_type'
      : cols.has('market_type')
        ? 'm.market_type'
        : 'NULL';

  const handicapCol = cols.has('handicap')
    ? 'm.handicap'
    : '0';

  const joinTip = hasTipId ? 'st.id = m.sent_tip_id' : 'FALSE';
  const joinLeg = hasLegId ? 'leg.id = m.sent_tip_leg_id' : 'FALSE';

  const r = await client.query(
    `
    SELECT
      ${eventCol} AS mapped_event_id,
      ${marketCol} AS mapped_market_id,
      ${selectionCol} AS mapped_selection_id,
      ${marketTypeCol} AS mapped_market_type,
      ${handicapCol} AS mapped_handicap,

      COALESCE(st.id, parent_st.id) AS resolved_sent_tip_id,
      leg.id AS resolved_sent_tip_leg_id,

      COALESCE(leg.fixture_id, st.fixture_id, parent_st.fixture_id) AS fixture_id,
      COALESCE(leg.market, st.market, parent_st.market) AS market,
      COALESCE(leg.pick, st.pick, parent_st.pick) AS pick,
      COALESCE(leg.odds, st.odds, parent_st.odds) AS odds,

      COALESCE(b.tip_date, parent_b.tip_date) AS tip_date,
      COALESCE(b.tip_window, parent_b.tip_window) AS tip_window,

      f.kickoff_utc,
      ht.name AS home_team,
      at.name AS away_team

    FROM betfair_tip_mappings m

    LEFT JOIN sent_tips st
      ON ${joinTip}
    LEFT JOIN sent_tip_batches b
      ON b.id = st.batch_id

    LEFT JOIN sent_tip_legs leg
      ON ${joinLeg}
    LEFT JOIN sent_tips parent_st
      ON parent_st.id = leg.sent_tip_id
    LEFT JOIN sent_tip_batches parent_b
      ON parent_b.id = parent_st.batch_id

    LEFT JOIN fixtures f
      ON f.id = COALESCE(leg.fixture_id, st.fixture_id, parent_st.fixture_id)
    LEFT JOIN teams ht
      ON ht.id = f.home_team_id
    LEFT JOIN teams at
      ON at.id = f.away_team_id

    WHERE COALESCE(b.tip_date, parent_b.tip_date) = $1::date
      AND ($2 = 'daily' OR COALESCE(b.tip_window, parent_b.tip_window) = $2)
      AND COALESCE(st.status, parent_st.status, 'pending') = 'pending'
      AND ${marketCol} IS NOT NULL
      AND ${selectionCol} IS NOT NULL

    ORDER BY f.kickoff_utc ASC NULLS LAST, resolved_sent_tip_id ASC, resolved_sent_tip_leg_id ASC
    `,
    [TARGET_DATE, WINDOW]
  );

  return r.rows.map((row) => {
    const tip = {
      sent_tip_id: row.resolved_sent_tip_id,
      sent_tip_leg_id: row.resolved_sent_tip_leg_id,
      fixture_id: row.fixture_id,
      market: row.market,
      pick: row.pick,
      odds: row.odds,
      home_team: row.home_team,
      away_team: row.away_team
    };

    const cfg = marketConfig(tip);

    return {
      source: 'mapping',
      provider: providerForTip(tip),
      eventId: row.mapped_event_id,
      marketId: row.mapped_market_id,
      selectionId: row.mapped_selection_id,
      marketType: row.mapped_market_type || cfg?.marketType,
      handicap: row.mapped_handicap ?? cfg?.handicap ?? 0,
      tip
    };
  }).filter((x) => x.eventId && x.marketId && x.selectionId && x.marketType);
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
        marketStartTime: { from, to }
      },
      marketProjection: ['EVENT', 'RUNNER_DESCRIPTION', 'MARKET_START_TIME'],
      maxResults: '50',
      sort: 'FIRST_TO_START'
    });

    for (const m of markets) {
      const score = scoreEventName(m.event?.name || '', tip.home_team, tip.away_team);
      if (score < 70) continue;

      if (!best || score > best.score) {
        best = { score, market: m };
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

    if (!best || score > best.score) best = { score, runner: r };
  }

  return best && best.score >= 70 ? best.runner : null;
}

function findSelection(tip, market, cfg) {
  const runners = market.runners || [];
  const pick = String(tip.pick || '').toLowerCase();

  if (cfg.kind === 'match_odds') {
    if (pick === 'x') return runners.find((r) => /draw/i.test(r.runnerName));
    if (pick === '1') return pickRunnerByName(runners, tip.home_team);
    if (pick === '2') return pickRunnerByName(runners, tip.away_team);
  }

  if (cfg.kind === 'double_chance') {
    for (const r of runners) {
      const name = String(r.runnerName || '').toLowerCase();

      if (pick === '1x' && /(home or draw|home\/draw|home draw|1x)/i.test(name)) return r;
      if (pick === 'x2' && /(draw or away|draw\/away|draw away|x2)/i.test(name)) return r;
      if (pick === '12' && /(home or away|home\/away|home away|12)/i.test(name)) return r;
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

async function loadRawTipsForFallback(client) {
  const r = await client.query(
    `
    SELECT
      st.id AS sent_tip_id,
      NULL::bigint AS sent_tip_leg_id,
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
      AND ($2 = 'daily' OR b.tip_window = $2)
      AND st.status = 'pending'
      AND st.bet_type = 'single'
      AND st.fixture_id IS NOT NULL

    UNION ALL

    SELECT
      parent.id AS sent_tip_id,
      leg.id AS sent_tip_leg_id,
      leg.fixture_id,
      leg.market,
      leg.pick,
      leg.odds,
      parent.stake,
      parent.confidence,
      b.tip_date,
      b.tip_window,
      f.kickoff_utc,
      ht.name AS home_team,
      at.name AS away_team
    FROM sent_tip_legs leg
    JOIN sent_tips parent ON parent.id = leg.sent_tip_id
    JOIN sent_tip_batches b ON b.id = parent.batch_id
    JOIN fixtures f ON f.id = leg.fixture_id
    JOIN teams ht ON ht.id = f.home_team_id
    JOIN teams at ON at.id = f.away_team_id
    WHERE b.tip_date = $1::date
      AND ($2 = 'daily' OR b.tip_window = $2)
      AND parent.status = 'pending'
      AND leg.status = 'pending'
      AND leg.fixture_id IS NOT NULL

    ORDER BY kickoff_utc ASC, sent_tip_id ASC, sent_tip_leg_id ASC NULLS FIRST
    `,
    [TARGET_DATE, WINDOW]
  );

  return r.rows;
}

function addGrouped(grouped, item) {
  const provider = item.provider;
  const row = bfbotRow(item);

  if (!grouped.has(provider)) grouped.set(provider, []);
  grouped.get(provider).push(row);
}

async function main() {
  ensureOutputDir();

  const client = await db.getClient();
  const grouped = new Map();
  const skipped = [];

  try {
    console.log(`Exporting BF Bot tips for ${TARGET_DATE} / ${WINDOW}`);

    const mapped = await loadMappedSelections(client);
    console.log(`Loaded mapped Betfair selections: ${mapped.length}`);

    if (mapped.length > 0) {
      for (const m of mapped) {
        addGrouped(grouped, m);
      }
    } else {
      const tips = await loadRawTipsForFallback(client);
      console.log(`No mapping rows found. Fallback API lookup for tips/legs: ${tips.length}`);

      for (const tip of tips) {
        const cfg = marketConfig(tip);

        if (!cfg) {
          skipped.push({
            sent_tip_id: tip.sent_tip_id,
            sent_tip_leg_id: tip.sent_tip_leg_id,
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
              sent_tip_leg_id: tip.sent_tip_leg_id,
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
              sent_tip_leg_id: tip.sent_tip_leg_id,
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

          addGrouped(grouped, {
            source: 'fallback_api',
            provider: providerForTip(tip),
            eventId: market.event.id,
            marketId: market.marketId,
            selectionId: runner.selectionId,
            marketType: cfg.marketType,
            handicap: cfg.handicap,
            tip
          });

          console.log(
            `Mapped fallback: tip=${tip.sent_tip_id} leg=${tip.sent_tip_leg_id || '-'} | ${tip.home_team} v ${tip.away_team} | ${tip.market} ${tip.pick}`
          );
        } catch (err) {
          skipped.push({
            sent_tip_id: tip.sent_tip_id,
            sent_tip_leg_id: tip.sent_tip_leg_id,
            fixture_id: tip.fixture_id,
            market: tip.market,
            pick: tip.pick,
            reason: err.message
          });
        }
      }
    }

    const header = 'Provider,EventId,MarketId,SelectionId,MarketType,Handicap,BetType';
    const allRows = [];

    for (const [provider, rows] of grouped.entries()) {
      const uniqueRows = [...new Set(rows)];
      allRows.push(...uniqueRows);

      const filename = path.join(
        OUTPUT_DIR,
        `bfbot-${TARGET_DATE}-${WINDOW}-${safeFile(provider)}.csv`
      );

      fs.writeFileSync(filename, [header, ...uniqueRows].join('\n') + '\n', 'utf8');
      console.log(`Wrote ${uniqueRows.length} rows: ${filename}`);
    }

    const allFile = path.join(OUTPUT_DIR, `bfbot-${TARGET_DATE}-${WINDOW}-all.csv`);
    fs.writeFileSync(allFile, [header, ...new Set(allRows)].join('\n') + '\n', 'utf8');
    console.log(`Wrote ${new Set(allRows).size} rows: ${allFile}`);

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
