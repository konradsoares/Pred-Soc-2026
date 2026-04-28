const fs = require('fs');
const path = require('path');
const nodemailer = require('nodemailer');
const db = require('../db/connection');
const env = require('../config/env');

const ROOT_DIR = path.resolve(__dirname, '../..');
const CONFIG_PATH = path.join(ROOT_DIR, 'config', 'app.config.json');

function loadConfig() {
  return JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
}

function todayDateISO() {
  return new Date().toISOString().slice(0, 10);
}

function buildTransport(config) {
  return nodemailer.createTransport({
    host: config.email.smtp_host,
    port: config.email.smtp_port,
    secure: config.email.secure,
    auth: {
      user: env[config.email.user_env],
      pass: env[config.email.pass_env]
    }
  });
}

function money(value, currency = 'EUR') {
  const n = Number(value || 0);
  return `${currency === 'EUR' ? '€' : currency + ' '}${n.toFixed(2)}`;
}

function statusColor(status) {
  if (status === 'won') return '#16a34a';
  if (status === 'lost') return '#dc2626';
  if (status === 'void') return '#64748b';
  return '#f59e0b';
}

function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

async function loadResults(client, targetDate) {
  const result = await client.query(
    `
    SELECT
      b.tip_date,
      b.tip_window,
      b.currency,
      st.id AS tip_id,
      st.bet_type,
      st.market,
      st.pick,
      st.odds,
      st.stake,
      st.status,
      st.profit_loss,
      st.fixture_id,
      ht.name AS home_team,
      at.name AS away_team,
      fs.home_goals,
      fs.away_goals
    FROM sent_tips st
    JOIN sent_tip_batches b ON b.id = st.batch_id
    LEFT JOIN fixtures f ON f.id = st.fixture_id
    LEFT JOIN teams ht ON ht.id = f.home_team_id
    LEFT JOIN teams at ON at.id = f.away_team_id
    LEFT JOIN fixture_scores fs ON fs.fixture_id = f.id
    WHERE b.tip_date = $1::date
    ORDER BY b.tip_window, st.bet_type, st.id
    `,
    [targetDate]
  );

  return result.rows;
}

async function loadAccumulatorLegs(client, tipIds) {
  if (!tipIds.length) return [];

  const result = await client.query(
    `
    SELECT
      l.sent_tip_id,
      l.market,
      l.pick,
      l.odds,
      l.status,
      ht.name AS home_team,
      at.name AS away_team,
      fs.home_goals,
      fs.away_goals
    FROM sent_tip_legs l
    LEFT JOIN fixtures f ON f.id = l.fixture_id
    LEFT JOIN teams ht ON ht.id = f.home_team_id
    LEFT JOIN teams at ON at.id = f.away_team_id
    LEFT JOIN fixture_scores fs ON fs.fixture_id = f.id
    WHERE l.sent_tip_id = ANY($1::bigint[])
    ORDER BY l.sent_tip_id, l.id
    `,
    [tipIds]
  );

  return result.rows;
}

function summarize(rows) {
  const totalStaked = rows.reduce((sum, r) => sum + Number(r.stake || 0), 0);
  const totalPL = rows.reduce((sum, r) => sum + Number(r.profit_loss || 0), 0);
  const won = rows.filter((r) => r.status === 'won').length;
  const lost = rows.filter((r) => r.status === 'lost').length;
  const pending = rows.filter((r) => r.status === 'pending').length;

  return { totalStaked, totalPL, won, lost, pending };
}

function renderTipRow(row, currency) {
  const fixture = row.home_team
    ? `${row.home_team} vs ${row.away_team}`
    : `${row.bet_type} #${row.tip_id}`;

  const score =
    row.home_goals !== null && row.away_goals !== null
      ? `${row.home_goals}-${row.away_goals}`
      : 'pending score';

  return `
    <tr>
      <td style="padding:12px;border-bottom:1px solid #1f2937;">
        <div style="font-weight:700;color:#f8fafc;">${escapeHtml(fixture)}</div>
        <div style="font-size:12px;color:#94a3b8;margin-top:4px;">
          ${escapeHtml(row.market || row.bet_type)} • ${escapeHtml(row.pick || '')} @ ${Number(row.odds || 0).toFixed(2)}
          • Stake ${money(row.stake, currency)}
          • Score ${escapeHtml(score)}
        </div>
      </td>
      <td style="padding:12px;border-bottom:1px solid #1f2937;text-align:right;">
        <span style="display:inline-block;background:${statusColor(row.status)};color:#fff;border-radius:999px;padding:5px 9px;font-size:12px;font-weight:700;">
          ${escapeHtml(row.status)}
        </span>
      </td>
      <td style="padding:12px;border-bottom:1px solid #1f2937;text-align:right;color:${Number(row.profit_loss || 0) >= 0 ? '#22c55e' : '#ef4444'};font-weight:700;">
        ${row.profit_loss === null ? '-' : money(row.profit_loss, currency)}
      </td>
    </tr>
  `;
}

function renderLegs(tipId, legs) {
  const rows = legs
    .filter((l) => Number(l.sent_tip_id) === Number(tipId))
    .map((l) => {
      const score =
        l.home_goals !== null && l.away_goals !== null
          ? `${l.home_goals}-${l.away_goals}`
          : 'pending score';

      return `
        <div style="font-size:12px;color:#cbd5e1;margin-top:6px;padding-left:10px;border-left:2px solid #334155;">
          ${escapeHtml(l.home_team || 'Unknown')} vs ${escapeHtml(l.away_team || 'Unknown')}
          • ${escapeHtml(l.market)} ${escapeHtml(l.pick)}
          @ ${Number(l.odds || 0).toFixed(2)}
          • ${escapeHtml(score)}
          • <span style="color:${statusColor(l.status)};font-weight:700;">${escapeHtml(l.status)}</span>
        </div>
      `;
    })
    .join('');

  return rows || '';
}

function buildHtml(targetDate, windowName, rows, legs) {
  const currency = rows[0]?.currency || 'EUR';
  const summary = summarize(rows);

  const singles = rows.filter((r) => r.bet_type === 'single');
  const accas = rows.filter((r) => r.bet_type === 'accumulator');
  const systems = rows.filter((r) => r.bet_type === 'system');

  const renderAcca = (row) => `
    <tr>
      <td style="padding:12px;border-bottom:1px solid #1f2937;">
        <div style="font-weight:700;color:#f8fafc;">Accumulator #${row.tip_id}</div>
        <div style="font-size:12px;color:#94a3b8;margin-top:4px;">
          Total odds ${Number(row.odds || 0).toFixed(2)}
          • Stake ${money(row.stake, currency)}
        </div>
        ${renderLegs(row.tip_id, legs)}
      </td>
      <td style="padding:12px;border-bottom:1px solid #1f2937;text-align:right;">
        <span style="display:inline-block;background:${statusColor(row.status)};color:#fff;border-radius:999px;padding:5px 9px;font-size:12px;font-weight:700;">
          ${escapeHtml(row.status)}
        </span>
      </td>
      <td style="padding:12px;border-bottom:1px solid #1f2937;text-align:right;color:${Number(row.profit_loss || 0) >= 0 ? '#22c55e' : '#ef4444'};font-weight:700;">
        ${row.profit_loss === null ? '-' : money(row.profit_loss, currency)}
      </td>
    </tr>
  `;

  return `
<!DOCTYPE html>
<html>
<body style="margin:0;padding:0;background:#020617;font-family:Arial,Helvetica,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#020617;padding:24px 12px;">
    <tr>
      <td align="center">
        <table width="100%" cellpadding="0" cellspacing="0" style="max-width:820px;border-collapse:collapse;">
          <tr>
            <td style="background:#0f172a;border:1px solid #1f2937;border-radius:18px;padding:24px;">
              <div style="font-size:12px;letter-spacing:1.5px;color:#38bdf8;font-weight:700;text-transform:uppercase;">PredSoc Results</div>
              <div style="font-size:28px;font-weight:800;color:#f8fafc;margin-top:8px;">Results & P/L</div>
              <div style="font-size:14px;color:#94a3b8;margin-top:8px;">Date: ${escapeHtml(targetDate)} ${windowName ? `• Window: ${escapeHtml(windowName)}` : ''}</div>
            </td>
          </tr>

          <tr><td style="height:16px;"></td></tr>

          <tr>
            <td style="background:#111827;border:1px solid #1f2937;border-radius:14px;padding:16px;">
              <table width="100%">
                <tr>
                  <td style="color:#94a3b8;font-size:12px;">Staked</td>
                  <td style="color:#94a3b8;font-size:12px;">Won</td>
                  <td style="color:#94a3b8;font-size:12px;">Lost</td>
                  <td style="color:#94a3b8;font-size:12px;">Pending</td>
                  <td style="color:#94a3b8;font-size:12px;text-align:right;">P/L</td>
                </tr>
                <tr>
                  <td style="color:#f8fafc;font-weight:800;font-size:20px;">${money(summary.totalStaked, currency)}</td>
                  <td style="color:#22c55e;font-weight:800;font-size:20px;">${summary.won}</td>
                  <td style="color:#ef4444;font-weight:800;font-size:20px;">${summary.lost}</td>
                  <td style="color:#f59e0b;font-weight:800;font-size:20px;">${summary.pending}</td>
                  <td style="text-align:right;color:${summary.totalPL >= 0 ? '#22c55e' : '#ef4444'};font-weight:800;font-size:20px;">${money(summary.totalPL, currency)}</td>
                </tr>
              </table>
            </td>
          </tr>

          <tr><td style="height:18px;"></td></tr>

          <tr><td style="color:#f8fafc;font-size:18px;font-weight:800;padding-bottom:8px;">Singles</td></tr>
          <tr>
            <td style="background:#111827;border:1px solid #1f2937;border-radius:14px;overflow:hidden;">
              <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
                ${singles.length ? singles.map((r) => renderTipRow(r, currency)).join('') : '<tr><td style="padding:16px;color:#94a3b8;">No singles.</td></tr>'}
              </table>
            </td>
          </tr>

          <tr><td style="height:18px;"></td></tr>

          <tr><td style="color:#f8fafc;font-size:18px;font-weight:800;padding-bottom:8px;">Accumulators</td></tr>
          <tr>
            <td style="background:#111827;border:1px solid #1f2937;border-radius:14px;overflow:hidden;">
              <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
                ${accas.length ? accas.map(renderAcca).join('') : '<tr><td style="padding:16px;color:#94a3b8;">No accumulators.</td></tr>'}
              </table>
            </td>
          </tr>

          <tr><td style="height:18px;"></td></tr>

          <tr><td style="color:#f8fafc;font-size:18px;font-weight:800;padding-bottom:8px;">System Bets</td></tr>
          <tr>
            <td style="background:#111827;border:1px solid #1f2937;border-radius:14px;padding:16px;color:#94a3b8;">
              ${systems.length ? `${systems.length} stored, not auto-settled yet.` : 'No system bets.'}
            </td>
          </tr>

        </table>
      </td>
    </tr>
  </table>
</body>
</html>
  `;
}

async function sendResultsEmail(config, targetDate, windowName, rows, legs) {
  if (!config.email.enabled) return;
  if (!rows.length) {
    console.log('No result rows found. Email not sent.');
    return;
  }

  const transporter = buildTransport(config);
  const html = buildHtml(targetDate, windowName, rows, legs);
  const summary = summarize(rows);
  const currency = rows[0]?.currency || 'EUR';

  await transporter.sendMail({
    from: config.email.from,
    to: config.email.to.join(', '),
    subject: `${config.email.subject_prefix} Results ${targetDate}${windowName ? ` ${windowName}` : ''} | P/L ${money(summary.totalPL, currency)}`,
    html,
    text: `PredSoc Results ${targetDate} ${windowName || ''}\nP/L: ${money(summary.totalPL, currency)}`
  });
}

async function main() {
  const targetDate = process.argv[2] || todayDateISO();
  const windowName = process.argv[3] || null;

  const config = loadConfig();
  const client = await db.getClient();

  try {
    const rows = await loadResults(client, targetDate);
    const tipIds = rows
      .filter((r) => r.bet_type === 'accumulator' || r.bet_type === 'system')
      .map((r) => r.tip_id);

    const legs = await loadAccumulatorLegs(client, tipIds);

    await sendResultsEmail(config, targetDate, windowName, rows, legs);
    console.log(`Results email processed for ${targetDate}${windowName ? ` / ${windowName}` : ''}`);
  } finally {
    client.release();
    await db.pool.end();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
