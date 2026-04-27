const nodemailer = require('nodemailer');
const env = require('../config/env');

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

function confidenceColor(confidence) {
  const c = Number(confidence || 0);
  if (c >= 0.75) return '#16a34a'; // green
  if (c >= 0.62) return '#f59e0b'; // amber
  return '#ef4444'; // red
}

function oddsBadgeColor(odds) {
  const o = Number(odds || 0);
  if (o >= 2.0) return '#8b5cf6';
  if (o >= 1.5) return '#2563eb';
  return '#475569';
}

function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function fixtureNameMap(tipsFile) {
  const map = new Map();
  const fixtures = tipsFile.payload?.fixtures || [];

  for (const f of fixtures) {
    map.set(
      Number(f.fixture_id),
      `${f.home_team} vs ${f.away_team}`
    );
  }

  return map;
}

function formatTipsAsText(tipsFile) {
  const ai = tipsFile.ai_tips || {};
  const singles = ai.singles || [];
  const accas = ai.accumulators || [];
  const systems = ai.system_bets || [];
  const fixtureMap = fixtureNameMap(tipsFile);

  const lines = [];
  lines.push(`PredSoc tips for ${tipsFile.date}`);
  lines.push('');

  lines.push('Singles');
  if (!singles.length) lines.push('- none');
  for (const s of singles) {
    const name = fixtureMap.get(Number(s.fixture_id)) || `Fixture ${s.fixture_id}`;
    lines.push(
      `- ${name}: ${s.market} ${s.pick} @ ${s.odds} | confidence ${s.confidence} | ${s.reason}`
    );
  }

  lines.push('');
  lines.push('Accumulators');
  if (!accas.length) lines.push('- none');
  for (const a of accas) {
    lines.push(`- ${a.name} @ ${a.total_odds} | confidence ${a.confidence}`);
    for (const leg of a.legs) {
      const name = fixtureMap.get(Number(leg.fixture_id)) || `Fixture ${leg.fixture_id}`;
      lines.push(`  • ${name}: ${leg.market} ${leg.pick} @ ${leg.odds}`);
    }
    lines.push(`  Reason: ${a.reason}`);
  }

  lines.push('');
  lines.push('System bets');
  if (!systems.length) lines.push('- none');
  for (const s of systems) {
    lines.push(`- ${s.type}`);
    for (const leg of s.legs) {
      const name = fixtureMap.get(Number(leg.fixture_id)) || `Fixture ${leg.fixture_id}`;
      lines.push(`  • ${name}: ${leg.market} ${leg.pick} @ ${leg.odds}`);
    }
    lines.push(`  Reason: ${s.reason}`);
  }

  return lines.join('\n');
}

function renderSingleCard(single, fixtureMap) {
  const name = escapeHtml(fixtureMap.get(Number(single.fixture_id)) || `Fixture ${single.fixture_id}`);
  const market = escapeHtml(single.market);
  const pick = escapeHtml(single.pick);
  const odds = Number(single.odds || 0).toFixed(2);
  const confidence = Number(single.confidence || 0);
  const reason = escapeHtml(single.reason || '');
  const confColor = confidenceColor(confidence);
  const oddColor = oddsBadgeColor(odds);

  return `
    <tr>
      <td style="padding:0 0 14px 0;">
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;background:#111827;border:1px solid #1f2937;border-radius:14px;">
          <tr>
            <td style="padding:16px;">
              <div style="font-size:15px;font-weight:700;color:#f8fafc;margin-bottom:10px;">${name}</div>
              <div style="margin-bottom:12px;">
                <span style="display:inline-block;background:#0f172a;border:1px solid #334155;color:#e2e8f0;border-radius:999px;padding:6px 10px;font-size:12px;font-weight:700;margin-right:8px;">${market}</span>
                <span style="display:inline-block;background:#1e293b;border:1px solid #475569;color:#ffffff;border-radius:999px;padding:6px 10px;font-size:12px;font-weight:700;margin-right:8px;">${pick}</span>
                <span style="display:inline-block;background:${oddColor};color:#ffffff;border-radius:999px;padding:6px 10px;font-size:12px;font-weight:700;margin-right:8px;">ODDS ${odds}</span>
                <span style="display:inline-block;background:${confColor};color:#ffffff;border-radius:999px;padding:6px 10px;font-size:12px;font-weight:700;">CONF ${confidence.toFixed(2)}</span>
              </div>
              <div style="font-size:13px;line-height:1.5;color:#cbd5e1;">${reason}</div>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  `;
}

function renderAccaCard(acca, fixtureMap) {
  const totalOdds = Number(acca.total_odds || 0).toFixed(2);
  const confidence = Number(acca.confidence || 0);
  const confColor = confidenceColor(confidence);
  const reason = escapeHtml(acca.reason || '');
  const legs = (acca.legs || [])
    .map((leg) => {
      const name = escapeHtml(fixtureMap.get(Number(leg.fixture_id)) || `Fixture ${leg.fixture_id}`);
      return `
        <tr>
          <td style="padding:10px 0;border-top:1px solid #1f2937;">
            <div style="font-size:13px;font-weight:700;color:#f8fafc;">${name}</div>
            <div style="font-size:12px;color:#94a3b8;margin-top:4px;">
              ${escapeHtml(leg.market)} • ${escapeHtml(leg.pick)}
              <span style="display:inline-block;background:${oddsBadgeColor(leg.odds)};color:#fff;border-radius:999px;padding:4px 8px;font-size:11px;font-weight:700;margin-left:8px;">@ ${Number(leg.odds || 0).toFixed(2)}</span>
            </div>
          </td>
        </tr>
      `;
    })
    .join('');

  return `
    <tr>
      <td style="padding:0 0 14px 0;">
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;background:#111827;border:1px solid #1f2937;border-radius:14px;">
          <tr>
            <td style="padding:16px;">
              <div style="display:flex;justify-content:space-between;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:12px;">
                <div style="font-size:15px;font-weight:700;color:#f8fafc;">${escapeHtml(acca.name || 'Accumulator')}</div>
                <div>
                  <span style="display:inline-block;background:#8b5cf6;color:#fff;border-radius:999px;padding:6px 10px;font-size:12px;font-weight:700;margin-right:8px;">TOTAL ${totalOdds}</span>
                  <span style="display:inline-block;background:${confColor};color:#fff;border-radius:999px;padding:6px 10px;font-size:12px;font-weight:700;">CONF ${confidence.toFixed(2)}</span>
                </div>
              </div>
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
                ${legs}
              </table>
              <div style="font-size:13px;line-height:1.5;color:#cbd5e1;margin-top:10px;">${reason}</div>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  `;
}

function renderSystemCard(systemBet, fixtureMap) {
  const legs = (systemBet.legs || [])
    .map((leg) => {
      const name = escapeHtml(fixtureMap.get(Number(leg.fixture_id)) || `Fixture ${leg.fixture_id}`);
      return `
        <tr>
          <td style="padding:10px 0;border-top:1px solid #1f2937;">
            <div style="font-size:13px;font-weight:700;color:#f8fafc;">${name}</div>
            <div style="font-size:12px;color:#94a3b8;margin-top:4px;">
              ${escapeHtml(leg.market)} • ${escapeHtml(leg.pick)}
              <span style="display:inline-block;background:${oddsBadgeColor(leg.odds)};color:#fff;border-radius:999px;padding:4px 8px;font-size:11px;font-weight:700;margin-left:8px;">@ ${Number(leg.odds || 0).toFixed(2)}</span>
            </div>
          </td>
        </tr>
      `;
    })
    .join('');

  return `
    <tr>
      <td style="padding:0 0 14px 0;">
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;background:#111827;border:1px solid #1f2937;border-radius:14px;">
          <tr>
            <td style="padding:16px;">
              <div style="font-size:15px;font-weight:700;color:#f8fafc;margin-bottom:12px;">${escapeHtml(systemBet.type || 'System Bet')}</div>
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
                ${legs}
              </table>
              <div style="font-size:13px;line-height:1.5;color:#cbd5e1;margin-top:10px;">${escapeHtml(systemBet.reason || '')}</div>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  `;
}

function renderEmptyCard(text) {
  return `
    <tr>
      <td style="padding:0 0 14px 0;">
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;background:#111827;border:1px solid #1f2937;border-radius:14px;">
          <tr>
            <td style="padding:16px;font-size:13px;color:#94a3b8;">${escapeHtml(text)}</td>
          </tr>
        </table>
      </td>
    </tr>
  `;
}

function buildHtmlEmail(tipsFile) {
  const ai = tipsFile.ai_tips || {};
  const singles = ai.singles || [];
  const accas = ai.accumulators || [];
  const systems = ai.system_bets || [];
  const fixtureMap = fixtureNameMap(tipsFile);

  const singlesHtml = singles.length
    ? singles.map((s) => renderSingleCard(s, fixtureMap)).join('')
    : renderEmptyCard('No singles met the rules for this run.');

  const accasHtml = accas.length
    ? accas.map((a) => renderAccaCard(a, fixtureMap)).join('')
    : renderEmptyCard('No accumulator met the target odds range safely.');

  const systemsHtml = systems.length
    ? systems.map((s) => renderSystemCard(s, fixtureMap)).join('')
    : renderEmptyCard('No system bets suggested for this run.');

  return `
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>PredSoc Tips</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
</head>
<body style="margin:0;padding:0;background:#020617;font-family:Arial,Helvetica,sans-serif;">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;background:#020617;">
    <tr>
      <td align="center" style="padding:24px 12px;">
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;max-width:760px;">
          <tr>
            <td style="padding:0 0 18px 0;">
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;background:linear-gradient(135deg,#0f172a,#111827);border:1px solid #1f2937;border-radius:18px;">
                <tr>
                  <td style="padding:24px;">
                    <div style="font-size:12px;letter-spacing:1.5px;color:#38bdf8;font-weight:700;text-transform:uppercase;margin-bottom:8px;">PredSoc Daily Picks</div>
                    <div style="font-size:28px;line-height:1.2;font-weight:800;color:#f8fafc;margin-bottom:8px;">Today’s Betting Card</div>
                    <div style="font-size:14px;color:#94a3b8;">Date: ${escapeHtml(tipsFile.date)}</div>
                  </td>
                </tr>
              </table>
            </td>
          </tr>

          <tr>
            <td style="padding:8px 0 10px 0;font-size:18px;font-weight:800;color:#f8fafc;">Singles</td>
          </tr>
          ${singlesHtml}

          <tr>
            <td style="padding:8px 0 10px 0;font-size:18px;font-weight:800;color:#f8fafc;">Accumulators</td>
          </tr>
          ${accasHtml}

          <tr>
            <td style="padding:8px 0 10px 0;font-size:18px;font-weight:800;color:#f8fafc;">System Bets</td>
          </tr>
          ${systemsHtml}

          <tr>
            <td style="padding:10px 0 0 0;">
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;background:#0f172a;border:1px solid #1f2937;border-radius:14px;">
                <tr>
                  <td style="padding:16px;font-size:12px;line-height:1.6;color:#94a3b8;">
                    Synthetic odds are generated from internal probabilities for testing and ranking. They are not live bookmaker prices.
                  </td>
                </tr>
              </table>
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

async function sendTipsEmail(config, tipsFile) {
  if (!config.email.enabled) return;

  const transporter = buildTransport(config);
  const subject = `${config.email.subject_prefix} ${tipsFile.date} Today's Picks`;
  const text = formatTipsAsText(tipsFile);
  const html = buildHtmlEmail(tipsFile);

  await transporter.sendMail({
    from: config.email.from,
    to: config.email.to.join(', '),
    subject,
    text,
    html
  });
}

module.exports = {
  sendTipsEmail
};
