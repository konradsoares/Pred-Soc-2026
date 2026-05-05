const axios = require('axios');
const env = require('../config/env');
const db = require('../db/connection');

const ALERT_COOLDOWN_MINUTES = 30;

function buildAlertKey(o) {
  return `${o.betfairEventId}:${o.marketId}:${o.selectionId}`;
}

async function wasRecentlySent(alertKey) {
  const result = await db.query(
    `
    SELECT id
    FROM inplay_telegram_alerts
    WHERE alert_key = $1
      AND sent_at >= NOW() - ($2 || ' minutes')::interval
    LIMIT 1
    `,
    [alertKey, ALERT_COOLDOWN_MINUTES]
  );

  return result.rows.length > 0;
}

async function markSent(alertKey, opportunity) {
  await db.query(
    `
    INSERT INTO inplay_telegram_alerts (
      alert_key,
      betfair_event_id,
      market_id,
      selection_id,
      payload
    )
    VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (alert_key)
    DO UPDATE SET
      sent_at = NOW(),
      payload = EXCLUDED.payload
    `,
    [
      alertKey,
      opportunity.betfairEventId,
      opportunity.marketId,
      opportunity.selectionId,
      JSON.stringify(opportunity)
    ]
  );
}

async function sendTelegramMessage(text) {
  if (!env.TELEGRAM_BOT_TOKEN || !env.TELEGRAM_CHAT_ID) {
    console.warn('[telegram] Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID');
    return false;
  }

  await axios.post(
    `https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/sendMessage`,
    {
      chat_id: env.TELEGRAM_CHAT_ID,
      text,
      parse_mode: 'HTML',
      disable_web_page_preview: true
    },
    { timeout: 10000 }
  );

  return true;
}

function formatOpportunityMessage(o) {
  return `
⚽ <b>In-Play Opportunity</b>

<b>Match:</b> ${o.homeTeam} v ${o.awayTeam}
<b>Market:</b> ${o.marketType}
<b>Pick:</b> ${o.runnerName}

<b>Odds:</b> ${o.backOdd}
<b>Implied:</b> ${(o.impliedProbability * 100).toFixed(1)}%
<b>Model:</b> ${(o.modelProbability * 100).toFixed(1)}%
<b>Edge:</b> ${(o.edge * 100).toFixed(1)}%

<b>Risk:</b> ${o.riskLevel}
<b>Confidence:</b> ${o.confidenceScore}/100

<b>Reason:</b> ${o.reason || '-'}

<b>Live:</b> ${o.liveContext?.score || '-'} / ${o.liveContext?.minute || '-'}
`.trim();
}

async function sendOpportunityAlerts(opportunities) {
  const alerts = opportunities.filter(o => o.alert);

  let sent = 0;
  let skippedDuplicate = 0;

  for (const opportunity of alerts) {
    const alertKey = buildAlertKey(opportunity);

    if (await wasRecentlySent(alertKey)) {
      skippedDuplicate++;
      continue;
    }

    const ok = await sendTelegramMessage(formatOpportunityMessage(opportunity));

    if (ok) {
      await markSent(alertKey, opportunity);
      sent++;
    }
  }

  return {
    sent,
    skippedDuplicate
  };
}

module.exports = {
  sendOpportunityAlerts
};
