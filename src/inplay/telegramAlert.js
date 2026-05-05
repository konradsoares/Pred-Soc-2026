const axios = require('axios');
const env = require('../config/env');

async function sendTelegramMessage(text) {
  if (!env.TELEGRAM_BOT_TOKEN || !env.TELEGRAM_CHAT_ID) {
    console.warn('[telegram] Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID');
    return;
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

  for (const opportunity of alerts) {
    await sendTelegramMessage(formatOpportunityMessage(opportunity));
  }

  return alerts.length;
}

module.exports = {
  sendOpportunityAlerts
};
