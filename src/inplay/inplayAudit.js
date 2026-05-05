const db = require('../db/connection');

async function startScanRun() {
  const result = await db.query(`
    INSERT INTO inplay_scan_runs (status)
    VALUES ('running')
    RETURNING id
  `);

  return result.rows[0].id;
}

async function finishScanRun(scanRunId, payload = {}) {
  const {
    status = 'finished',
    totalEvents = 0,
    totalMarkets = 0,
    totalOpportunities = 0,
    errorMessage = null,
  } = payload;

  await db.query(
    `
    UPDATE inplay_scan_runs
    SET
      finished_at = NOW(),
      status = $2,
      total_events = $3,
      total_markets = $4,
      total_opportunities = $5,
      error_message = $6
    WHERE id = $1
    `,
    [
      scanRunId,
      status,
      totalEvents,
      totalMarkets,
      totalOpportunities,
      errorMessage,
    ]
  );
}

async function saveOpportunity(scanRunId, opportunity) {
  await db.query(
    `
    INSERT INTO inplay_opportunities (
      scan_run_id,
      betfair_event_id,
      betfair_event_name,
      fixture_id,
      home_team,
      away_team,
      market_id,
      market_type,
      market_status,
      selection_id,
      runner_name,
      back_odd,
      implied_probability,
      model_probability,
      edge,
      risk_level,
      reason,
      stats_summary,
      raw_snapshot
    )
    VALUES (
      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
      $11,$12,$13,$14,$15,$16,$17,$18,$19
    )
    `,
    [
      scanRunId,
      opportunity.betfairEventId,
      opportunity.betfairEventName,
      opportunity.fixtureId || null,
      opportunity.homeTeam || null,
      opportunity.awayTeam || null,
      opportunity.marketId,
      opportunity.marketType,
      opportunity.marketStatus,
      opportunity.selectionId,
      opportunity.runnerName,
      opportunity.backOdd,
      opportunity.impliedProbability,
      opportunity.modelProbability,
      opportunity.edge,
      opportunity.riskLevel,
      opportunity.reason,
      JSON.stringify(opportunity.statsSummary || {}),
      JSON.stringify(opportunity.rawSnapshot || {}),
    ]
  );
}

module.exports = {
  startScanRun,
  finishScanRun,
  saveOpportunity,
};
