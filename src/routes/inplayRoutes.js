const express = require('express');
const { scanInplayOpportunities } = require('../inplay/inplayScanner');
const { sendOpportunityAlerts } = require('../inplay/telegramAlert');
const router = express.Router();

// router.post('/scan', async (req, res) => {
//   try {
//     const result = await scanInplayOpportunities();

//     res.json({
//       ok: true,
//       ...result
//     });
//   } catch (error) {
//     console.error('[inplay scan failed]', error);

//     res.status(500).json({
//       ok: false,
//       error: error.message
//     });
//   }
// });
router.post('/scan', async (req, res) => {
  try {
    const debug =
      req.query.debug === 'true' ||
      req.body?.debug === true;

    const telegram =
      req.query.telegram === 'true' ||
      req.body?.telegram === true;

    const result = await scanInplayOpportunities({ debug });

    let telegramSent = 0;

    if (telegram) {
      telegramSent = await sendOpportunityAlerts(result.opportunities || []);
    }

    res.json({
      ok: true,
      telegramEnabled: telegram,
      telegramSent,
      ...result
    });
  } catch (error) {
    console.error('[inplay scan failed]', error);

    res.status(500).json({
      ok: false,
      error: error.message
    });
  }
});
const db = require('../db/connection');

router.get('/recent', async (req, res) => {
  try {
    const run = await db.query(`
      SELECT *
      FROM inplay_scan_runs
      ORDER BY id DESC
      LIMIT 1
    `);

    if (!run.rows.length) {
      return res.json({
        ok: true,
        opportunities: [],
        message: 'No scans yet'
      });
    }

    const scanRunId = run.rows[0].id;

    const opps = await db.query(
      `
      SELECT *
      FROM inplay_opportunities
      WHERE scan_run_id = $1
      ORDER BY edge DESC
      `,
      [scanRunId]
    );

    res.json({
      ok: true,
      scanRun: run.rows[0],
      opportunities: opps.rows
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, error: err.message });
  }
});
module.exports = router;
