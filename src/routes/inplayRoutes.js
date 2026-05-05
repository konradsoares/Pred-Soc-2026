const express = require('express');
const { scanInplayOpportunities } = require('../inplay/inplayScanner');

const router = express.Router();

router.post('/scan', async (req, res) => {
  try {
    const result = await scanInplayOpportunities();

    res.json({
      ok: true,
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

module.exports = router;
