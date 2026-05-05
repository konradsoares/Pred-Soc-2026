const express = require('express');
const path = require('path');
const env = require('./config/env');

const inplayRoutes = require('./routes/inplayRoutes');

const app = express();

const PORT = env.PORT || process.env.PORT || 3000;

app.use(express.json());

app.use('/api/inplay', inplayRoutes);

app.use('/', express.static(path.join(__dirname, '../public')));

app.get('/health', (req, res) => {
  res.json({
    ok: true,
    service: 'pred-soc-2026-inplay',
    timestamp: new Date().toISOString()
  });
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`[server] Pred-Soc-2026 intranet server running on port ${PORT}`);
});
