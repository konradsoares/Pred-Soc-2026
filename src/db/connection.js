const env = require('../config/env');
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: env.DATABASE_URL
});

module.exports = {
  query: (text, params) => pool.query(text, params),
  getClient: () => pool.connect(),
  pool
};
