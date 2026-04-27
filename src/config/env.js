const path = require('path');
require('dotenv').config({
  path: path.resolve(__dirname, '../../docker/.env')
});

module.exports = process.env;
