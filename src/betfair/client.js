const axios = require('axios');
const env = require('../config/env');

const BETFAIR_RPC_URL = 'https://api.betfair.com/exchange/betting/json-rpc/v1';

class BetfairClient {
  constructor() {
    if (!env.BETFAIR_APP_KEY) {
      throw new Error('BETFAIR_APP_KEY is missing from docker/.env');
    }

    if (!env.BETFAIR_SESSION_TOKEN) {
      throw new Error('BETFAIR_SESSION_TOKEN is missing from docker/.env');
    }

    this.appKey = env.BETFAIR_APP_KEY;
    this.sessionToken = env.BETFAIR_SESSION_TOKEN;
  }

  async rpc(method, params) {
    const payload = {
      jsonrpc: '2.0',
      method: `SportsAPING/v1.0/${method}`,
      params,
      id: Date.now()
    };

    const response = await axios.post(BETFAIR_RPC_URL, payload, {
      timeout: 30000,
      headers: {
        'X-Application': this.appKey,
        'X-Authentication': this.sessionToken,
        'Content-Type': 'application/json'
      }
    });

    if (response.data.error) {
      throw new Error(
        `Betfair ${method} failed: ${JSON.stringify(response.data.error)}`
      );
    }

    return response.data.result;
  }

  listEvents(filter) {
    return this.rpc('listEvents', { filter });
  }

  listMarketCatalogue(filter, maxResults = '1000') {
    return this.rpc('listMarketCatalogue', {
      filter,
      maxResults,
      sort: 'FIRST_TO_START',
      marketProjection: [
        'EVENT',
        'COMPETITION',
        'MARKET_START_TIME',
        'MARKET_DESCRIPTION',
        'RUNNER_DESCRIPTION'
      ]
    });
  }
  listMarketBook(marketIds) {
    return this.rpc('listMarketBook', {
      marketIds,
      priceProjection: {
        priceData: ['EX_BEST_OFFERS']
      }
    });
  }  
}

module.exports = BetfairClient;
