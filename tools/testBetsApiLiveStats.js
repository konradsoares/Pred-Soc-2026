require('../src/config/env');

const {
  getBetsApiInplayGames,
  getBetsApiMatchStats,
  findBestBetsApiMatch,
  validateLiveStats
} = require('../src/inplay/liveStatsValidator');

function usage() {
  console.log(`
Usage:

node tools/testBetsApiLiveStats.js "Home Team" "Away Team" "MARKET_TYPE" "Runner Name"

Example:

node tools/testBetsApiLiveStats.js "Gnistan" "FC Inter" "DOUBLE_CHANCE" "Draw or Away"
`);
}

async function main() {
  const homeTeam = process.argv[2];
  const awayTeam = process.argv[3];
  const marketType = process.argv[4];
  const runnerName = process.argv[5];

  if (!homeTeam || !awayTeam || !marketType || !runnerName) {
    usage();
    process.exit(1);
  }

  const testOpportunity = {
    homeTeam,
    awayTeam,
    betfairEventName: `${homeTeam} v ${awayTeam}`,
    marketType,
    runnerName
  };

  console.log('Test opportunity:');
  console.log(testOpportunity);

  console.log('\nFetching BetsAPI Betfair Exchange in-play football games...');
  const games = await getBetsApiInplayGames();

  console.log(`Found football games: ${games.length}`);

  console.log('\nFirst 20 normalized games:');
  console.table(
    games.slice(0, 20).map(g => ({
      id: g.id,
      ourEventId: g.ourEventId,
      league: g.league,
      eventName: g.eventName,
      home: g.home,
      score: g.score,
      away: g.away,
      minute: g.minute
    }))
  );

  console.log('\nFinding best match...');
  const match = findBestBetsApiMatch(testOpportunity, games);

  console.log(match);

  if (!match) {
    console.log('No confident match found.');
    return;
  }

  console.log('\nFetching match stats/details...');
  const stats = await getBetsApiMatchStats(match.ourEventId || match.id);

  console.log(JSON.stringify({
    score: stats.score,
    minute: stats.minute,
    stats: stats.stats,
    rawKeys: Object.keys(stats.raw || {})
  }, null, 2));

  console.log('\nRunning full validation...');
  const validation = await validateLiveStats(testOpportunity);

  console.log(JSON.stringify(validation, null, 2));
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
