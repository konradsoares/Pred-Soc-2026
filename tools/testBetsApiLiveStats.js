require('../src/config/env');

const {
  getBetsApiInplayGames,
  getBetsApiMatchStats,
  findBestBetsApiMatch,
  validateLiveStats
} = require('../src/inplay/liveStatsValidator');

async function main() {
  const testOpportunity = {
    homeTeam: process.argv[2] || 'FC Seoul',
    awayTeam: process.argv[3] || 'FC Anyang',
    betfairEventName: `${process.argv[2] || 'FC Seoul'} v ${process.argv[3] || 'FC Anyang'}`,
    marketType: process.argv[4] || 'OVER_UNDER_25',
    runnerName: process.argv[5] || 'Over 2.5 Goals'
  };

  console.log('Test opportunity:');
  console.log(testOpportunity);

  console.log('\nFetching BetsAPI in-play games...');
  const games = await getBetsApiInplayGames();

  console.log(`Found games: ${games.length}`);

  console.log('\nFirst 10 normalized games:');
  console.table(
    games.slice(0, 10).map(g => ({
      id: g.id,
      league: g.league,
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

  console.log('\nFetching match stats...');
  const stats = await getBetsApiMatchStats(match.id);

  console.log(JSON.stringify({
    score: stats.score,
    minute: stats.minute,
    stats: stats.stats
  }, null, 2));

  console.log('\nRunning full validation...');
  const validation = await validateLiveStats(testOpportunity);

  console.log(JSON.stringify(validation, null, 2));
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
