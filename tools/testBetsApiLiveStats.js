const {
  getBetsApiInplayGames,
  getBetsApiMatchStats,
  findBestBetsApiMatch
} = require('../src/inplay/liveStatsValidator');

async function main() {
  const testOpportunity = {
    homeTeam: 'FC Seoul',
    awayTeam: 'FC Anyang',
    betfairEventName: 'FC Seoul v FC Anyang',
    marketType: 'OVER_UNDER_25',
    runnerName: 'Over 2.5 Goals'
  };

  console.log('Fetching BetsAPI in-play games...');
  const games = await getBetsApiInplayGames();

  console.log(`Found games: ${games.length}`);

  console.log('\nFirst 10 games:');
  console.table(
    games.slice(0, 10).map(g => ({
      id: g.betsapiId,
      league: g.league,
      home: g.home,
      score: g.score,
      away: g.away,
      url: g.url
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
  const stats = await getBetsApiMatchStats(match.url);

  console.log(JSON.stringify(stats, null, 2));
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
