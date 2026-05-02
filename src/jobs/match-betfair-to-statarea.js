const db = require('../db/connection');
const { fetchFixturesByDate } = require('../scrapers/statareaFixtures');

const SOURCE_NAME = 'betfair';

function todayDateISO() {
  return new Date().toISOString().slice(0, 10);
}

function normalizeName(value) {
  return String(value || '')
    .toLowerCase()
    .normalize('NFKD')
    .replace(/&/g, 'and')
    .replace(/\bfc\b/g, '')
    .replace(/\bafc\b/g, '')
    .replace(/\bcf\b/g, '')
    .replace(/\bsc\b/g, '')
    .replace(/\butd\b/g, 'united')
    .replace(/\bst\b/g, 'saint')
    .replace(/[^\w\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function namesFullyMatch(a, b) {
  const left = normalizeName(a);
  const right = normalizeName(b);

  if (!left || !right) return false;

  return left === right;
}

function tokenSet(value) {
  return new Set(normalizeName(value).split(' ').filter(Boolean));
}

function nameScore(a, b) {
  const left = tokenSet(a);
  const right = tokenSet(b);

  if (!left.size || !right.size) return 0;

  let matches = 0;

  for (const token of left) {
    if (right.has(token)) matches += 1;
  }

  return matches / Math.max(left.size, right.size);
}

function combinedScore(betfairFixture, statareaFixture) {
  const homeScore = nameScore(
    betfairFixture.home_team,
    statareaFixture.home_team
  );

  const awayScore = nameScore(
    betfairFixture.away_team,
    statareaFixture.away_team
  );

  const timeDiff = minutesDiff(
    betfairFixture.kickoff_utc,
    statareaFixture.kickoff_utc
  );

  return {
    homeScore,
    awayScore,
    timeDiff,
    totalScore: Number(((homeScore + awayScore) / 2).toFixed(3))
  };
}

function debugClosestMatches(betfairFixture, statareaFixtures, limit = 5) {
  return statareaFixtures
    .map((s) => ({
      home_team: s.home_team,
      away_team: s.away_team,
      kickoff_utc: s.kickoff_utc,
      compare_url: s.compare_url,
      ...combinedScore(betfairFixture, s)
    }))
    .filter((x) => x.timeDiff <= 180)
    .sort((a, b) => {
      if (b.totalScore !== a.totalScore) return b.totalScore - a.totalScore;
      return a.timeDiff - b.timeDiff;
    })
    .slice(0, limit);
}

function minutesDiff(a, b) {
  const da = new Date(a).getTime();
  const dbb = new Date(b).getTime();

  if (!Number.isFinite(da) || !Number.isFinite(dbb)) return Infinity;

  return Math.abs(da - dbb) / 1000 / 60;
}

async function loadBetfairFixtures(client, targetDate) {
  const result = await client.query(
    `
    SELECT
      f.id AS fixture_id,
      f.external_id AS betfair_event_id,
      f.kickoff_utc,
      ht.name AS home_team,
      at.name AS away_team,
      c.name AS competition,
      co.name AS country
    FROM fixtures f
    JOIN teams ht ON ht.id = f.home_team_id
    JOIN teams at ON at.id = f.away_team_id
    LEFT JOIN competitions c ON c.id = f.competition_id
    LEFT JOIN countries co ON co.id = f.country_id
    WHERE f.source_name = $1
      AND f.fixture_date = $2::date
      AND f.compare_url IS NULL
    ORDER BY f.kickoff_utc ASC, f.id ASC
    `,
    [SOURCE_NAME, targetDate]
  );

  return result.rows;
}

function findStrictMatch(betfairFixture, statareaFixtures) {
  const candidates = [];

  for (const statarea of statareaFixtures) {
    const homeMatch = namesFullyMatch(
      betfairFixture.home_team,
      statarea.home_team
    );

    const awayMatch = namesFullyMatch(
      betfairFixture.away_team,
      statarea.away_team
    );

    if (!homeMatch || !awayMatch) continue;

    const timeDiff = minutesDiff(
      betfairFixture.kickoff_utc,
      statarea.kickoff_utc
    );

    if (timeDiff > 120) continue;

    candidates.push({
      statarea,
      timeDiff
    });
  }

  if (candidates.length !== 1) {
    return null;
  }

  return candidates[0].statarea;
}

async function updateCompareUrl(client, fixtureId, compareUrl) {
  await client.query(
    `
    UPDATE fixtures
    SET compare_url = $1,
        scraped_at = NOW()
    WHERE id = $2
    `,
    [compareUrl, fixtureId]
  );
}

async function main() {
  const targetDate = process.argv[2] || todayDateISO();

  console.log(`Matching Betfair fixtures to Statarea for ${targetDate}`);

  console.log('Fetching Statarea fixtures...');
  const statareaFixtures = await fetchFixturesByDate(targetDate);

  const usableStatareaFixtures = statareaFixtures.filter(
    (f) => f.home_team && f.away_team && f.compare_url
  );

  console.log(`Statarea fixtures scraped: ${statareaFixtures.length}`);
  console.log(`Statarea fixtures usable: ${usableStatareaFixtures.length}`);

  const client = await db.getClient();

  try {
    await client.query('BEGIN');

    const betfairFixtures = await loadBetfairFixtures(client, targetDate);

    console.log(`Betfair fixtures to match: ${betfairFixtures.length}`);

    let matched = 0;
    let skipped = 0;

    for (const fixture of betfairFixtures) {
      const match = findStrictMatch(fixture, usableStatareaFixtures);

      if (!match) {
        skipped += 1;
      
        console.log('\nNo strict match:');
        console.log(`Betfair: ${fixture.home_team} v ${fixture.away_team}`);
        console.log(`Kickoff: ${fixture.kickoff_utc}`);
        console.log(`Competition: ${fixture.competition || 'Unknown'}`);
        console.log(`Country: ${fixture.country || 'Unknown'}`);
      
        const closest = debugClosestMatches(fixture, usableStatareaFixtures, 5);
      
        if (!closest.length) {
          console.log('Closest Statarea candidates: none within 180 minutes');
        } else {
          console.log('Closest Statarea candidates:');
      
          for (const c of closest) {
            console.log(
              `  - ${c.home_team} v ${c.away_team} | ` +
              `score=${c.totalScore} home=${c.homeScore.toFixed(2)} away=${c.awayScore.toFixed(2)} ` +
              `timeDiff=${Math.round(c.timeDiff)}m | ${c.compare_url}`
            );
          }
        }
      
        continue;
      }

      await updateCompareUrl(client, fixture.fixture_id, match.compare_url);

      matched += 1;

      console.log(
        `Matched: ${fixture.home_team} v ${fixture.away_team} -> ${match.compare_url}`
      );
    }

    await client.query('COMMIT');

    console.log(`Match finished. Matched: ${matched}, skipped: ${skipped}`);
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Match failed:', err.message);
    process.exitCode = 1;
  } finally {
    client.release();
    await db.pool.end();
  }
}

main();
