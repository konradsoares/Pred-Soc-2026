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
    .replace(/\bfk\b/g, '')
    .replace(/\butd\b/g, 'united')
    .replace(/\bst\b/g, 'saint')
    .replace(/[^\w\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .replace(/[^a-z0-9]/g, '')
    .trim();
}
const sameCountry =
  normalize(statarea.country) === normalize(betfair.country);

const sameCompetition =
  normalize(statarea.competition).includes(normalize(betfair.competition)) ||
  normalize(betfair.competition).includes(normalize(statarea.competition));

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

function minutesDiff(a, b) {
  const da = new Date(a).getTime();
  const dbb = new Date(b).getTime();

  if (!Number.isFinite(da) || !Number.isFinite(dbb)) return Infinity;

  return Math.abs(da - dbb) / 1000 / 60;
}

function combinedScore(betfairFixture, statareaFixture) {
  const homeScore = nameScore(betfairFixture.home_team, statareaFixture.home_team);
  const awayScore = nameScore(betfairFixture.away_team, statareaFixture.away_team);
  const timeDiff = minutesDiff(betfairFixture.kickoff_utc, statareaFixture.kickoff_utc);

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
      co.name AS country,
      f.compare_url
    FROM fixtures f
    JOIN teams ht ON ht.id = f.home_team_id
    JOIN teams at ON at.id = f.away_team_id
    LEFT JOIN competitions c ON c.id = f.competition_id
    LEFT JOIN countries co ON co.id = f.country_id
    WHERE f.source_name = $1
      AND f.fixture_date = $2::date
      AND (
        f.compare_url IS NULL
        OR NOT EXISTS (
          SELECT 1
          FROM scraped_predictions sp
          WHERE sp.fixture_id = f.id
            AND sp.source_name = f.source_name
        )
      )
    ORDER BY f.kickoff_utc ASC, f.id ASC
    `,
    [SOURCE_NAME, targetDate]
  );

  return result.rows;
}

function findStrictMatch(betfairFixture, statareaFixtures) {
  const exactCandidates = [];
  const fuzzyCandidates = [];

  for (const statarea of statareaFixtures) {
    const timeDiff = minutesDiff(betfairFixture.kickoff_utc, statarea.kickoff_utc);

    if (timeDiff > 60) continue;

    const homeExact = namesFullyMatch(betfairFixture.home_team, statarea.home_team);
    const awayExact = namesFullyMatch(betfairFixture.away_team, statarea.away_team);

    if (homeExact && awayExact) {
      exactCandidates.push({ statarea, timeDiff });
      continue;
    }

    const score = combinedScore(betfairFixture, statarea);

    if (
      score.totalScore >= 0.50 &&
      score.homeScore >= 0.50 &&
      score.awayScore >= 0.50
    ) {
      fuzzyCandidates.push({
        statarea,
        ...score
      });
    }
  }

  if (exactCandidates.length === 1) {
    return {
      type: 'exact',
      statarea: exactCandidates[0].statarea,
      score: {
        totalScore: 1,
        homeScore: 1,
        awayScore: 1,
        timeDiff: exactCandidates[0].timeDiff
      }
    };
  }

  if (fuzzyCandidates.length === 1) {
    return {
      type: 'fuzzy',
      statarea: fuzzyCandidates[0].statarea,
      score: fuzzyCandidates[0]
    };
  }

  return null;
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

async function upsertScrapedPrediction(client, fixtureId, sourceName, statareaFixture) {
  await client.query(
    `
    INSERT INTO scraped_predictions (
      fixture_id,
      source_name,
      tip,
      prob_home,
      prob_draw,
      prob_away,
      prob_over_25,
      prob_under_25,
      raw_payload
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
    ON CONFLICT (fixture_id, source_name)
    DO UPDATE SET
      tip = EXCLUDED.tip,
      prob_home = EXCLUDED.prob_home,
      prob_draw = EXCLUDED.prob_draw,
      prob_away = EXCLUDED.prob_away,
      prob_over_25 = EXCLUDED.prob_over_25,
      prob_under_25 = EXCLUDED.prob_under_25,
      raw_payload = EXCLUDED.raw_payload
    `,
    [
      fixtureId,
      sourceName,
      statareaFixture.tip || null,
      statareaFixture.prob_home || null,
      statareaFixture.prob_draw || null,
      statareaFixture.prob_away || null,
      statareaFixture.prob_over_25 || null,
      statareaFixture.prob_under_25 || null,
      JSON.stringify(statareaFixture)
    ]
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

    console.log(`Betfair fixtures to match/update: ${betfairFixtures.length}`);

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

      await updateCompareUrl(client, fixture.fixture_id, match.statarea.compare_url);

      await upsertScrapedPrediction(
        client,
        fixture.fixture_id,
        SOURCE_NAME,
        match.statarea
      );

      matched += 1;

      console.log(
        `Matched (${match.type}): ${fixture.home_team} v ${fixture.away_team} -> ` +
        `${match.statarea.compare_url} ` +
        `(score=${match.score.totalScore}, home=${match.score.homeScore.toFixed(2)}, ` +
        `away=${match.score.awayScore.toFixed(2)}, timeDiff=${Math.round(match.score.timeDiff)}m)`
      );
    }

    await client.query('COMMIT');

    console.log(`Match finished. Matched/updated: ${matched}, skipped: ${skipped}`);
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
