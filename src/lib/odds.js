function clamp(val, min, max) {
  return Math.max(min, Math.min(max, val));
}

function probToOdds(prob, margin = 1.06) {
  if (!prob || prob <= 0) return null;

  const adjusted = clamp(prob * margin, 1, 95);
  const odds = 1 / (adjusted / 100);

  return Number(odds.toFixed(2));
}

// Build markets from your scraped probabilities
function buildMarkets(fixture) {
  const markets = [];

  // 1X2
  if (fixture.prob_home)
    markets.push({
      market: '1X2',
      pick: '1',
      prob: fixture.prob_home,
      odds: probToOdds(fixture.prob_home)
    });

  if (fixture.prob_draw)
    markets.push({
      market: '1X2',
      pick: 'X',
      prob: fixture.prob_draw,
      odds: probToOdds(fixture.prob_draw)
    });

  if (fixture.prob_away)
    markets.push({
      market: '1X2',
      pick: '2',
      prob: fixture.prob_away,
      odds: probToOdds(fixture.prob_away)
    });

  // Over/Under 2.5
  if (fixture.prob_over_25)
    markets.push({
      market: 'goals',
      pick: 'over_2_5',
      prob: fixture.prob_over_25,
      odds: probToOdds(fixture.prob_over_25)
    });

  if (fixture.prob_under_25)
    markets.push({
      market: 'goals',
      pick: 'under_2_5',
      prob: fixture.prob_under_25,
      odds: probToOdds(fixture.prob_under_25)
    });

  // Double chance (derived)
  if (fixture.prob_home && fixture.prob_draw) {
    const prob = fixture.prob_home + fixture.prob_draw;
    markets.push({
      market: 'double_chance',
      pick: '1X',
      prob,
      odds: probToOdds(prob)
    });
  }

  if (fixture.prob_draw && fixture.prob_away) {
    const prob = fixture.prob_draw + fixture.prob_away;
    markets.push({
      market: 'double_chance',
      pick: 'X2',
      prob,
      odds: probToOdds(prob)
    });
  }

  if (fixture.prob_home && fixture.prob_away) {
    const prob = fixture.prob_home + fixture.prob_away;
    markets.push({
      market: 'double_chance',
      pick: '12',
      prob,
      odds: probToOdds(prob)
    });
  }

  return markets;
}

module.exports = {
  probToOdds,
  buildMarkets
};
