const MIN_BACK_ODD = 1.20;
const MIN_EDGE = 0.01;

function getBestBackOdd(runner) {
  const availableToBack =
    runner &&
    runner.ex &&
    Array.isArray(runner.ex.availableToBack)
      ? runner.ex.availableToBack
      : [];

  if (!availableToBack.length) {
    return null;
  }

  return Number(availableToBack[0].price);
}

function isMarketUsable(marketBook) {
  if (!marketBook) {
    return false;
  }

  if (marketBook.status !== 'OPEN') {
    return false;
  }

  if (marketBook.inplay !== true) {
    return false;
  }

  if (!Array.isArray(marketBook.runners) || marketBook.runners.length === 0) {
    return false;
  }

  return true;
}

function isRunnerUsable(runner) {
  if (!runner) {
    return false;
  }

  if (runner.status !== 'ACTIVE') {
    return false;
  }

  const odd = getBestBackOdd(runner);

  if (!odd || odd < MIN_BACK_ODD) {
    return false;
  }

  return true;
}

function isPositiveValueOpportunity(payload) {
  const {
    backOdd,
    impliedProbability,
    modelProbability,
    riskLevel,
    mappingConfidence,
  } = payload;

  if (!backOdd || backOdd < MIN_BACK_ODD) {
    return false;
  }

  if (!modelProbability || !impliedProbability) {
    return false;
  }

  if (modelProbability <= impliedProbability) {
    return false;
  }

  if (modelProbability - impliedProbability < MIN_EDGE) {
    return false;
  }

  if (mappingConfidence !== 'high') {
    return false;
  }

  if (riskLevel === 'reject') {
    return false;
  }

  return true;
}

module.exports = {
  MIN_BACK_ODD,
  MIN_EDGE,
  getBestBackOdd,
  isMarketUsable,
  isRunnerUsable,
  isPositiveValueOpportunity,
};
