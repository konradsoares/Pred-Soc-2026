CREATE TABLE IF NOT EXISTS inplay_scan_runs (
    id BIGSERIAL PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'running',
    total_events INTEGER DEFAULT 0,
    total_markets INTEGER DEFAULT 0,
    total_opportunities INTEGER DEFAULT 0,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS inplay_opportunities (
    id BIGSERIAL PRIMARY KEY,
    scan_run_id BIGINT REFERENCES inplay_scan_runs(id) ON DELETE CASCADE,

    betfair_event_id TEXT NOT NULL,
    betfair_event_name TEXT NOT NULL,

    fixture_id BIGINT,
    home_team TEXT,
    away_team TEXT,

    market_id TEXT NOT NULL,
    market_type TEXT NOT NULL,
    market_status TEXT,

    selection_id BIGINT NOT NULL,
    runner_name TEXT NOT NULL,

    back_odd NUMERIC(10, 4) NOT NULL,
    implied_probability NUMERIC(10, 6) NOT NULL,
    model_probability NUMERIC(10, 6) NOT NULL,
    edge NUMERIC(10, 6) NOT NULL,

    risk_level TEXT NOT NULL,
    reason TEXT,
    stats_summary JSONB,
    raw_snapshot JSONB,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_inplay_opportunities_scan_run
ON inplay_opportunities(scan_run_id);

CREATE INDEX IF NOT EXISTS idx_inplay_opportunities_event
ON inplay_opportunities(betfair_event_id);

CREATE INDEX IF NOT EXISTS idx_inplay_opportunities_market
ON inplay_opportunities(market_id);

CREATE INDEX IF NOT EXISTS idx_inplay_opportunities_created_at
ON inplay_opportunities(created_at);
