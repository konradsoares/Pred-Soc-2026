CREATE TABLE IF NOT EXISTS countries (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    code TEXT,
    flag_url TEXT
);

CREATE TABLE IF NOT EXISTS competitions (
    id BIGSERIAL PRIMARY KEY,
    source_name TEXT NOT NULL DEFAULT 'scraper',
    external_id TEXT,
    country_id INT REFERENCES countries(id) ON DELETE SET NULL,
    name TEXT NOT NULL,
    type TEXT NOT NULL, -- league | cup | tournament | friendly
    UNIQUE (source_name, external_id)
);

CREATE TABLE IF NOT EXISTS seasons (
    id SERIAL PRIMARY KEY,
    competition_id BIGINT NOT NULL REFERENCES competitions(id) ON DELETE CASCADE,
    season_label TEXT NOT NULL, -- e.g. 2025/2026
    start_date DATE,
    end_date DATE,
    UNIQUE (competition_id, season_label)
);

CREATE TABLE IF NOT EXISTS teams (
    id BIGSERIAL PRIMARY KEY,
    source_name TEXT NOT NULL DEFAULT 'scraper',
    external_id TEXT,
    country_id INT REFERENCES countries(id) ON DELETE SET NULL,
    name TEXT NOT NULL,
    short_name TEXT,
    UNIQUE (source_name, external_id)
);

CREATE TABLE IF NOT EXISTS fixtures (
    id BIGSERIAL PRIMARY KEY,
    source_name TEXT NOT NULL DEFAULT 'scraper',
    external_id TEXT,
    competition_id BIGINT REFERENCES competitions(id) ON DELETE SET NULL,
    season_id INT REFERENCES seasons(id) ON DELETE SET NULL,
    country_id INT REFERENCES countries(id) ON DELETE SET NULL,
    home_team_id BIGINT NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    away_team_id BIGINT NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    kickoff_utc TIMESTAMPTZ NOT NULL,
    venue TEXT,
    round TEXT,
    status TEXT,
    is_friendly BOOLEAN NOT NULL DEFAULT FALSE,
    scraped_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_name, external_id)
);

CREATE TABLE IF NOT EXISTS fixture_scores (
    fixture_id BIGINT PRIMARY KEY REFERENCES fixtures(id) ON DELETE CASCADE,
    home_goals INT,
    away_goals INT,
    halftime_home INT,
    halftime_away INT,
    fulltime_home INT,
    fulltime_away INT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS team_recent_stats (
    id BIGSERIAL PRIMARY KEY,
    fixture_id BIGINT NOT NULL REFERENCES fixtures(id) ON DELETE CASCADE,
    team_id BIGINT NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    matches_considered INT NOT NULL DEFAULT 5,
    wins INT,
    draws INT,
    losses INT,
    goals_for INT,
    goals_against INT,
    clean_sheets INT,
    failed_to_score INT,
    btts INT,
    over_25 INT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (fixture_id, team_id, matches_considered)
);

CREATE TABLE IF NOT EXISTS head_to_head_stats (
    id BIGSERIAL PRIMARY KEY,
    fixture_id BIGINT NOT NULL REFERENCES fixtures(id) ON DELETE CASCADE,
    home_team_id BIGINT NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    away_team_id BIGINT NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    matches_considered INT NOT NULL DEFAULT 5,
    home_wins INT,
    draws INT,
    away_wins INT,
    home_goals INT,
    away_goals INT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (fixture_id, home_team_id, away_team_id, matches_considered)
);

CREATE TABLE IF NOT EXISTS scraped_predictions (
    id BIGSERIAL PRIMARY KEY,
    fixture_id BIGINT NOT NULL REFERENCES fixtures(id) ON DELETE CASCADE,
    source_name TEXT NOT NULL,
    tip TEXT,
    prob_home NUMERIC(8,4),
    prob_draw NUMERIC(8,4),
    prob_away NUMERIC(8,4),
    prob_over_25 NUMERIC(8,4),
    prob_under_25 NUMERIC(8,4),
    raw_payload JSONB,
    scraped_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS model_predictions (
    id BIGSERIAL PRIMARY KEY,
    fixture_id BIGINT NOT NULL REFERENCES fixtures(id) ON DELETE CASCADE,
    model_name TEXT NOT NULL,
    home_win_prob NUMERIC(8,4),
    draw_prob NUMERIC(8,4),
    away_win_prob NUMERIC(8,4),
    over_25_prob NUMERIC(8,4),
    btts_prob NUMERIC(8,4),
    predicted_score TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fixtures_kickoff_utc ON fixtures(kickoff_utc);
CREATE INDEX IF NOT EXISTS idx_fixtures_competition_id ON fixtures(competition_id);
CREATE INDEX IF NOT EXISTS idx_fixtures_home_team_id ON fixtures(home_team_id);
CREATE INDEX IF NOT EXISTS idx_fixtures_away_team_id ON fixtures(away_team_id);
CREATE INDEX IF NOT EXISTS idx_teams_name ON teams(name);
CREATE INDEX IF NOT EXISTS idx_competitions_name ON competitions(name);