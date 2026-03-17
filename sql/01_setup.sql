-- ============================================================
-- 01_setup.sql
-- WhoScored Serie A — Database setup
-- Eseguito automaticamente da PostgreSQL al primo avvio
-- del container Docker
-- ============================================================

-- Tabella eventi con campi fissi e qualifier in JSONB
CREATE TABLE IF NOT EXISTS eventi (
    id          SERIAL PRIMARY KEY,
    match_id    INT NOT NULL,
    match_date  DATE,
    player_id   FLOAT,
    player_name VARCHAR(100),
    event_type  VARCHAR(50),
    event_value INT,
    outcome     VARCHAR(50),
    minuto      INT,
    secondo     FLOAT,
    team_id     INT,
    team_name   VARCHAR(100),
    start_x     FLOAT,
    start_y     FLOAT,
    end_x       FLOAT,
    end_y       FLOAT,
    qualifiers  JSONB
);

-- Indice su match_id per velocizzare le query per partita
CREATE INDEX IF NOT EXISTS idx_eventi_match_id
    ON eventi(match_id);

-- Indice su event_type per velocizzare i filtri per tipo evento
CREATE INDEX IF NOT EXISTS idx_eventi_event_type
    ON eventi(event_type);

-- Indice JSONB per velocizzare le query sui qualifier
CREATE INDEX IF NOT EXISTS idx_eventi_qualifiers
    ON eventi USING GIN(qualifiers);