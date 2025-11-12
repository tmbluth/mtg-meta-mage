-- TopDeck Tournament Database Schema
-- PostgreSQL schema for storing MTG tournament data from TopDeck.gg API

-- Tournaments table
CREATE TABLE IF NOT EXISTS tournaments (
    tournament_id TEXT PRIMARY KEY,
    tournament_name TEXT NOT NULL,
    format TEXT NOT NULL,
    start_date INTEGER NOT NULL,
    swiss_num INTEGER,
    top_cut INTEGER,
    city TEXT,
    state TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Players table
CREATE TABLE IF NOT EXISTS players (
    player_id TEXT NOT NULL,
    tournament_id TEXT NOT NULL,
    name TEXT NOT NULL,
    wins INTEGER DEFAULT 0,
    wins_swiss INTEGER,
    wins_bracket INTEGER,
    win_rate FLOAT,
    losses INTEGER DEFAULT 0,
    losses_swiss INTEGER,
    losses_bracket INTEGER,
    draws INTEGER DEFAULT 0,
    points INTEGER,
    standing INTEGER,
    PRIMARY KEY (player_id, tournament_id),
    FOREIGN KEY (tournament_id) REFERENCES tournaments(tournament_id) ON DELETE CASCADE
);

-- Decklists table
CREATE TABLE IF NOT EXISTS decklists (
    decklist_id SERIAL PRIMARY KEY,
    player_id TEXT NOT NULL,
    tournament_id TEXT NOT NULL,
    decklist_text TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (player_id, tournament_id),
    FOREIGN KEY (player_id, tournament_id) REFERENCES players(player_id, tournament_id) ON DELETE CASCADE
);

-- Match rounds table
CREATE TABLE IF NOT EXISTS match_rounds (
    round_number INTEGER NOT NULL,
    tournament_id TEXT NOT NULL,
    round_description TEXT,
    PRIMARY KEY (round_number, tournament_id),
    FOREIGN KEY (tournament_id) REFERENCES tournaments(tournament_id) ON DELETE CASCADE
);

-- Matches table (1v1 only)
CREATE TABLE IF NOT EXISTS matches (
    round_number INTEGER NOT NULL,
    tournament_id TEXT NOT NULL,
    match_num INTEGER NOT NULL,
    player1_id TEXT NOT NULL,
    player2_id TEXT,
    winner_id TEXT,
    status TEXT NOT NULL,
    PRIMARY KEY (round_number, tournament_id, match_num),
    FOREIGN KEY (round_number, tournament_id) REFERENCES match_rounds(round_number, tournament_id) ON DELETE CASCADE,
    FOREIGN KEY (player1_id, tournament_id) REFERENCES players(player_id, tournament_id) ON DELETE CASCADE,
    FOREIGN KEY (player2_id, tournament_id) REFERENCES players(player_id, tournament_id) ON DELETE SET NULL
);

-- Load metadata table for tracking incremental loads
CREATE TABLE IF NOT EXISTS load_metadata (
    id SERIAL PRIMARY KEY,
    last_load_timestamp INTEGER NOT NULL,
    last_load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    load_type TEXT NOT NULL DEFAULT 'incremental',
    tournaments_loaded INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_tournaments_format ON tournaments(format);
CREATE INDEX IF NOT EXISTS idx_tournaments_start_date ON tournaments(start_date);
CREATE INDEX IF NOT EXISTS idx_tournaments_updated_at ON tournaments(updated_at);
CREATE INDEX IF NOT EXISTS idx_players_tournament_id ON players(tournament_id);
CREATE INDEX IF NOT EXISTS idx_players_standing ON players(tournament_id, standing);
CREATE INDEX IF NOT EXISTS idx_decklists_player_tournament ON decklists(player_id, tournament_id);
CREATE INDEX IF NOT EXISTS idx_matches_tournament ON matches(tournament_id);
CREATE INDEX IF NOT EXISTS idx_matches_players ON matches(player1_id, player2_id);

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger to automatically update updated_at
CREATE TRIGGER update_tournaments_updated_at BEFORE UPDATE ON tournaments
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

