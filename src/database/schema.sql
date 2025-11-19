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
    data_type TEXT NOT NULL,
    objects_loaded INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Cards table for storing Scryfall oracle card data
-- This table stores canonical card information from Scryfall's oracle cards bulk data.
-- Each card represents a unique oracle card (not a specific printing), identified by card_id.
-- Cards are loaded via ETLPipeline.insert_cards() which downloads Scryfall bulk data.
-- Fields:
--   card_id: Scryfall UUID for the oracle card (primary key)
--   set: Set code for the card (e.g., 'M21', 'MH2')
--   collector_num: Collector number within the set
--   name: Card name (required, indexed for decklist matching)
--   oracle_text: Rules text of the card
--   rulings: Comma-separated list of official rulings
--   type_line: Card type line (e.g., 'Creature — Human Wizard')
--   mana_cost: Mana cost string (e.g., '{1}{R}')
--   cmc: Converted mana cost (float)
--   color_identity: Array of color identity letters (e.g., ['R', 'U'])
--   scryfall_uri: Link to card on Scryfall
CREATE TABLE IF NOT EXISTS cards (
    card_id TEXT PRIMARY KEY,
    set TEXT,
    collector_num TEXT,
    name TEXT NOT NULL,
    oracle_text TEXT,
    rulings TEXT,
    type_line TEXT,
    mana_cost TEXT,
    cmc FLOAT,
    color_identity TEXT[],
    scryfall_uri TEXT
);

-- Deck cards junction table linking decklists to individual cards
-- This table stores parsed card entries from decklists, linking them to the cards table.
-- Each row represents a card in a specific decklist with its quantity and section.
-- Cards are parsed from decklist_text using ETLPipeline.parse_decklist() and stored via insert_deck_cards().
-- Fields:
--   decklist_id: Foreign key to decklists table
--   card_id: Foreign key to cards table (matched by card name)
--   section: Either 'mainboard' or 'sideboard' (enforced by CHECK constraint)
--   quantity: Number of copies of this card in the section (must be > 0)
-- Note: Cards not found in the cards table are logged but not stored.
CREATE TABLE IF NOT EXISTS deck_cards (
    decklist_id INTEGER NOT NULL,
    card_id TEXT NOT NULL,
    section TEXT NOT NULL CHECK (section IN ('mainboard', 'sideboard')),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    PRIMARY KEY (decklist_id, card_id, section),
    FOREIGN KEY (decklist_id) REFERENCES decklists(decklist_id) ON DELETE CASCADE,
    FOREIGN KEY (card_id) REFERENCES cards(card_id) ON DELETE CASCADE
);

-- Archetypes table for storing LLM-based deck classifications
-- This table stores archetype classifications for decklists generated by LLM analysis.
-- Each row represents a classification attempt, allowing multiple classifications per deck over time.
-- Fields:
--   archetype_id: Unique identifier for each classification (primary key)
--   decklist_id: Foreign key to decklists table (the deck being classified)
--   format: Tournament format (e.g., "Modern", "Standard")
--   main_title: Archetype name based on key cards/themes (e.g., "amulet_titan", "elves")
--   color_identity: Human-readable color description (e.g., "dimir", "jeskai", "colorless")
--   strategy: One of "aggro", "midrange", "control", "ramp", or "combo"
--   archetype_confidence: LLM confidence score from 0 to 1
--   llm_model: Model used for classification (e.g., "gpt-4o-mini", "claude-3-5-sonnet")
--   prompt_id: Version identifier for prompt used (e.g., "archetype_classification_v1")
--   classified_at: Timestamp when classification was performed
CREATE TABLE IF NOT EXISTS archetypes (
    archetype_id SERIAL PRIMARY KEY,
    decklist_id INTEGER NOT NULL,
    format TEXT NOT NULL,
    main_title TEXT NOT NULL,
    color_identity TEXT,
    strategy TEXT NOT NULL CHECK (strategy IN ('aggro', 'midrange', 'control', 'ramp', 'combo')),
    archetype_confidence FLOAT NOT NULL CHECK (archetype_confidence >= 0 AND archetype_confidence <= 1),
    llm_model TEXT NOT NULL,
    prompt_id TEXT NOT NULL,
    classified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (decklist_id) REFERENCES decklists(decklist_id) ON DELETE CASCADE
);

-- Add archetype_id column to decklists (nullable, will be populated later)
DO $$ 
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'decklists' AND column_name = 'archetype_id') THEN
        ALTER TABLE decklists ADD COLUMN archetype_id INTEGER;
    END IF;
END $$;

-- Add foreign key from decklists to archetypes (references latest classification)
DO $$ 
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_decklists_archetype') THEN
        ALTER TABLE decklists ADD CONSTRAINT fk_decklists_archetype 
            FOREIGN KEY (archetype_id) REFERENCES archetypes(archetype_id) ON DELETE SET NULL;
    END IF;
END $$;

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_tournaments_format ON tournaments(format);
CREATE INDEX IF NOT EXISTS idx_tournaments_start_date ON tournaments(start_date);
CREATE INDEX IF NOT EXISTS idx_tournaments_updated_at ON tournaments(updated_at);
CREATE INDEX IF NOT EXISTS idx_players_tournament_id ON players(tournament_id);
CREATE INDEX IF NOT EXISTS idx_players_standing ON players(tournament_id, standing);
CREATE INDEX IF NOT EXISTS idx_decklists_player_tournament ON decklists(player_id, tournament_id);
CREATE INDEX IF NOT EXISTS idx_matches_tournament ON matches(tournament_id);
CREATE INDEX IF NOT EXISTS idx_matches_players ON matches(player1_id, player2_id);

-- Indexes for cards table
CREATE INDEX IF NOT EXISTS idx_cards_name ON cards(name);
CREATE INDEX IF NOT EXISTS idx_cards_color_identity ON cards USING GIN(color_identity);

-- Indexes for deck_cards table
CREATE INDEX IF NOT EXISTS idx_deck_cards_decklist_id ON deck_cards(decklist_id);
CREATE INDEX IF NOT EXISTS idx_deck_cards_card_id ON deck_cards(card_id);

-- Indexes for archetypes table
CREATE INDEX IF NOT EXISTS idx_archetypes_decklist_id ON archetypes(decklist_id);
CREATE INDEX IF NOT EXISTS idx_archetypes_format ON archetypes(format);
CREATE INDEX IF NOT EXISTS idx_decklists_archetype_id ON decklists(archetype_id);

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger to automatically update updated_at
DROP TRIGGER IF EXISTS update_tournaments_updated_at ON tournaments;
CREATE TRIGGER update_tournaments_updated_at BEFORE UPDATE ON tournaments
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

