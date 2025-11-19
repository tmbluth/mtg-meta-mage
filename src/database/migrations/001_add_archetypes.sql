-- Migration: Add archetypes table and archetype_id to decklists
-- Purpose: Enable LLM-based archetype classification for decklists

BEGIN;

-- Step 1: Add archetype_id column to decklists (nullable, will be populated later)
ALTER TABLE decklists ADD COLUMN IF NOT EXISTS archetype_id INTEGER;

-- Step 2: Create archetypes table
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

-- Step 3: Add foreign key constraint from decklists to archetypes
-- Drop constraint if it already exists (idempotent)
DO $$ 
BEGIN
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_decklists_archetype') THEN
        ALTER TABLE decklists DROP CONSTRAINT fk_decklists_archetype;
    END IF;
END $$;

ALTER TABLE decklists ADD CONSTRAINT fk_decklists_archetype 
    FOREIGN KEY (archetype_id) REFERENCES archetypes(archetype_id) ON DELETE SET NULL;

-- Step 4: Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_archetypes_decklist_id ON archetypes(decklist_id);
CREATE INDEX IF NOT EXISTS idx_archetypes_format ON archetypes(format);
CREATE INDEX IF NOT EXISTS idx_decklists_archetype_id ON decklists(archetype_id);

COMMIT;

