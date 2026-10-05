-- Vector Store Initialization
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Table for content embeddings and metadata
CREATE TABLE IF NOT EXISTS content_embeddings (
    chunk_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    content_id TEXT NOT NULL,
    title TEXT NOT NULL,
    chunk_type TEXT NOT NULL DEFAULT 'metadata', -- 'metadata', 'synopsis', 'subtitle'
    chunk_index INT NOT NULL DEFAULT 0,
    chunk_text TEXT NOT NULL,
    language VARCHAR(10) NOT NULL DEFAULT 'en',
    season INT,
    episode INT,
    start_seconds FLOAT,
    end_seconds FLOAT,
    genres TEXT[],
    embedding vector(384), -- default dimension for all-MiniLM-L6-v2 / local models
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for vector similarity search (cosine distance)
CREATE INDEX IF NOT EXISTS idx_content_embeddings_vector 
ON content_embeddings 
USING ivfflat (embedding vector_cosine_ops) 
WITH (lists = 100);

-- Content ID index for metadata retrieval
CREATE INDEX IF NOT EXISTS idx_content_embeddings_content_id 
ON content_embeddings(content_id);
