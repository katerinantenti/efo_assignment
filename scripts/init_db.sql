-- ============================================================================
-- EFO Data Pipeline - Database Schema
-- ============================================================================
-- Purpose: PostgreSQL schema for storing EFO terms, synonyms, relationships,
--          MeSH cross-references, and pipeline execution metadata
-- Normalization: 3NF (Third Normal Form)
-- Version: 1.0.0
-- Date: 2025-10-11
-- ============================================================================

-- ============================================================================
-- 1. MAIN ENTITY TABLE: EFO Terms
-- ============================================================================

-- Stores core EFO (Experimental Factor Ontology) term information
CREATE TABLE IF NOT EXISTS efo_terms (
    -- Primary key: Internal auto-incrementing ID
    id SERIAL PRIMARY KEY,
    
    -- Business keys: EFO identifier and IRI (both must be unique)
    term_id VARCHAR(50) NOT NULL UNIQUE,
    iri TEXT NOT NULL UNIQUE,
    
    -- Core term attributes
    label TEXT NOT NULL,
    description TEXT,
    
    -- Change detection support for incremental updates
    content_hash CHAR(64),
    
    -- Audit timestamps
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance optimization
CREATE INDEX IF NOT EXISTS idx_efo_terms_term_id ON efo_terms(term_id);
CREATE INDEX IF NOT EXISTS idx_efo_terms_iri ON efo_terms(iri);
CREATE INDEX IF NOT EXISTS idx_efo_terms_content_hash ON efo_terms(content_hash);
CREATE INDEX IF NOT EXISTS idx_efo_terms_updated_at ON efo_terms(updated_at DESC);

-- Table comment
COMMENT ON TABLE efo_terms IS 'Primary table storing EFO ontology terms with their core attributes';
COMMENT ON COLUMN efo_terms.term_id IS 'EFO identifier in format EFO:0000001';
COMMENT ON COLUMN efo_terms.iri IS 'Full IRI URL for the term';
COMMENT ON COLUMN efo_terms.content_hash IS 'SHA-256 hash of term content for change detection';

-- ============================================================================
-- 2. SYNONYMS TABLE: Alternative Term Names
-- ============================================================================

-- Stores alternative names/labels for EFO terms (one-to-many relationship)
CREATE TABLE IF NOT EXISTS efo_synonyms (
    -- Primary key
    id SERIAL PRIMARY KEY,
    
    -- Foreign key to parent term
    term_id INTEGER NOT NULL,
    
    -- Synonym information
    synonym TEXT NOT NULL,
    synonym_type VARCHAR(50),
    
    -- Foreign key constraint
    CONSTRAINT fk_efo_synonyms_term
        FOREIGN KEY (term_id)
        REFERENCES efo_terms(id)
        ON DELETE CASCADE
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_efo_synonyms_term_id ON efo_synonyms(term_id);
CREATE INDEX IF NOT EXISTS idx_efo_synonyms_composite ON efo_synonyms(term_id, synonym);

-- Prevent duplicate synonyms for the same term
CREATE UNIQUE INDEX IF NOT EXISTS idx_efo_synonyms_unique 
    ON efo_synonyms(term_id, synonym);

-- Table comment
COMMENT ON TABLE efo_synonyms IS 'Stores alternative names for EFO terms, maintaining 1:N relationship';
COMMENT ON COLUMN efo_synonyms.synonym_type IS 'Classification of synonym (exact, broad, narrow, related)';

-- ============================================================================
-- 3. RELATIONSHIPS TABLE: Ontology Hierarchy
-- ============================================================================

-- Stores parent-child relationships between EFO terms (many-to-many)
CREATE TABLE IF NOT EXISTS efo_relationships (
    -- Primary key
    id SERIAL PRIMARY KEY,
    
    -- Foreign keys to child and parent terms
    child_id INTEGER NOT NULL,
    parent_id INTEGER NOT NULL,
    
    -- Relationship classification
    relationship_type VARCHAR(50) DEFAULT 'is_a',
    
    -- Foreign key constraints
    CONSTRAINT fk_efo_relationships_child
        FOREIGN KEY (child_id)
        REFERENCES efo_terms(id)
        ON DELETE CASCADE,
    
    CONSTRAINT fk_efo_relationships_parent
        FOREIGN KEY (parent_id)
        REFERENCES efo_terms(id)
        ON DELETE CASCADE,
    
    -- Prevent duplicate relationships
    CONSTRAINT uq_efo_relationships_child_parent
        UNIQUE (child_id, parent_id)
);

-- Indexes for bidirectional hierarchy queries
CREATE INDEX IF NOT EXISTS idx_efo_relationships_child ON efo_relationships(child_id);
CREATE INDEX IF NOT EXISTS idx_efo_relationships_parent ON efo_relationships(parent_id);

-- Table comment
COMMENT ON TABLE efo_relationships IS 'Junction table representing parent-child relationships in EFO ontology hierarchy';
COMMENT ON COLUMN efo_relationships.relationship_type IS 'Type of relationship (typically "is_a" for class hierarchy)';

-- ============================================================================
-- 4. MESH CROSS-REFERENCES TABLE: External Database Links
-- ============================================================================

-- Stores cross-references to MeSH (Medical Subject Headings) terms
CREATE TABLE IF NOT EXISTS mesh_cross_references (
    -- Primary key
    id SERIAL PRIMARY KEY,
    
    -- Foreign key to EFO term
    term_id INTEGER NOT NULL,
    
    -- MeSH reference information
    mesh_id VARCHAR(50) NOT NULL,
    database VARCHAR(20) DEFAULT 'MSH',
    
    -- Foreign key constraint
    CONSTRAINT fk_mesh_xrefs_term
        FOREIGN KEY (term_id)
        REFERENCES efo_terms(id)
        ON DELETE CASCADE,
    
    -- Prevent duplicate cross-references
    CONSTRAINT uq_mesh_xrefs_term_mesh
        UNIQUE (term_id, mesh_id)
);

-- Indexes for lookups
CREATE INDEX IF NOT EXISTS idx_mesh_xrefs_term_id ON mesh_cross_references(term_id);
CREATE INDEX IF NOT EXISTS idx_mesh_xrefs_mesh_id ON mesh_cross_references(mesh_id);

-- Table comment
COMMENT ON TABLE mesh_cross_references IS 'Stores MeSH term cross-references for EFO terms';
COMMENT ON COLUMN mesh_cross_references.mesh_id IS 'MeSH identifier (e.g., D005060)';
COMMENT ON COLUMN mesh_cross_references.database IS 'Database source identifier (typically "MSH")';

-- ============================================================================
-- 5. PIPELINE EXECUTION METADATA TABLE
-- ============================================================================

-- Tracks pipeline execution history for monitoring and incremental updates
CREATE TABLE IF NOT EXISTS pipeline_executions (
    -- Primary key
    execution_id SERIAL PRIMARY KEY,
    
    -- Execution timestamps
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    
    -- Execution classification
    execution_mode VARCHAR(20) NOT NULL,
    status VARCHAR(20) NOT NULL,
    
    -- Statistics
    terms_fetched INTEGER DEFAULT 0,
    terms_inserted INTEGER DEFAULT 0,
    terms_updated INTEGER DEFAULT 0,
    terms_skipped INTEGER DEFAULT 0,
    
    -- Error tracking
    error_message TEXT,
    
    -- Constraints
    CONSTRAINT chk_pipeline_status 
        CHECK (status IN ('running', 'success', 'failed')),
    
    CONSTRAINT chk_pipeline_mode
        CHECK (execution_mode IN ('full', 'incremental', 'test'))
);

-- Indexes for querying execution history
CREATE INDEX IF NOT EXISTS idx_pipeline_executions_started ON pipeline_executions(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_executions_status_mode ON pipeline_executions(status, execution_mode);

-- Table comment
COMMENT ON TABLE pipeline_executions IS 'Tracks pipeline execution history and statistics';
COMMENT ON COLUMN pipeline_executions.execution_mode IS 'Execution mode: full, incremental, or test';
COMMENT ON COLUMN pipeline_executions.status IS 'Execution status: running, success, or failed';

-- ============================================================================
-- HELPER FUNCTIONS AND TRIGGERS (Optional Enhancement)
-- ============================================================================

-- Function to automatically update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to auto-update updated_at on efo_terms modifications
CREATE TRIGGER trigger_update_efo_terms_updated_at
    BEFORE UPDATE ON efo_terms
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- END OF SCHEMA
-- ============================================================================

