"""
PostgreSQL Data Loader

Implements bulk database operations for EFO terms, synonyms, relationships, and MeSH cross-references.
Uses efficient batch inserts with conflict resolution for idempotency.
"""

import logging
import psycopg2
from datetime import datetime
from typing import List, Tuple, Dict, Optional


logger = logging.getLogger('efo_pipeline.loader')


def connect(connection_params: dict):
    """
    Create and return a PostgreSQL database connection.
    
    Args:
        connection_params (dict): Connection parameters (host, port, dbname, user, password)
    
    Returns:
        psycopg2.connection: Database connection object
    
    Raises:
        psycopg2.Error: If connection fails
    """
    try:
        conn = psycopg2.connect(**connection_params)
        logger.info(f"Connected to database: {connection_params['dbname']}@{connection_params['host']}")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Database connection failed: {e}")
        raise


def close(connection):
    """
    Close database connection safely.
    
    Args:
        connection: psycopg2 connection object
    """
    if connection:
        try:
            connection.close()
            logger.info("Database connection closed")
        except Exception as e:
            logger.warning(f"Error closing connection: {e}")


def bulk_insert_terms(connection, terms_batch: List[Dict]) -> Tuple[int, int]:
    """
    Bulk insert EFO terms using upsert pattern for idempotency.
    
    Args:
        connection: Database connection
        terms_batch (List[Dict]): List of term dictionaries with keys: term_id, iri, label, description, content_hash
    
    Returns:
        Tuple[int, int]: (inserted_count, updated_count)
    """
    if not terms_batch:
        return (0, 0)
    
    cursor = connection.cursor()
    inserted = 0
    updated = 0
    
    try:
        # Prepare data tuples
        data = [
            (
                term['term_id'],
                term['iri'],
                term['label'],
                term.get('description'),
                term.get('content_hash')
            )
            for term in terms_batch
        ]
        
        # Upsert query with conflict resolution
        query = """
            INSERT INTO efo_terms (term_id, iri, label, description, content_hash)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (term_id) 
            DO UPDATE SET
                iri = EXCLUDED.iri,
                label = EXCLUDED.label,
                description = EXCLUDED.description,
                content_hash = EXCLUDED.content_hash,
                updated_at = CURRENT_TIMESTAMP
        """
        
        # Execute batch insert
        cursor.executemany(query, data)
        
        # Count affected rows
        inserted = cursor.rowcount
        # Note: We can't distinguish inserts from updates with executemany, so we count them all as inserted
        
        connection.commit()
        logger.info(f"Terms batch: {inserted} inserted, {updated} updated")
        
        return (inserted, updated)
        
    except psycopg2.Error as e:
        connection.rollback()
        logger.error(f"Failed to insert terms batch: {e}")
        raise
    finally:
        cursor.close()


def bulk_insert_synonyms(connection, synonyms_batch: List[Tuple[str, str]]) -> int:
    """
    Bulk insert synonyms with conflict resolution.
    
    Args:
        connection: Database connection
        synonyms_batch (List[Tuple]): List of (term_id_string, synonym) tuples
    
    Returns:
        int: Number of synonyms inserted
    """
    if not synonyms_batch:
        return 0
    
    # Filter out any synonyms with NULL, empty, or whitespace-only term_id (data quality check)
    valid_synonyms = [
        (term_id.strip(), synonym.strip()) 
        for term_id, synonym in synonyms_batch 
        if term_id and synonym and str(term_id).strip() and str(synonym).strip()
    ]
    
    skipped = len(synonyms_batch) - len(valid_synonyms)
    if skipped > 0:
        logger.warning(f"Skipped {skipped} synonyms with invalid term_id or synonym")
    
    if not valid_synonyms:
        return 0
    
    cursor = connection.cursor()
    
    try:
        # Upsert query using SELECT to get internal ID from term_id string
        # Only inserts if the term exists (skips if term not found)
        query = """
            INSERT INTO efo_synonyms (term_id, synonym)
            SELECT id, %s::text
            FROM efo_terms
            WHERE term_id = %s::text
            ON CONFLICT (term_id, synonym) DO NOTHING
        """
        
        # Reorder tuples: (term_id, synonym) -> (synonym, term_id) for query
        reordered_synonyms = [(synonym, term_id) for term_id, synonym in valid_synonyms]
        
        cursor.executemany(query, reordered_synonyms)
        inserted = cursor.rowcount
        connection.commit()
        
        logger.info(f"Synonyms batch: {inserted} inserted")
        return inserted
        
    except psycopg2.Error as e:
        connection.rollback()
        logger.error(f"Failed to insert synonyms batch: {e}")
        raise
    finally:
        cursor.close()


def bulk_insert_relationships(connection, relationships_batch: List[Tuple[int, int]]) -> int:
    """
    Bulk insert parent-child relationships.
    
    Args:
        connection: Database connection
        relationships_batch (List[Tuple]): List of (child_id, parent_id) tuples
    
    Returns:
        int: Number of relationships inserted
    """
    if not relationships_batch:
        return 0
    
    cursor = connection.cursor()
    
    try:
        # Upsert query - ignore duplicates
        query = """
            INSERT INTO efo_relationships (child_id, parent_id, relationship_type)
            VALUES (%s, %s, 'is_a')
            ON CONFLICT (child_id, parent_id) DO NOTHING
        """
        
        cursor.executemany(query, relationships_batch)
        inserted = cursor.rowcount
        connection.commit()
        
        logger.info(f"Relationships batch: {inserted} inserted")
        return inserted
        
    except psycopg2.Error as e:
        connection.rollback()
        logger.error(f"Failed to insert relationships batch: {e}")
        raise
    finally:
        cursor.close()


def bulk_insert_mesh_xrefs(connection, mesh_batch: List[Tuple[str, str, str]]) -> int:
    """
    Bulk insert MeSH cross-references.
    
    Args:
        connection: Database connection
        mesh_batch (List[Tuple]): List of (term_id_string, mesh_id, database) tuples
    
    Returns:
        int: Number of cross-references inserted
    """
    if not mesh_batch:
        return 0
    
    # Filter out any xrefs with NULL, empty, or whitespace-only values (data quality check)
    valid_xrefs = [
        (term_id.strip(), mesh_id.strip(), database.strip()) 
        for term_id, mesh_id, database in mesh_batch 
        if term_id and mesh_id and database and str(term_id).strip() and str(mesh_id).strip()
    ]
    
    skipped = len(mesh_batch) - len(valid_xrefs)
    if skipped > 0:
        logger.warning(f"Skipped {skipped} MeSH xrefs with invalid values")
    
    if not valid_xrefs:
        return 0
    
    cursor = connection.cursor()
    
    try:
        # Upsert query using SELECT to get internal ID from term_id string
        # Only inserts if the term exists (skips if term not found)
        query = """
            INSERT INTO mesh_cross_references (term_id, mesh_id, database)
            SELECT id, %s::text, %s::text
            FROM efo_terms
            WHERE term_id = %s::text
            ON CONFLICT (term_id, mesh_id) DO NOTHING
        """
        
        # Reorder tuples: (term_id, mesh_id, database) -> (mesh_id, database, term_id) for query
        reordered_xrefs = [(mesh_id, database, term_id) for term_id, mesh_id, database in valid_xrefs]
        
        cursor.executemany(query, reordered_xrefs)
        inserted = cursor.rowcount
        connection.commit()
        
        logger.info(f"MeSH cross-references batch: {inserted} inserted")
        return inserted
        
    except psycopg2.Error as e:
        connection.rollback()
        logger.error(f"Failed to insert MeSH cross-references batch: {e}")
        raise
    finally:
        cursor.close()


def create_execution_record(connection, execution_mode: str) -> int:
    """
    Create a new pipeline execution record and return its ID.
    
    Args:
        connection: Database connection
        execution_mode (str): 'full', 'incremental', or 'test'
    
    Returns:
        int: execution_id for this run
    """
    cursor = connection.cursor()
    
    try:
        query = """
            INSERT INTO pipeline_executions (started_at, execution_mode, status)
            VALUES (CURRENT_TIMESTAMP, %s, 'running')
            RETURNING execution_id
        """
        
        cursor.execute(query, (execution_mode,))
        execution_id = cursor.fetchone()[0]
        connection.commit()
        
        logger.info(f"Created execution record: ID={execution_id}, mode={execution_mode}")
        return execution_id
        
    except psycopg2.Error as e:
        connection.rollback()
        logger.error(f"Failed to create execution record: {e}")
        raise
    finally:
        cursor.close()


def update_execution_record(
    connection,
    execution_id: int,
    status: str,
    stats: Optional[Dict] = None,
    error_message: Optional[str] = None
):
    """
    Update pipeline execution record with completion status and statistics.
    
    Args:
        connection: Database connection
        execution_id (int): ID of the execution record
        status (str): 'success' or 'failed'
        stats (Dict, optional): Statistics dict with keys: terms_fetched, terms_inserted, terms_updated, terms_skipped
        error_message (str, optional): Error message if status='failed'
    """
    cursor = connection.cursor()
    
    try:
        if stats is None:
            stats = {}
        
        query = """
            UPDATE pipeline_executions
            SET completed_at = CURRENT_TIMESTAMP,
                status = %s,
                terms_fetched = %s,
                terms_inserted = %s,
                terms_updated = %s,
                terms_skipped = %s,
                error_message = %s
            WHERE execution_id = %s
        """
        
        cursor.execute(query, (
            status,
            stats.get('terms_fetched', 0),
            stats.get('terms_inserted', 0),
            stats.get('terms_updated', 0),
            stats.get('terms_skipped', 0),
            error_message,
            execution_id
        ))
        
        connection.commit()
        logger.info(f"Updated execution record {execution_id}: status={status}")
        
    except psycopg2.Error as e:
        connection.rollback()
        logger.error(f"Failed to update execution record: {e}")
        raise
    finally:
        cursor.close()


def get_last_successful_execution(connection, execution_mode: str) -> Optional[datetime]:
    """
    Get the timestamp of the last successful execution for the given mode.
    
    Used for incremental updates to determine which terms need refreshing.
    
    Args:
        connection: Database connection
        execution_mode (str): Execution mode to query
    
    Returns:
        datetime or None: Timestamp of last successful run, or None if no successful runs
    """
    cursor = connection.cursor()
    
    try:
        query = """
            SELECT MAX(completed_at)
            FROM pipeline_executions
            WHERE status = 'success'
              AND execution_mode = %s
        """
        
        cursor.execute(query, (execution_mode,))
        result = cursor.fetchone()
        
        if result and result[0]:
            logger.info(f"Last successful {execution_mode} execution: {result[0]}")
            return result[0]
        else:
            logger.info(f"No previous successful {execution_mode} executions found")
            return None
            
    except psycopg2.Error as e:
        logger.error(f"Failed to query execution history: {e}")
        raise
    finally:
        cursor.close()


def get_stored_term_hashes(connection) -> Dict[str, str]:
    """
    Retrieve all stored term IDs and their content hashes for incremental updates.
    
    Args:
        connection: Database connection
    
    Returns:
        Dict[str, str]: Mapping of term_id -> content_hash
    """
    cursor = connection.cursor()
    
    try:
        query = """
            SELECT term_id, content_hash
            FROM efo_terms
            WHERE content_hash IS NOT NULL
        """
        
        cursor.execute(query)
        hashes = {term_id: content_hash for term_id, content_hash in cursor.fetchall()}
        
        logger.info(f"Retrieved {len(hashes)} stored term hashes")
        return hashes
        
    except psycopg2.Error as e:
        logger.error(f"Failed to retrieve term hashes: {e}")
        raise
    finally:
        cursor.close()


def get_iri_to_id_map(connection) -> Dict[str, int]:
    """
    Get mapping of term IRI to internal database ID.
    
    Used for resolving parent references when creating relationships.
    
    Args:
        connection: Database connection
    
    Returns:
        Dict[str, int]: Mapping of iri -> id
    """
    cursor = connection.cursor()
    
    try:
        query = "SELECT iri, id FROM efo_terms"
        cursor.execute(query)
        
        iri_map = {iri: term_id for iri, term_id in cursor.fetchall()}
        logger.info(f"Retrieved IRI-to-ID mapping: {len(iri_map)} terms")
        
        return iri_map
        
    except psycopg2.Error as e:
        logger.error(f"Failed to retrieve IRI-to-ID mapping: {e}")
        raise
    finally:
        cursor.close()

