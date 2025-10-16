"""
EFO Data Pipeline - Main Orchestration

Coordinates the ETL (Extract-Transform-Load) process for retrieving EFO terms
from the OLS API and storing them in PostgreSQL.

Execution modes:
- test: Limited records for development/testing
- full: Complete dataset retrieval
- incremental: Only changed terms since last successful run
"""

import logging
import sys
import argparse
from datetime import datetime

# Import configuration
from src.config import get_config, setup_logging

# Import extractor (API client)
from src.extractors import ols_client

# Import transformer (data normalization)
from src.transformers import efo_transformer

# Import loader (database operations)
from src.loaders import postgres_loader


def parse_arguments():
    """
    Parse command-line arguments.
    
    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description='EFO Data Pipeline - Retrieve EFO terms from OLS API and store in PostgreSQL'
    )
    
    parser.add_argument(
        '--mode',
        choices=['test', 'full', 'incremental'],
        help='Execution mode (overrides EXECUTION_MODE env var)'
    )
    
    parser.add_argument(
        '--limit',
        type=int,
        help='Record limit for test mode (overrides RECORD_LIMIT env var)'
    )
    
    return parser.parse_args()


def main():
    """
    Main pipeline execution function.
    
    Orchestrates the complete ETL process:
    1. Load configuration
    2. Connect to database
    3. Extract data from OLS API
    4. Transform and validate data
    5. Load data into database (bulk operations)
    6. Track execution metadata
    """
    # Parse command-line arguments
    args = parse_arguments()
    
    # Load configuration
    try:
        config = get_config()
        
        # Override config with command-line arguments
        if args.mode:
            config.execution_mode = args.mode
        if args.limit is not None:
            config.record_limit = args.limit
        
    except Exception as e:
        print(f"Configuration error: {e}")
        sys.exit(1)
    
    # Setup logging
    logger = setup_logging(config.log_level)
    
    # Log startup
    logger.info("="*80)
    logger.info("EFO Data Pipeline v1.0.0")
    logger.info("="*80)
    logger.info(f"Execution mode: {config.execution_mode}")
    logger.info(f"Record limit: {config.record_limit if config.record_limit > 0 else 'unlimited'}")
    logger.info(f"Batch size: {config.batch_size}")
    logger.info(f"Database: {config.db_name}@{config.db_host}:{config.db_port}")
    logger.info(f"OLS API: {config.ols_base_url}")
    logger.info("="*80)
    
    # Connect to database
    connection = None
    execution_id = None
    
    try:
        # Establish database connection
        logger.info("Connecting to database...")
        connection = postgres_loader.connect(config.get_db_connection_params())
        
        # Create execution record
        execution_id = postgres_loader.create_execution_record(
            connection,
            config.execution_mode
        )
        logger.info(f"Execution ID: {execution_id}")
        
        # Statistics tracking
        stats = {
            'terms_fetched': 0,
            'terms_inserted': 0,
            'terms_updated': 0,
            'terms_skipped': 0
        }
        
        # Handle incremental mode: load existing term hashes
        stored_hashes = {}
        if config.execution_mode == 'incremental':
            logger.info("Loading stored term hashes for incremental update...")
            stored_hashes = postgres_loader.get_stored_term_hashes(connection)
            logger.info(f"Loaded {len(stored_hashes)} existing term hashes")
        
        # Phase 1: Extract and process terms (with synonyms)
        logger.info("="*80)
        logger.info("PHASE 1: Extracting and loading terms with synonyms")
        logger.info("="*80)
        
        terms_batch = []
        synonyms_batch = []
        parent_refs_map = {}  # Map term_id -> parent_iris for Phase 2
        parent_urls_map = {}  # Map term_iri -> parent_url for batch fetching
        mesh_xrefs_map = {}   # Map term_id -> list of MeSH IDs
        
        # Determine record limit based on mode
        limit = config.record_limit if config.execution_mode == 'test' else None
        
        # Fetch terms from OLS API
        logger.info("Fetching terms from OLS API...")
        for term_json in ols_client.fetch_all_terms(
            config.ols_base_url,
            config.ols_request_delay,
            limit
        ):
            stats['terms_fetched'] += 1
            
            # Extract data from JSON
            term_data = ols_client.extract_term_data(term_json)
            synonyms_list = ols_client.extract_synonyms(term_json)
            parent_urls = ols_client.extract_parent_iris(term_json)  # Get URLs (not IRIs yet)
            mesh_ids = ols_client.extract_mesh_xrefs(term_json)
            
            # Normalize term
            normalized_term = efo_transformer.normalize_term(term_data)
            if not normalized_term:
                logger.warning(f"Skipping invalid term: {term_data.get('term_id')}")
                stats['terms_skipped'] += 1
                continue
            
            # Add content hash for change detection (temporarily use empty parent list)
            efo_transformer.add_content_hash_to_term(
                normalized_term,
                synonyms_list,
                []  # We'll fetch parents in batch later
            )
            
            # Incremental mode: skip unchanged terms
            if config.execution_mode == 'incremental':
                term_id = normalized_term['term_id']
                current_hash = normalized_term['content_hash']
                stored_hash = stored_hashes.get(term_id)
                
                if stored_hash == current_hash:
                    stats['terms_skipped'] += 1
                    continue  # Skip - no changes
            
            # Add to batch
            terms_batch.append(normalized_term)
            
            # Store parent URL for batch fetching in Phase 2
            if parent_urls:
                # Store the first parent URL (most terms have one parent link)
                parent_urls_map[normalized_term['iri']] = parent_urls[0] if isinstance(parent_urls, list) else parent_urls
            
            # Store MeSH xrefs for Phase 2
            if mesh_ids:
                mesh_xrefs_map[normalized_term['term_id']] = mesh_ids
            
            # Normalize and add synonyms (only if term has valid term_id)
            if normalized_term.get('term_id') and synonyms_list:
                normalized_synonyms = efo_transformer.normalize_synonyms(
                    normalized_term['term_id'],
                    synonyms_list
                )
                synonyms_batch.extend(normalized_synonyms)
            
            # Insert batches when size threshold reached
            if len(terms_batch) >= config.batch_size:
                inserted, updated = postgres_loader.bulk_insert_terms(connection, terms_batch)
                stats['terms_inserted'] += inserted
                stats['terms_updated'] += updated
                terms_batch.clear()
            
            if len(synonyms_batch) >= config.batch_size:
                postgres_loader.bulk_insert_synonyms(connection, synonyms_batch)
                synonyms_batch.clear()
        
        # Insert remaining terms and synonyms
        if terms_batch:
            inserted, updated = postgres_loader.bulk_insert_terms(connection, terms_batch)
            stats['terms_inserted'] += inserted
            stats['terms_updated'] += updated
            terms_batch.clear()
        
        if synonyms_batch:
            postgres_loader.bulk_insert_synonyms(connection, synonyms_batch)
            synonyms_batch.clear()
        
        logger.info(f"Phase 1 complete: {stats['terms_inserted']} inserted, {stats['terms_updated']} updated, {stats['terms_skipped']} skipped")
        
        # Phase 1.5: Batch fetch all parent relationships (PERFORMANCE OPTIMIZATION)
        if parent_urls_map:
            logger.info("="*80)
            logger.info("PHASE 1.5: Batch fetching parent relationships (async)")
            logger.info("="*80)
            logger.info(f"Fetching parents for {len(parent_urls_map)} terms with parent links...")
            
            # Get all unique parent URLs
            parent_urls_list = list(parent_urls_map.values())
            
            # Batch fetch all parents concurrently (10-20x faster!)
            parent_url_to_iris = ols_client.batch_fetch_parents(parent_urls_list, config.ols_request_delay)
            
            # Convert back to IRI-based mapping
            for child_iri, parent_url in parent_urls_map.items():
                parent_iris = parent_url_to_iris.get(parent_url, [])
                if parent_iris:
                    parent_refs_map[child_iri] = parent_iris
            
            logger.info(f"Successfully fetched parents for {len(parent_refs_map)} terms")
        else:
            logger.info("No parent relationships to fetch")
        
        # Phase 2: Process relationships (requires IRI-to-ID mapping)
        logger.info("="*80)
        logger.info("PHASE 2: Processing ontology relationships")
        logger.info("="*80)
        
        logger.info("Building IRI-to-ID mapping...")
        iri_to_id_map = postgres_loader.get_iri_to_id_map(connection)
        logger.info(f"Mapped {len(iri_to_id_map)} terms")
        
        relationships_batch = []
        
        for child_iri, parent_iris in parent_refs_map.items():
            normalized_relationships = efo_transformer.normalize_relationships(
                child_iri,
                parent_iris,
                iri_to_id_map
            )
            relationships_batch.extend(normalized_relationships)
            
            # Insert batch when size threshold reached
            if len(relationships_batch) >= config.batch_size:
                postgres_loader.bulk_insert_relationships(connection, relationships_batch)
                relationships_batch.clear()
        
        # Insert remaining relationships
        if relationships_batch:
            postgres_loader.bulk_insert_relationships(connection, relationships_batch)
            relationships_batch.clear()
        
        logger.info("Relationships processing complete")
        
        # Process MeSH cross-references
        logger.info(f"Processing MeSH cross-references for {len(mesh_xrefs_map)} terms...")
        mesh_xrefs_batch = []
        
        for term_id, mesh_ids in mesh_xrefs_map.items():
            normalized_mesh_xrefs = efo_transformer.normalize_mesh_xrefs(
                term_id,
                mesh_ids
            )
            mesh_xrefs_batch.extend(normalized_mesh_xrefs)
            
            # Insert batch when size threshold reached
            if len(mesh_xrefs_batch) >= config.batch_size:
                postgres_loader.bulk_insert_mesh_xrefs(connection, mesh_xrefs_batch)
                mesh_xrefs_batch.clear()
        
        # Insert remaining MeSH xrefs
        if mesh_xrefs_batch:
            postgres_loader.bulk_insert_mesh_xrefs(connection, mesh_xrefs_batch)
            mesh_xrefs_batch.clear()
        
        logger.info("Phase 2 complete: Relationships and MeSH xrefs processed")
        
        # Update execution record with success
        postgres_loader.update_execution_record(
            connection,
            execution_id,
            'success',
            stats
        )
        
        # Log completion
        logger.info("="*80)
        logger.info("PIPELINE EXECUTION COMPLETE")
        logger.info("="*80)
        logger.info(f"Terms fetched: {stats['terms_fetched']}")
        logger.info(f"Terms inserted: {stats['terms_inserted']}")
        logger.info(f"Terms updated: {stats['terms_updated']}")
        logger.info(f"Terms skipped: {stats['terms_skipped']}")
        logger.info("="*80)
        
    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user")
        if connection and execution_id:
            postgres_loader.update_execution_record(
                connection,
                execution_id,
                'failed',
                error_message='Interrupted by user'
            )
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        if connection and execution_id:
            postgres_loader.update_execution_record(
                connection,
                execution_id,
                'failed',
                stats,
                error_message=str(e)
            )
        sys.exit(1)
        
    finally:
        # Close database connection
        if connection:
            postgres_loader.close(connection)


if __name__ == '__main__':
    main()

