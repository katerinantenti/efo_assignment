"""
EFO Data Transformer

Normalizes and validates data extracted from OLS API for database storage.
Implements content hashing for change detection in incremental updates.
"""

import logging
import hashlib
from typing import Dict, List, Tuple, Optional


logger = logging.getLogger('efo_pipeline.transformer')


def normalize_term(term_dict: Dict) -> Optional[Dict]:
    """
    Normalize and validate term data for database insertion.
    
    Args:
        term_dict (Dict): Raw term data with keys: term_id, iri, label, description
    
    Returns:
        Dict or None: Normalized term dict, or None if validation fails
    """
    # Validate required fields
    term_id = term_dict.get('term_id', '').strip()
    iri = term_dict.get('iri', '').strip()
    label = term_dict.get('label', '').strip()
    
    if not term_id or not iri or not label:
        logger.warning(f"Invalid term: missing required fields (term_id={term_id}, iri={iri}, label={label})")
        return None
    
    # Sanitize description (may be None)
    description = term_dict.get('description')
    if description:
        description = description.strip()
        if not description:
            description = None
    
    # Return normalized term
    return {
        'term_id': term_id,
        'iri': iri,
        'label': label,
        'description': description,
        'content_hash': None  # Will be computed later if needed
    }


def normalize_synonyms(term_id: str, synonyms_list: List[str]) -> List[Tuple[str, str]]:
    """
    Normalize synonyms for database insertion.
    
    Args:
        term_id (str): EFO term ID that owns these synonyms
        synonyms_list (List[str]): List of synonym strings
    
    Returns:
        List[Tuple[str, str]]: List of (term_id, synonym) tuples ready for database
    """
    normalized = []
    
    for synonym in synonyms_list:
        if synonym and isinstance(synonym, str):
            synonym = synonym.strip()
            if synonym:
                normalized.append((term_id, synonym))
    
    return normalized


def normalize_relationships(
    child_iri: str,
    parent_iris: List[str],
    iri_to_id_map: Dict[str, int]
) -> List[Tuple[int, int]]:
    """
    Normalize parent-child relationships for database insertion.
    
    Maps IRIs to internal database IDs using the provided mapping.
    
    Args:
        child_iri (str): IRI of the child term
        parent_iris (List[str]): List of parent term IRIs or hrefs
        iri_to_id_map (Dict[str, int]): Mapping of IRI -> internal database ID
    
    Returns:
        List[Tuple[int, int]]: List of (child_id, parent_id) tuples ready for database
    """
    relationships = []
    
    # Get child's internal ID
    child_id = iri_to_id_map.get(child_iri)
    if not child_id:
        logger.debug(f"Child IRI not found in mapping: {child_iri}")
        return relationships
    
    # Process each parent
    for parent_ref in parent_iris:
        # The parent_ref might be a full href or just an IRI
        # Try to extract the IRI if it's a URL
        parent_iri = extract_iri_from_href(parent_ref)
        
        # Get parent's internal ID
        parent_id = iri_to_id_map.get(parent_iri)
        if parent_id:
            relationships.append((child_id, parent_id))
        else:
            logger.debug(f"Parent IRI not found in mapping: {parent_iri}")
    
    return relationships


def extract_iri_from_href(href: str) -> str:
    """
    Extract IRI from OLS API href URL.
    
    The href might be:
    - A direct IRI: http://www.ebi.ac.uk/efo/EFO_0000001
    - A URL with IRI parameter: ...?iri=http://...
    
    Args:
        href (str): The href string from _links
    
    Returns:
        str: Extracted IRI
    """
    # If it contains "?iri=", extract the IRI parameter
    if '?iri=' in href:
        iri_param = href.split('?iri=')[1]
        # URL decode if needed (basic handling)
        iri_param = iri_param.split('&')[0]  # Remove other params
        return iri_param
    
    # If it's an OLS terms endpoint, extract the IRI from path
    if '/terms/' in href:
        # Format: .../ontologies/efo/terms/http%3A%2F%2F...
        parts = href.split('/terms/')
        if len(parts) > 1:
            encoded_iri = parts[1]
            # Basic URL decode
            decoded_iri = encoded_iri.replace('%3A', ':').replace('%2F', '/')
            return decoded_iri
    
    # Otherwise, assume it's already an IRI
    return href


def normalize_mesh_xrefs(term_id: str, mesh_ids_list: List[str]) -> List[Tuple[str, str, str]]:
    """
    Normalize MeSH cross-references for database insertion.
    
    Args:
        term_id (str): EFO term ID that owns these cross-references
        mesh_ids_list (List[str]): List of MeSH ID strings
    
    Returns:
        List[Tuple[str, str, str]]: List of (term_id, mesh_id, 'MSH') tuples ready for database
    """
    normalized = []
    
    for mesh_id in mesh_ids_list:
        if mesh_id and isinstance(mesh_id, str):
            mesh_id = mesh_id.strip()
            if mesh_id:
                normalized.append((term_id, mesh_id, 'MSH'))
    
    return normalized


def compute_term_hash(
    term_dict: Dict,
    synonyms_list: List[str],
    parent_iris: List[str]
) -> str:
    """
    Compute SHA-256 hash of term content for change detection.
    
    The hash is computed from:
    - Term label and description
    - Sorted list of synonyms
    - Sorted list of parent IRIs
    
    This allows incremental updates to detect when a term's content has changed.
    
    Args:
        term_dict (Dict): Term data dict
        synonyms_list (List[str]): List of synonyms
        parent_iris (List[str]): List of parent IRIs
    
    Returns:
        str: 64-character hexadecimal hash string
    """
    # Build content string from all significant attributes
    label = term_dict.get('label', '')
    description = term_dict.get('description') or ''
    
    # Sort lists for consistent hashing
    sorted_synonyms = sorted(synonyms_list)
    sorted_parents = sorted(parent_iris)
    
    # Concatenate all content
    content = f"{label}|{description}|{','.join(sorted_synonyms)}|{','.join(sorted_parents)}"
    
    # Compute SHA-256 hash
    hash_obj = hashlib.sha256(content.encode('utf-8'))
    return hash_obj.hexdigest()


def add_content_hash_to_term(
    term_dict: Dict,
    synonyms_list: List[str],
    parent_iris: List[str]
) -> Dict:
    """
    Add content_hash field to term dict.
    
    Modifies the term_dict in place and returns it for convenience.
    
    Args:
        term_dict (Dict): Normalized term dict
        synonyms_list (List[str]): List of synonyms
        parent_iris (List[str]): List of parent IRIs
    
    Returns:
        Dict: Term dict with content_hash added
    """
    term_dict['content_hash'] = compute_term_hash(term_dict, synonyms_list, parent_iris)
    return term_dict

