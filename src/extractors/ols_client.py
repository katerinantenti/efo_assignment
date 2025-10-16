"""
OLS API Client

Implements data retrieval from the Ontology Lookup Service (OLS) API.
Handles pagination, retries, and data extraction from EFO term JSON responses.
"""

import logging
import time
import requests
import asyncio
import aiohttp
import sys
import ssl
from typing import Dict, List, Optional, Generator

# Try to import certifi for better SSL handling on Windows
try:
    import certifi
    CERTIFI_AVAILABLE = True
except ImportError:
    CERTIFI_AVAILABLE = False


logger = logging.getLogger('efo_pipeline.extractor')


def test_connection():
    """
    Diagnostic function to test if we can connect to the OLS API.
    Call this to diagnose Windows connectivity issues.
    """
    test_url = "https://www.ebi.ac.uk/ols4/api/ontologies/efo"
    
    logger.info("="*60)
    logger.info("DIAGNOSTIC: Testing connection to OLS API")
    logger.info("="*60)
    
    # Test 1: Basic requests
    logger.info("Test 1: Basic requests.get()")
    try:
        response = requests.get(test_url, timeout=10)
        logger.info(f"  ✅ SUCCESS: HTTP {response.status_code}")
    except Exception as e:
        logger.error(f"  ❌ FAILED: {type(e).__name__}: {e}")
    
    # Test 2: With certifi
    if CERTIFI_AVAILABLE:
        logger.info("Test 2: requests with certifi")
        try:
            response = requests.get(test_url, timeout=10, verify=certifi.where())
            logger.info(f"  ✅ SUCCESS: HTTP {response.status_code}")
        except Exception as e:
            logger.error(f"  ❌ FAILED: {type(e).__name__}: {e}")
    else:
        logger.warning("Test 2: SKIPPED (certifi not installed)")
        logger.warning("  Run: pip install certifi")
    
    # Test 3: SSL info
    logger.info("Test 3: SSL/Certificate info")
    logger.info(f"  Python version: {sys.version}")
    logger.info(f"  Platform: {sys.platform}")
    logger.info(f"  Certifi available: {CERTIFI_AVAILABLE}")
    if CERTIFI_AVAILABLE:
        logger.info(f"  Certifi location: {certifi.where()}")
    
    logger.info("="*60)


def fetch_terms_page(base_url: str, page_number: int, delay: float = 0.1) -> Optional[Dict]:
    """
    Fetch a single page of EFO terms from the OLS API.
    
    Implements retry logic with exponential backoff for transient errors.
    
    Args:
        base_url (str): OLS API base URL
        page_number (int): Page number to fetch (0-indexed)
        delay (float): Delay in seconds after successful request (courtesy delay)
    
    Returns:
        Dict or None: JSON response as dict, or None on failure
    """
    url = f"{base_url}/ontologies/efo/terms"
    params = {'page': page_number}
    
    # Retry configuration
    max_retries = 3
    retry_delay = 1  # Initial delay in seconds
    
    for attempt in range(max_retries):
        try:
            logger.debug(f"Fetching page {page_number} (attempt {attempt + 1}/{max_retries})")
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()  # Raise exception for 4xx/5xx status codes
            
            # Add courtesy delay to avoid overwhelming the API
            if delay > 0:
                time.sleep(delay)
            
            return response.json()
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:  # Rate limited
                logger.warning(f"Rate limited on page {page_number}, waiting longer...")
                time.sleep(retry_delay * 2)
            elif e.response.status_code >= 500:  # Server error
                logger.warning(f"Server error on page {page_number}: {e}")
                time.sleep(retry_delay)
            else:  # Client error (4xx)
                logger.error(f"Client error on page {page_number}: {e}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request failed on page {page_number}: {e}")
            time.sleep(retry_delay)
        
        # Exponential backoff for retry
        retry_delay *= 2
    
    logger.error(f"Failed to fetch page {page_number} after {max_retries} attempts")
    return None


def extract_term_data(term_json: Dict) -> Dict:
    """
    Extract core term data from OLS API JSON response.
    
    Args:
        term_json (Dict): Single term object from API response
    
    Returns:
        Dict: Extracted term data with keys: term_id, iri, label, description
    """
    return {
        'term_id': term_json.get('obo_id', ''),
        'iri': term_json.get('iri', ''),
        'label': term_json.get('label', ''),
        'description': term_json.get('description', [''])[0] if term_json.get('description') else None
    }


def extract_synonyms(term_json: Dict) -> List[str]:
    """
    Extract synonyms from term JSON.
    
    Args:
        term_json (Dict): Single term object from API response
    
    Returns:
        List[str]: List of synonym strings (may be empty)
    """
    synonyms = term_json.get('synonyms', [])
    
    # Filter out None and empty strings
    if synonyms:
        return [syn for syn in synonyms if syn]
    return []


def fetch_parent_iris(term_json: Dict, delay: float = 0.1) -> List[str]:
    """
    Fetch parent term IRIs by making an additional API call.
    
    The OLS API doesn't include parent IRIs directly in list responses.
    Instead, it provides a URL to fetch them. This function follows that URL.
    
    Args:
        term_json (Dict): Single term object from API response
        delay (float): Delay after request (seconds)
    
    Returns:
        List[str]: List of parent IRIs (may be empty)
    """
    parents = []
    
    # Check for parents link in _links section
    links = term_json.get('_links', {})
    parents_link = links.get('parents')
    
    # Extract the URL to fetch parents
    parent_url = None
    if isinstance(parents_link, dict):
        parent_url = parents_link.get('href')
    elif isinstance(parents_link, str):
        parent_url = parents_link
    
    # If no parent URL, return empty list
    if not parent_url:
        return []
    
    # Fetch parent data from the API
    try:
        logger.debug(f"Fetching parents from: {parent_url}")
        response = requests.get(parent_url, timeout=10)
        response.raise_for_status()
        
        # Add courtesy delay
        if delay > 0:
            time.sleep(delay)
        
        parent_data = response.json()
        
        # Extract parent IRIs from response
        embedded = parent_data.get('_embedded', {})
        parent_terms = embedded.get('terms', [])
        
        for parent_term in parent_terms:
            parent_iri = parent_term.get('iri')
            if parent_iri:
                parents.append(parent_iri)
        
        logger.debug(f"Found {len(parents)} parent(s)")
        
    except requests.exceptions.RequestException as e:
        logger.warning(f"Failed to fetch parents from {parent_url}: {e}")
    
    return parents


async def fetch_parent_iris_async(session: aiohttp.ClientSession, parent_url: str) -> List[str]:
    """
    Async version: Fetch parent term IRIs from a parent URL.
    
    Args:
        session (aiohttp.ClientSession): Async HTTP session
        parent_url (str): URL to fetch parents from
    
    Returns:
        List[str]: List of parent IRIs (may be empty)
    """
    try:
        async with session.get(parent_url, timeout=aiohttp.ClientTimeout(total=10)) as response:
            if response.status == 200:
                parent_data = await response.json()
                embedded = parent_data.get('_embedded', {})
                parent_terms = embedded.get('terms', [])
                return [pt.get('iri') for pt in parent_terms if pt.get('iri')]
    except Exception as e:
        logger.warning(f"Failed to fetch parents from {parent_url}: {e}")
    return []


async def batch_fetch_parents_async(parent_urls: List[str], delay: float = 0.05) -> Dict[str, List[str]]:
    """
    Fetch multiple parent relationships concurrently using async requests.
    
    This is MUCH faster than sequential fetching (10-20x speedup).
    
    Args:
        parent_urls (List[str]): List of parent URLs to fetch
        delay (float): Delay between batches (seconds)
    
    Returns:
        Dict[str, List[str]]: Mapping of parent_url -> list of parent IRIs
    """
    results = {}
    
    # Create SSL context for better Windows compatibility
    ssl_context = None
    if CERTIFI_AVAILABLE:
        ssl_context = ssl.create_default_context(cafile=certifi.where())
    else:
        ssl_context = ssl.create_default_context()
    
    # Create async HTTP session with connection pooling
    # Windows compatibility: Use force_close=True to avoid connection issues
    connector = aiohttp.TCPConnector(
        limit=50,  # Max 50 concurrent connections (optimized)
        force_close=True,  # Close connections after each request (Windows compatibility)
        enable_cleanup_closed=True,  # Clean up closed SSL transports (Windows compatibility)
        ssl=ssl_context  # Use proper SSL context
    )
    timeout = aiohttp.ClientTimeout(total=30)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # Process URLs in batches to avoid overwhelming the API
        batch_size = 50  # Larger batches for better throughput
        for i in range(0, len(parent_urls), batch_size):
            batch = parent_urls[i:i+batch_size]
            
            # Fetch batch concurrently
            tasks = [fetch_parent_iris_async(session, url) for url in batch]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Store results (handle exceptions from gather)
            for url, result in zip(batch, batch_results):
                if isinstance(result, Exception):
                    logger.warning(f"Failed to fetch parents from {url}: {result}")
                    results[url] = []
                else:
                    results[url] = result
            
            # Small delay between batches
            if delay > 0 and i + batch_size < len(parent_urls):
                await asyncio.sleep(delay)
    
    return results


def batch_fetch_parents(parent_urls: List[str], delay: float = 0.05) -> Dict[str, List[str]]:
    """
    Synchronous wrapper for async batch parent fetching.
    
    This function ensures cross-platform compatibility by setting the appropriate
    event loop policy on Windows. On Windows, we use SelectorEventLoop instead of
    the default ProactorEventLoop to ensure consistent behavior with network operations.
    
    If async fetching fails, it falls back to synchronous fetching for reliability.
    
    Args:
        parent_urls (List[str]): List of parent URLs to fetch
        delay (float): Delay between batches (seconds)
    
    Returns:
        Dict[str, List[str]]: Mapping of parent_url -> list of parent IRIs
    """
    logger.info(f"Batch fetching parents for {len(parent_urls)} terms (async mode)")
    
    # Set the event loop policy for Windows compatibility
    # On Windows, use SelectorEventLoop instead of the default ProactorEventLoop
    # for better compatibility with network operations (aiohttp)
    if sys.platform == 'win32':
        # Check if we're on Python 3.8+ which has WindowsSelectorEventLoopPolicy
        if sys.version_info >= (3, 8):
            logger.debug("Windows detected: setting WindowsSelectorEventLoopPolicy for async compatibility")
            try:
                asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
            except Exception as e:
                logger.warning(f"Failed to set Windows event loop policy: {e}")
        else:
            logger.debug(f"Windows detected with Python {sys.version_info.major}.{sys.version_info.minor} - using default event loop")
    else:
        logger.debug(f"Platform: {sys.platform} - using default event loop policy")
    
    # Try async fetching first
    try:
        return asyncio.run(batch_fetch_parents_async(parent_urls, delay))
    except Exception as e:
        logger.error(f"Async batch fetching failed: {e}. Falling back to synchronous fetching...")
        logger.debug("Exception details:", exc_info=True)
        
        # Fallback: fetch parents synchronously
        return batch_fetch_parents_sync(parent_urls, delay)


def batch_fetch_parents_sync(parent_urls: List[str], delay: float = 0.05) -> Dict[str, List[str]]:
    """
    Synchronous fallback for batch parent fetching.
    
    This function fetches parent relationships sequentially using regular requests.
    Much slower than async, but guaranteed to work on any platform.
    Includes retry logic and SSL certificate handling for Windows compatibility.
    
    Args:
        parent_urls (List[str]): List of parent URLs to fetch
        delay (float): Delay between requests (seconds)
    
    Returns:
        Dict[str, List[str]]: Mapping of parent_url -> list of parent IRIs
    """
    logger.info(f"Fetching parents synchronously for {len(parent_urls)} URLs...")
    
    # Create a session with proper SSL configuration
    session = requests.Session()
    
    # Set SSL verification - use certifi if available for Windows
    if CERTIFI_AVAILABLE:
        logger.debug("Using certifi for SSL certificate verification")
        session.verify = certifi.where()
    
    # Set headers to mimic a browser (some servers block default requests headers)
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    })
    
    results = {}
    failed_count = 0
    success_count = 0
    
    for idx, url in enumerate(parent_urls):
        max_retries = 3
        retry_delay = 1
        
        for attempt in range(max_retries):
            try:
                response = session.get(url, timeout=15)
                
                if response.status_code == 200:
                    parent_data = response.json()
                    embedded = parent_data.get('_embedded', {})
                    parent_terms = embedded.get('terms', [])
                    parent_iris = [pt.get('iri') for pt in parent_terms if pt.get('iri')]
                    results[url] = parent_iris
                    success_count += 1
                    break  # Success, exit retry loop
                    
                elif response.status_code == 404:
                    # Not found - don't retry
                    logger.debug(f"No parents found (404) for {url}")
                    results[url] = []
                    break
                    
                elif response.status_code == 429:
                    # Rate limited - wait longer
                    logger.warning(f"Rate limited on {url}, waiting {retry_delay * 2}s...")
                    time.sleep(retry_delay * 2)
                    retry_delay *= 2
                    continue
                    
                else:
                    logger.warning(f"HTTP {response.status_code} for {url} (attempt {attempt + 1}/{max_retries})")
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                        retry_delay *= 2
                    else:
                        results[url] = []
                        failed_count += 1
                        
            except requests.exceptions.SSLError as e:
                logger.error(f"SSL Error for {url}: {e}")
                logger.error(f"  This might be a certificate verification issue on Windows")
                logger.error(f"  Try: pip install --upgrade certifi")
                results[url] = []
                failed_count += 1
                break  # Don't retry SSL errors
                
            except requests.exceptions.ConnectionError as e:
                logger.warning(f"Connection error for {url}: {e} (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    results[url] = []
                    failed_count += 1
                    
            except requests.exceptions.Timeout as e:
                logger.warning(f"Timeout for {url}: {e} (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    results[url] = []
                    failed_count += 1
                    
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed for {url}: {type(e).__name__}: {e}")
                results[url] = []
                failed_count += 1
                break
                
            except Exception as e:
                logger.error(f"Unexpected error for {url}: {type(e).__name__}: {e}")
                results[url] = []
                failed_count += 1
                break
        
        # Progress logging every 100 URLs with statistics
        if (idx + 1) % 100 == 0:
            logger.info(f"Progress: {idx + 1}/{len(parent_urls)} URLs processed "
                       f"(✓ {success_count} successful, ✗ {failed_count} failed)")
        
        # Add delay between requests
        if delay > 0:
            time.sleep(delay)
    
    session.close()
    
    logger.info(f"Synchronous fetching complete: {len(results)} URLs processed "
               f"(✓ {success_count} successful, ✗ {failed_count} failed)")
    
    if failed_count > 0:
        logger.warning(f"⚠️  {failed_count} URLs failed to fetch. Parent relationships for those terms will be incomplete.")
        logger.warning(f"   Success rate: {(success_count / len(parent_urls) * 100):.1f}%")
    
    return results


def extract_parent_iris(term_json: Dict) -> List[str]:
    """
    Extract parent term IRIs from the _links section (legacy - URLs only).
    
    Note: This function returns URLs, not IRIs. Use fetch_parent_iris() instead
    to get actual parent IRIs.
    
    Args:
        term_json (Dict): Single term object from API response
    
    Returns:
        List[str]: List of parent URLs (may be empty)
    """
    parents = []
    
    # Check for parents in _links section
    links = term_json.get('_links', {})
    parents_link = links.get('parents')
    
    if not parents_link:
        return []
    
    # Handle both dict (single link) and list (multiple links) formats
    if isinstance(parents_link, dict):
        # Single parent link: {"href": "url"}
        parent_url = parents_link.get('href', '')
        if parent_url:
            parents.append(parent_url)
    elif isinstance(parents_link, list):
        # Multiple parent links: [{"href": "url1"}, {"href": "url2"}]
        for parent_link in parents_link:
            if isinstance(parent_link, dict):
                parent_url = parent_link.get('href', '')
                if parent_url:
                    parents.append(parent_url)
    elif isinstance(parents_link, str):
        # Direct URL string (rare)
        parents.append(parents_link)
    
    return parents


def extract_mesh_xrefs(term_json: Dict) -> List[str]:
    """
    Extract MeSH term cross-references from OBO cross-reference data.
    
    Checks both 'obo_xref' field (structured format) and 'annotation.database_cross_reference' (string format).
    
    Args:
        term_json (Dict): Single term object from API response
    
    Returns:
        List[str]: List of MeSH IDs (without MSH: prefix)
    """
    mesh_ids = []
    
    # Method 1: Check obo_xref field (structured format with 'database' and 'id' keys)
    obo_xrefs = term_json.get('obo_xref') or []
    if isinstance(obo_xrefs, list):
        for xref in obo_xrefs:
            if isinstance(xref, dict):
                database = xref.get('database', '')
                xref_id = xref.get('id', '')
                
                # Check if it's a MeSH reference
                if database and ('MSH' in database.upper() or 'MESH' in database.upper()):
                    if xref_id:
                        mesh_ids.append(xref_id)
    
    # Method 2: Check annotation.database_cross_reference field (string format like "MSH:D123456")
    annotation = term_json.get('annotation', {})
    xrefs = annotation.get('database_cross_reference', [])
    
    if isinstance(xrefs, list):
        for xref in xrefs:
            if isinstance(xref, str) and xref.startswith('MSH:'):
                # Extract MeSH ID (remove MSH: prefix)
                mesh_id = xref.replace('MSH:', '', 1)
                if mesh_id and mesh_id not in mesh_ids:  # Avoid duplicates
                    mesh_ids.append(mesh_id)
    
    return mesh_ids


def fetch_all_terms(
    base_url: str,
    delay: float = 0.1,
    limit: Optional[int] = None
) -> Generator[Dict, None, None]:
    """
    Fetch all EFO terms from OLS API with pagination support.
    
    Yields terms one at a time as a generator for memory efficiency.
    
    Args:
        base_url (str): OLS API base URL
        delay (float): Delay between requests (seconds)
        limit (int, optional): Maximum number of terms to retrieve (for testing). None = all terms.
    
    Yields:
        Dict: Individual term objects from API responses
    """
    page_number = 0
    terms_yielded = 0
    
    logger.info(f"Starting term retrieval (limit: {limit if limit else 'unlimited'})")
    
    while True:
        # Check if we've reached the limit
        if limit and terms_yielded >= limit:
            logger.info(f"Reached term limit: {limit}")
            break
        
        # Fetch page
        response_data = fetch_terms_page(base_url, page_number, delay)
        
        if response_data is None:
            logger.error(f"Failed to fetch page {page_number}, stopping retrieval")
            break
        
        # Extract terms from response
        embedded = response_data.get('_embedded', {})
        terms = embedded.get('terms', [])
        
        if not terms:
            logger.info(f"No more terms found at page {page_number}")
            break
        
        # Yield terms (respecting limit)
        for term in terms:
            if limit and terms_yielded >= limit:
                break
            yield term
            terms_yielded += 1
        
        # Log progress
        if page_number % 10 == 0:
            logger.info(f"Fetched {terms_yielded} terms (page {page_number})")
        
        # Check pagination info
        page_info = response_data.get('page', {})
        total_pages = page_info.get('totalPages', 0)
        current_page = page_info.get('number', 0)
        
        # Stop if this was the last page
        if total_pages > 0 and current_page >= total_pages - 1:
            logger.info(f"Reached last page: {current_page}/{total_pages - 1}")
            break
        
        # Move to next page
        page_number += 1
    
    logger.info(f"Term retrieval complete: {terms_yielded} terms fetched")

