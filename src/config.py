"""
Configuration Management Module

Loads and validates configuration from environment variables using python-dotenv.
Provides centralized access to all pipeline configuration parameters.
"""

import os
import logging
from pathlib import Path
from dotenv import load_dotenv


class Config:
    """
    Configuration class for EFO Data Pipeline.
    
    Loads settings from environment variables and provides validated access.
    All configuration parameters have sensible defaults.
    """
    
    def __init__(self):
        """Initialize configuration by loading environment variables."""
        # Load .env file if it exists (for local development)
        env_path = Path('.') / '.env'
        if env_path.exists():
            load_dotenv(dotenv_path=env_path)
        
        # Database Configuration
        self.db_host = os.getenv('DB_HOST', 'localhost')
        self.db_port = int(os.getenv('DB_PORT', '5432'))
        self.db_name = os.getenv('DB_NAME', 'efo_data')
        self.db_user = os.getenv('DB_USER', 'efo_user')
        self.db_password = os.getenv('DB_PASSWORD', '')
        
        # OLS API Configuration
        self.ols_base_url = os.getenv('OLS_BASE_URL', 'https://www.ebi.ac.uk/ols4/api')
        self.ols_request_delay = float(os.getenv('OLS_REQUEST_DELAY', '0.1'))
        
        # Pipeline Configuration
        self.batch_size = int(os.getenv('BATCH_SIZE', '250'))
        self.log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
        self.execution_mode = os.getenv('EXECUTION_MODE', 'test').lower()
        self.record_limit = int(os.getenv('RECORD_LIMIT', '100'))
        
        # Validate configuration
        self._validate()
    
    def _validate(self):
        """Validate configuration parameters."""
        # Validate execution mode
        valid_modes = ['full', 'incremental', 'test']
        if self.execution_mode not in valid_modes:
            raise ValueError(
                f"Invalid EXECUTION_MODE: {self.execution_mode}. "
                f"Must be one of: {', '.join(valid_modes)}"
            )
        
        # Validate log level
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR']
        if self.log_level not in valid_levels:
            raise ValueError(
                f"Invalid LOG_LEVEL: {self.log_level}. "
                f"Must be one of: {', '.join(valid_levels)}"
            )
        
        # Validate numeric parameters
        if self.batch_size < 1:
            raise ValueError(f"BATCH_SIZE must be positive, got: {self.batch_size}")
        
        if self.ols_request_delay < 0:
            raise ValueError(f"OLS_REQUEST_DELAY must be non-negative, got: {self.ols_request_delay}")
        
        if self.record_limit < 0:
            raise ValueError(f"RECORD_LIMIT must be non-negative, got: {self.record_limit}")
        
        # Warn if no database password set
        if not self.db_password:
            logging.warning("DB_PASSWORD not set - this may cause connection failures")
    
    def get_db_connection_params(self):
        """
        Get database connection parameters as a dict.
        
        Returns:
            dict: Connection parameters for psycopg2.connect()
        """
        return {
            'host': self.db_host,
            'port': self.db_port,
            'dbname': self.db_name,
            'user': self.db_user,
            'password': self.db_password
        }
    
    def __repr__(self):
        """String representation of configuration (hides password)."""
        return (
            f"Config(db_host={self.db_host}, db_port={self.db_port}, "
            f"db_name={self.db_name}, db_user={self.db_user}, "
            f"execution_mode={self.execution_mode}, batch_size={self.batch_size})"
        )


def setup_logging(log_level='INFO'):
    """
    Configure logging for the pipeline.
    
    Sets up structured logging with timestamps and appropriate formatting
    for Docker container output.
    
    Args:
        log_level (str): Logging level (DEBUG, INFO, WARNING, ERROR)
    
    Returns:
        logging.Logger: Configured logger instance
    """
    # Convert string level to logging constant
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    
    # Configure root logger
    logging.basicConfig(
        level=numeric_level,
        format='[%(asctime)s] [%(levelname)s] [%(module)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Get logger for the application
    logger = logging.getLogger('efo_pipeline')
    logger.setLevel(numeric_level)
    
    return logger


# Global configuration instance (singleton pattern)
_config_instance = None


def get_config():
    """
    Get the global configuration instance.
    
    Uses singleton pattern to ensure only one configuration object exists.
    
    Returns:
        Config: Global configuration instance
    """
    global _config_instance
    if _config_instance is None:
        _config_instance = Config()
    return _config_instance

