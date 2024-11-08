import configparser
import logging
import os
from datetime import datetime
import json
from typing import Dict, Any

class ConfigUtils:
    @staticmethod
    def load_config(config_file: str = 'db.ini') -> configparser.ConfigParser:
        """
        Load configuration from db.ini file
        
        Args:
            config_file (str): Path to configuration file
            
        Returns:
            configparser.ConfigParser: Loaded configuration object
            
        Raises:
            FileNotFoundError: If config file doesn't exist
        """
        config = configparser.ConfigParser()
        if os.path.exists(config_file):
            config.read(config_file)
        else:
            config = ConfigUtils.create_default_config()
            ConfigUtils.save_config(config)
        return config

    @staticmethod
    def save_config(config: configparser.ConfigParser, config_file: str = 'db.ini') -> bool:
        """
        Save configuration to db.ini file
        
        Args:
            config (configparser.ConfigParser): Configuration object to save
            config_file (str): Path to save configuration file
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            with open(config_file, 'w') as configfile:
                config.write(configfile)
            return True
        except Exception as e:
            logging.error(f"Failed to save config: {str(e)}")
            return False

    @staticmethod
    def create_default_config() -> configparser.ConfigParser:
        """
        Create default configuration with standard settings
        
        Returns:
            configparser.ConfigParser: Default configuration object
        """
        config = configparser.ConfigParser()
        
        # PostgreSQL defaults
        config['postgresql'] = {
            'host': 'localhost',
            'port': '5432',
            'database': 'skie',
            'user': 'postgres',
            'password': 'postgres'
        }
        
        # MongoDB defaults
        config['mongodb'] = {
            'host': 'localhost',
            'port': '27017',
            'database': 'skie',
            'user': 'admin',
            'password': 'admin'
        }
        
        # Neo4j defaults
        config['neo4j'] = {
            'url': 'bolt://localhost:7687',
            'user': 'neo4j',
            'password': 'neo4j_password'
        }
        
        # Kafka defaults
        config['kafka'] = {
            'bootstrap_servers': 'localhost:9092',
            'zookeeper': 'localhost:2181',
            'schema_registry': 'http://localhost:8081',
            'connect_rest': 'http://localhost:8083',
            'topic_prefix': 'skie',
            'group_id': 'skie_group',
            'auto_offset_reset': 'earliest'
        }
        
        return config

class LoggingUtils:
    @staticmethod
    def setup_logging(
        log_file: str = 'app.log',
        log_level: int = logging.INFO,
        format_string: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ) -> logging.Logger:
        """
        Setup logging configuration
        
        Args:
            log_file (str): Path to log file
            log_level (int): Logging level
            format_string (str): Log format string
            
        Returns:
            logging.Logger: Configured logger object
        """
        # Create logs directory if it doesn't exist
        log_dir = 'logs'
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        log_path = os.path.join(log_dir, log_file)
        
        # Configure logging
        logging.basicConfig(
            level=log_level,
            format=format_string,
            handlers=[
                logging.FileHandler(log_path),
                logging.StreamHandler()
            ]
        )
        
        logger = logging.getLogger(__name__)
        logger.info("Logging initialized")
        return logger

class MappingUtils:
    @staticmethod
    def validate_mapping(mapping_data: Dict[str, Any]) -> tuple[bool, str]:
        """
        Validate mapping configuration
        
        Args:
            mapping_data (dict): Mapping configuration to validate
            
        Returns:
            tuple[bool, str]: (is_valid, error_message)
        """
        try:
            # Check required top-level keys
            required_keys = ['source', 'target', 'columns']
            for key in required_keys:
                if key not in mapping_data:
                    return False, f"Missing required key: {key}"
            
            # Validate source configuration
            source = mapping_data['source']
            if not all(key in source for key in ['type', 'table']):
                return False, "Invalid source configuration"
                
            # Validate target configuration
            target = mapping_data['target']
            if not all(key in target for key in ['type', 'label']):
                return False, "Invalid target configuration"
                
            # Validate columns mapping
            columns = mapping_data['columns']
            if not isinstance(columns, dict):
                return False, "Columns must be a dictionary"
            if not columns:
                return False, "No columns mapped"
            
            return True, "Mapping configuration is valid"
            
        except Exception as e:
            return False, f"Validation error: {str(e)}"

    @staticmethod
    def format_timestamp(timestamp=None, format_string: str = '%Y-%m-%d %H:%M:%S') -> str:
        """
        Format timestamp string
        
        Args:
            timestamp: Timestamp to format (defaults to current time)
            format_string (str): Desired format string
            
        Returns:
            str: Formatted timestamp string
        """
        if timestamp is None:
            timestamp = datetime.now()
        elif isinstance(timestamp, (int, float)):
            timestamp = datetime.fromtimestamp(timestamp)
        elif isinstance(timestamp, str):
            try:
                timestamp = datetime.fromisoformat(timestamp)
            except ValueError:
                try:
                    timestamp = datetime.fromtimestamp(float(timestamp))
                except ValueError:
                    return str(timestamp)
        
        try:
            return timestamp.strftime(format_string)
        except Exception as e:
            logging.error(f"Error formatting timestamp: {str(e)}")
            return str(timestamp)

class FileUtils:
    @staticmethod
    def ensure_directory(directory: str) -> bool:
        """
        Ensure directory exists, create if it doesn't
        
        Args:
            directory (str): Directory path
            
        Returns:
            bool: True if directory exists or was created successfully
        """
        try:
            if not os.path.exists(directory):
                os.makedirs(directory)
            return True
        except Exception as e:
            logging.error(f"Failed to create directory {directory}: {str(e)}")
            return False

    @staticmethod
    def save_json(data: Dict[str, Any], filepath: str, pretty: bool = True) -> bool:
        """
        Save data to JSON file
        
        Args:
            data (dict): Data to save
            filepath (str): Path to save file
            pretty (bool): Whether to format JSON prettily
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            directory = os.path.dirname(filepath)
            if directory and not os.path.exists(directory):
                os.makedirs(directory)
                
            with open(filepath, 'w') as f:
                if pretty:
                    json.dump(data, f, indent=2)
                else:
                    json.dump(data, f)
            return True
        except Exception as e:
            logging.error(f"Failed to save JSON file {filepath}: {str(e)}")
            return False

    @staticmethod
    def load_json(filepath: str) -> Dict[str, Any]:
        """
        Load data from JSON file
        
        Args:
            filepath (str): Path to JSON file
            
        Returns:
            dict: Loaded data or empty dict if failed
        """
        try:
            with open(filepath, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Failed to load JSON file {filepath}: {str(e)}")
            return {}