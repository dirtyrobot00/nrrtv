"""Configuration management module.

This module provides a singleton Config class that loads configuration from:
1. YAML files (config/config.yaml, config/sources.yaml)
2. Environment variables (.env file)

Environment variables override YAML values using ${VAR_NAME} syntax.
"""

import os
import re
from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from dotenv import load_dotenv


class ConfigError(Exception):
    """Raised when configuration is invalid or missing."""
    pass


class Config:
    """Singleton configuration manager.

    Loads configuration from YAML files and environment variables.
    Supports variable interpolation: ${VAR_NAME} in YAML is replaced
    with the value of environment variable VAR_NAME.

    Usage:
        config = Config()
        db_path = config.get("database.path")
        neo4j_uri = config.get("neo4j.uri")
    """

    _instance: Optional['Config'] = None
    _config: Dict[str, Any] = {}
    _sources: Dict[str, Any] = {}

    def __new__(cls) -> 'Config':
        """Ensure only one instance exists (singleton pattern)."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, config_dir: Optional[Path] = None, env_file: Optional[Path] = None):
        """Initialize configuration.

        Args:
            config_dir: Directory containing config.yaml and sources.yaml.
                       Defaults to PROJECT_ROOT/config
            env_file: Path to .env file. Defaults to PROJECT_ROOT/.env
        """
        # Only initialize once
        if self._initialized:
            return

        # Determine project root (parent of src/)
        self.project_root = Path(__file__).parent.parent.parent

        # Set config directory
        if config_dir is None:
            config_dir = self.project_root / "config"
        self.config_dir = Path(config_dir)

        # Load environment variables
        if env_file is None:
            env_file = self.project_root / ".env"
        if env_file.exists():
            load_dotenv(env_file)

        # Load configuration files
        self._load_config()
        self._load_sources()

        self._initialized = True

    def _load_config(self) -> None:
        """Load main configuration from config.yaml."""
        config_file = self.config_dir / "config.yaml"

        if not config_file.exists():
            raise ConfigError(f"Configuration file not found: {config_file}")

        with open(config_file, 'r', encoding='utf-8') as f:
            raw_config = yaml.safe_load(f)

        # Interpolate environment variables
        self._config = self._interpolate_env_vars(raw_config)

    def _load_sources(self) -> None:
        """Load data sources configuration from sources.yaml."""
        sources_file = self.config_dir / "sources.yaml"

        if not sources_file.exists():
            raise ConfigError(f"Sources file not found: {sources_file}")

        with open(sources_file, 'r', encoding='utf-8') as f:
            raw_sources = yaml.safe_load(f)

        # Interpolate environment variables
        self._sources = self._interpolate_env_vars(raw_sources)

    def _interpolate_env_vars(self, data: Any) -> Any:
        """Recursively replace ${VAR_NAME} with environment variable values.

        Args:
            data: Configuration data (dict, list, str, or other)

        Returns:
            Data with interpolated values
        """
        if isinstance(data, dict):
            return {k: self._interpolate_env_vars(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._interpolate_env_vars(item) for item in data]
        elif isinstance(data, str):
            return self._replace_env_var(data)
        else:
            return data

    def _replace_env_var(self, value: str) -> Any:
        """Replace ${VAR_NAME} with environment variable value.

        Args:
            value: String potentially containing ${VAR_NAME}

        Returns:
            Replaced value (str, int, float, or bool)
        """
        pattern = r'\$\{(\w+)\}'
        match = re.search(pattern, value)

        if not match:
            return value

        # If the entire string is just ${VAR}, return the env var value with type conversion
        if re.fullmatch(pattern, value):
            var_name = match.group(1)
            env_value = os.getenv(var_name)

            if env_value is None:
                raise ConfigError(f"Environment variable not found: {var_name}")

            # Type conversion
            return self._convert_type(env_value)

        # Otherwise, replace all ${VAR} occurrences in the string
        def replacer(match):
            var_name = match.group(1)
            env_value = os.getenv(var_name)
            if env_value is None:
                raise ConfigError(f"Environment variable not found: {var_name}")
            return env_value

        return re.sub(pattern, replacer, value)

    def _convert_type(self, value: str) -> Any:
        """Convert string value to appropriate type.

        Args:
            value: String value from environment variable

        Returns:
            Converted value (str, int, float, or bool)
        """
        # Boolean conversion
        if value.lower() in ('true', 'yes', '1'):
            return True
        if value.lower() in ('false', 'no', '0'):
            return False

        # Numeric conversion
        try:
            if '.' in value:
                return float(value)
            return int(value)
        except ValueError:
            pass

        # Return as string
        return value

    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value by dot-separated key.

        Args:
            key: Dot-separated key (e.g., "database.path", "neo4j.uri")
            default: Default value if key not found

        Returns:
            Configuration value

        Examples:
            >>> config = Config()
            >>> config.get("database.type")
            'sqlite'
            >>> config.get("neo4j.uri")
            'bolt://localhost:7687'
            >>> config.get("nonexistent.key", "default_value")
            'default_value'
        """
        keys = key.split('.')
        value = self._config

        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default

        return value

    def get_source(self, source_type: str, source_name: str) -> Optional[Dict[str, Any]]:
        """Get data source configuration.

        Args:
            source_type: Type of source ("pdf_sources" or "news_sources")
            source_name: Name of the source

        Returns:
            Source configuration dict or None

        Examples:
            >>> config = Config()
            >>> config.get_source("pdf_sources", "naver_finance_research")
            {'name': 'naver_finance_research', 'type': 'web_scrape', ...}
        """
        sources = self._sources.get(source_type, [])
        for source in sources:
            if source.get('name') == source_name:
                return source
        return None

    def get_all_sources(self, source_type: str, enabled_only: bool = True) -> list:
        """Get all data sources of a specific type.

        Args:
            source_type: Type of source ("pdf_sources" or "news_sources")
            enabled_only: If True, return only enabled sources

        Returns:
            List of source configurations
        """
        sources = self._sources.get(source_type, [])
        if enabled_only:
            return [s for s in sources if s.get('enabled', False)]
        return sources

    def get_company_ticker(self, company_name: str) -> Optional[str]:
        """Get ticker symbol for a company name.

        Args:
            company_name: Company name or alias

        Returns:
            Ticker symbol or None
        """
        companies = self._sources.get('company_tickers', [])

        for company in companies:
            # Exact name match
            if company.get('name') == company_name:
                return company.get('ticker')

            # Alias match
            aliases = company.get('aliases', [])
            if company_name in aliases:
                return company.get('ticker')

        return None

    def get_keywords(self, category: str) -> list:
        """Get keyword list for a category.

        Args:
            category: Keyword category (e.g., "investment_opinion", "risk_factors")

        Returns:
            List of keywords
        """
        keywords = self._sources.get('keywords', {})
        return keywords.get(category, [])

    @property
    def project_root(self) -> Path:
        """Get project root directory."""
        return self._project_root

    @project_root.setter
    def project_root(self, value: Path) -> None:
        """Set project root directory."""
        self._project_root = Path(value)

    def reload(self) -> None:
        """Reload configuration from files.

        Useful for picking up changes without restarting the application.
        """
        self._load_config()
        self._load_sources()


# Global configuration instance
_config = None


def get_config() -> Config:
    """Get the global configuration instance.

    Returns:
        Config singleton instance
    """
    global _config
    if _config is None:
        _config = Config()
    return _config
