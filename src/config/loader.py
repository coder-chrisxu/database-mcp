"""Configuration loader for MCP Database Tools."""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, List, Optional
from pydantic import ValidationError

from .models import DatabaseSource


class ConfigurationLoader:
    """Loads and validates configuration from YAML files."""
    
    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path or self._find_config_file()
        self.config_data: Dict[str, Any] = {}
        self.sources: Dict[str, DatabaseSource] = {}
        
        if self.config_path:
            self.load_config()
    
    def _find_config_file(self) -> Optional[str]:
        """Find configuration file in common locations."""
        search_paths = [
            "tools.yaml",
            "config/tools.yaml",
            "tools.yml",
            "config/tools.yml",
            os.path.expanduser("~/.config/database-mcp/tools.yaml"),
            "/etc/database-mcp/tools.yaml"
        ]
        
        for path in search_paths:
            if os.path.exists(path):
                return path
        
        return None
    
    def load_config(self, config_path: Optional[str] = None) -> bool:
        """Load configuration from YAML file."""
        if config_path:
            self.config_path = config_path
        
        if not self.config_path or not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        try:
            with open(self.config_path, 'r', encoding='utf-8') as file:
                self.config_data = yaml.safe_load(file)
            
            self._parse_sources()
            
            return True
            
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in configuration file: {e}")
        except Exception as e:
            raise RuntimeError(f"Error loading configuration: {e}")
    
    def _parse_sources(self):
        """Parse database sources from configuration."""
        sources_data = self.config_data.get('sources', {})
        
        for source_name, source_data in sources_data.items():
            try:
                source = DatabaseSource(
                    name=source_name,
                    **source_data
                )
                self.sources[source_name] = source
            except ValidationError as e:
                print(f"Warning: Invalid source configuration for '{source_name}': {e}")
                continue
    
    def get_source(self, source_name: str) -> Optional[DatabaseSource]:
        """Get a database source by name."""
        return self.sources.get(source_name)
    
    def list_sources(self) -> List[str]:
        """List all available source names."""
        return list(self.sources.keys())
    
    def validate_config(self) -> List[str]:
        """Validate configuration and return list of errors."""
        errors = []
        
        # Check that all sources have valid configurations
        for source_name, source in self.sources.items():
            if not source.name:
                errors.append(f"Source '{source_name}' has no name")
        
        return errors
    
    def reload(self) -> bool:
        """Reload configuration from file."""
        return self.load_config()
    
    def get_config_summary(self) -> Dict[str, Any]:
        """Get a summary of the loaded configuration."""
        return {
            "config_file": self.config_path,
            "sources_count": len(self.sources),
            "sources": list(self.sources.keys())
        }


# Global configuration loader instance
config_loader = ConfigurationLoader()