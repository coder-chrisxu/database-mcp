"""Configuration loader for MCP Database Tools."""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
from pydantic import BaseModel, Field, ValidationError

from .models import DatabaseSource, DatabaseTool, Toolset


class ConfigurationLoader:
    """Loads and validates configuration from YAML files."""
    
    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path or self._find_config_file()
        self.config_data: Dict[str, Any] = {}
        self.sources: Dict[str, DatabaseSource] = {}
        self.tools: Dict[str, DatabaseTool] = {}
        self.toolsets: Dict[str, Toolset] = {}
        
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
            self._parse_tools()
            self._parse_toolsets()
            
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
    
    def _parse_tools(self):
        """Parse database tools from configuration."""
        tools_data = self.config_data.get('tools', {})
        
        for tool_name, tool_data in tools_data.items():
            try:
                tool = DatabaseTool(
                    name=tool_name,
                    **tool_data
                )
                self.tools[tool_name] = tool
            except ValidationError as e:
                print(f"Warning: Invalid tool configuration for '{tool_name}': {e}")
                continue
    
    def _parse_toolsets(self):
        """Parse toolsets from configuration."""
        toolsets_data = self.config_data.get('toolsets', {})
        
        for toolset_name, tool_names in toolsets_data.items():
            try:
                toolset = Toolset(
                    name=toolset_name,
                    tools=tool_names
                )
                self.toolsets[toolset_name] = toolset
            except ValidationError as e:
                print(f"Warning: Invalid toolset configuration for '{toolset_name}': {e}")
                continue
    
    def get_source(self, source_name: str) -> Optional[DatabaseSource]:
        """Get a database source by name."""
        return self.sources.get(source_name)
    
    def get_tool(self, tool_name: str) -> Optional[DatabaseTool]:
        """Get a database tool by name."""
        return self.tools.get(tool_name)
    
    def get_toolset(self, toolset_name: str) -> Optional[Toolset]:
        """Get a toolset by name."""
        return self.toolsets.get(toolset_name)
    
    def get_tools_by_source(self, source_name: str) -> List[DatabaseTool]:
        """Get all tools that use a specific source."""
        return [tool for tool in self.tools.values() if tool.source == source_name]
    
    def get_tools_by_kind(self, kind: str) -> List[DatabaseTool]:
        """Get all tools of a specific kind."""
        return [tool for tool in self.tools.values() if tool.kind == kind]
    
    def list_sources(self) -> List[str]:
        """List all available source names."""
        return list(self.sources.keys())
    
    def list_tools(self) -> List[str]:
        """List all available tool names."""
        return list(self.tools.keys())
    
    def list_toolsets(self) -> List[str]:
        """List all available toolset names."""
        return list(self.toolsets.keys())
    
    def validate_config(self) -> List[str]:
        """Validate configuration and return list of errors."""
        errors = []
        
        # Check that all tools reference valid sources
        for tool_name, tool in self.tools.items():
            if tool.source not in self.sources:
                errors.append(f"Tool '{tool_name}' references unknown source '{tool.source}'")
        
        # Check that all toolsets reference valid tools
        for toolset_name, toolset in self.toolsets.items():
            for tool_name in toolset.tools:
                if tool_name not in self.tools:
                    errors.append(f"Toolset '{toolset_name}' references unknown tool '{tool_name}'")
        
        return errors
    
    def reload(self) -> bool:
        """Reload configuration from file."""
        return self.load_config()
    
    def get_config_summary(self) -> Dict[str, Any]:
        """Get a summary of the loaded configuration."""
        return {
            "config_file": self.config_path,
            "sources_count": len(self.sources),
            "tools_count": len(self.tools),
            "toolsets_count": len(self.toolsets),
            "sources": list(self.sources.keys()),
            "tools": list(self.tools.keys()),
            "toolsets": list(self.toolsets.keys())
        }


# Global configuration loader instance
config_loader = ConfigurationLoader()
