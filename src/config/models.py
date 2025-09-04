"""Configuration models for MCP Database Tools."""

from typing import Dict, Any, List, Optional, Union
from pydantic import BaseModel, Field, field_validator


class DatabaseSource(BaseModel):
    """Database source configuration."""
    name: str = Field(..., description="Source name")
    kind: str = Field(..., description="Database type (postgres, mysql, oracle)")
    host: str = Field(..., description="Database host")
    port: int = Field(..., description="Database port")
    database: str = Field(..., description="Database name")
    user: str = Field(..., description="Database username")
    password: str = Field(..., description="Database password")
    
    # Optional fields
    schema: Optional[str] = Field(None, description="Database schema (for Oracle)")
    service_name: Optional[str] = Field(None, description="Oracle service name")
    sid: Optional[str] = Field(None, description="Oracle SID")
    sslmode: Optional[str] = Field(None, description="SSL mode (for PostgreSQL)")
    charset: Optional[str] = Field(None, description="Character set (for MySQL)")
    timeout: Optional[int] = Field(None, description="Connection timeout in seconds")
    
    @field_validator('kind')
    @classmethod
    def validate_kind(cls, v):
        """Validate database kind."""
        valid_kinds = ['postgres', 'mysql', 'oracle']
        if v not in valid_kinds:
            raise ValueError(f'Database kind must be one of: {valid_kinds}')
        return v
    
    @field_validator('port')
    @classmethod
    def validate_port(cls, v):
        """Validate port number."""
        if not 1 <= v <= 65535:
            raise ValueError('Port must be between 1 and 65535')
        return v


class ToolParameter(BaseModel):
    """Tool parameter definition."""
    name: str = Field(..., description="Parameter name")
    type: str = Field(..., description="Parameter type")
    description: str = Field(..., description="Parameter description")
    required: bool = Field(default=True, description="Whether parameter is required")
    default: Optional[Any] = Field(None, description="Default value")
    
    @field_validator('type')
    @classmethod
    def validate_type(cls, v):
        """Validate parameter type."""
        valid_types = ['string', 'integer', 'number', 'boolean', 'object', 'array']
        if v not in valid_types:
            raise ValueError(f'Parameter type must be one of: {valid_types}')
        return v


class DatabaseTool(BaseModel):
    """Database tool configuration."""
    name: str = Field(..., description="Tool name")
    kind: str = Field(..., description="Tool kind")
    source: str = Field(..., description="Source name this tool uses")
    description: str = Field(..., description="Tool description")
    
    # SQL-specific fields
    statement: Optional[str] = Field(None, description="SQL statement for SQL tools")
    
    # Parameters
    parameters: Optional[List[ToolParameter]] = Field(None, description="Tool parameters")
    
    # Additional configuration
    timeout: Optional[int] = Field(None, description="Execution timeout in seconds")
    max_rows: Optional[int] = Field(None, description="Maximum rows to return")
    
    @field_validator('kind')
    @classmethod
    def validate_kind(cls, v):
        """Validate tool kind."""
        valid_kinds = [
            'database-connection',
            'postgres-sql',
            'mysql-sql', 
            'oracle-sql',
            'generic-sql'
        ]
        if v not in valid_kinds:
            raise ValueError(f'Tool kind must be one of: {valid_kinds}')
        return v


class Toolset(BaseModel):
    """Toolset configuration."""
    name: str = Field(..., description="Toolset name")
    tools: List[str] = Field(..., description="List of tool names in this toolset")
    description: Optional[str] = Field(None, description="Toolset description")


class ConfigurationValidation(BaseModel):
    """Configuration validation result."""
    valid: bool = Field(..., description="Whether configuration is valid")
    errors: List[str] = Field(default_factory=list, description="Validation errors")
    warnings: List[str] = Field(default_factory=list, description="Validation warnings")
    
    def add_error(self, error: str):
        """Add a validation error."""
        self.errors.append(error)
        self.valid = False
    
    def add_warning(self, warning: str):
        """Add a validation warning."""
        self.warnings.append(warning)
    
    def is_valid(self) -> bool:
        """Check if configuration is valid."""
        return self.valid and len(self.errors) == 0
