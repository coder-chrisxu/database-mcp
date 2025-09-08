"""Configuration models for MCP Database Tools."""

from typing import Optional
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
    db_schema: Optional[str] = Field(None, description="Database schema (for Oracle)")
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