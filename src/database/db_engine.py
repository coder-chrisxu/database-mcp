"""Base database strategy interface."""

import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool

# Settings removed - using default values
from config.models import DatabaseSource

logger = logging.getLogger(__name__)


class DatabaseEngine(ABC):
    """Abstract base class for database-specific engine implementations."""
    
    def __init__(self, config: DatabaseSource):
        self.config = config
        self.engine: Optional[Engine] = None
    
    @property
    @abstractmethod
    def db_type(self) -> str:
        """Return the database type identifier."""
        pass
    
    @property
    @abstractmethod
    def driver_name(self) -> str:
        """Return the SQLAlchemy driver name."""
        pass
    
    @abstractmethod
    def build_connection_string(self) -> str:
        """Build the database-specific connection string."""
        pass
    
    @abstractmethod
    def get_version_query(self) -> str:
        """Get the query to retrieve database version."""
        pass
    
    @abstractmethod
    def get_size_query(self) -> str:
        """Get the query to retrieve database size."""
        pass
    
    @abstractmethod
    def get_tables_query(self) -> str:
        """Get the query to list tables."""
        pass
    
    def create_engine(self) -> Engine:
        """Create and return a SQLAlchemy engine."""
        if self.engine is None:
            connection_string = self.build_connection_string()
            
            self.engine = create_engine(
                connection_string,
                poolclass=QueuePool,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,
                echo=False,
                pool_recycle=3600,
                pool_timeout=30
            )
            
            logger.info(f"Created new {self.db_type} engine for {self.config.host}:{self.config.port}")
        
        return self.engine
    
    def test_connection(self) -> bool:
        """Test the database connection."""
        try:
            engine = self.create_engine()
            with engine.connect() as connection:
                result = connection.execute(text("SELECT 1"))
                result.fetchone()
                logger.info(f"Successfully connected to {self.db_type} database at {self.config.host}:{self.config.port}")
                return True
                
        except Exception as e:
            logger.error(f"Failed to connect to {self.db_type} database at {self.config.host}:{self.config.port}: {e}")
            return False
    
    def execute_query(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute a SQL query and return results."""
        try:
            engine = self.create_engine()
            
            with engine.connect() as connection:
                if params:
                    result = connection.execute(text(sql), params)
                else:
                    result = connection.execute(text(sql))
                
                # Handle different result types
                if result.returns_rows:
                    columns = list(result.keys())
                    rows = [dict(zip(columns, row)) for row in result.fetchall()]
                    return {
                        "success": True,
                        "columns": columns,
                        "rows": rows,
                        "row_count": len(rows)
                    }
                else:
                    return {
                        "success": True,
                        "message": "Query executed successfully",
                        "row_count": result.rowcount
                    }
                    
        except Exception as e:
            logger.error(f"Error executing query on {self.db_type}: {e}")
            return {"error": str(e)}
    
    def get_database_info(self) -> Dict[str, Any]:
        """Get general information about the database."""
        try:
            engine = self.create_engine()
            
            with engine.connect() as connection:
                version_result = connection.execute(text(self.get_version_query()))
                version = version_result.fetchone()
                
                size_result = connection.execute(text(self.get_size_query()))
                size = size_result.fetchone()
                
                return {
                    "success": True,
                    "database_type": self.db_type,
                    "version": version[0] if version else "Unknown",
                    "size": size[0] if size else "Unknown"
                }
                
        except Exception as e:
            logger.error(f"Error getting database info for {self.db_type}: {e}")
            return {"error": str(e)}
    
    def list_tables(self) -> Dict[str, Any]:
        """List all tables in the database."""
        try:
            engine = self.create_engine()
            
            with engine.connect() as connection:
                result = connection.execute(text(self.get_tables_query()))
                tables = [row[0] for row in result.fetchall()]
                
                return {
                    "success": True,
                    "tables": tables,
                    "count": len(tables)
                }
                
        except Exception as e:
            logger.error(f"Error listing tables for {self.db_type}: {e}")
            return {"error": str(e)}
    
    def close_engine(self):
        """Close and dispose of the engine."""
        if self.engine:
            try:
                self.engine.dispose()
                self.engine = None
                logger.info(f"Closed {self.db_type} engine")
            except Exception as e:
                logger.error(f"Error closing {self.db_type} engine: {e}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close_engine()
