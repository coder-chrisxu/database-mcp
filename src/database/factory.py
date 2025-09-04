"""Factory for creating database strategies."""

from typing import Dict, Type
from database.db_engine import DatabaseEngine
from database.postgres_engine import PostgreSQLEngine
from database.mysql_engine import MySQLEngine
from database.oracle_engine import OracleEngine
from config.models import DatabaseSource


class DatabaseEngineFactory:
    """Factory for creating database-specific engine implementations."""
    
    _engines: Dict[str, Type[DatabaseEngine]] = {
        "postgres": PostgreSQLEngine,
        "postgresql": PostgreSQLEngine,
        "mysql": MySQLEngine,
        "oracle": OracleEngine
    }
    
    @classmethod
    def create_engine(cls, db_type: str, config: DatabaseSource) -> DatabaseEngine:
        """Create a database engine for the specified database type."""
        engine_class = cls._engines.get(db_type.lower())
        
        if not engine_class:
            raise ValueError(f"Unsupported database type: {db_type}")
        
        return engine_class(config)
    
    @classmethod
    def get_supported_types(cls) -> list[str]:
        """Get list of supported database types."""
        return list(cls._engines.keys())
    
    @classmethod
    def is_supported(cls, db_type: str) -> bool:
        """Check if a database type is supported."""
        return db_type.lower() in cls._engines
    
    @classmethod
    def register_engine(cls, db_type: str, engine_class: Type[DatabaseEngine]):
        """Register a new database engine."""
        cls._engines[db_type.lower()] = engine_class
