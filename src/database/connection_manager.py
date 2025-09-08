"""Enhanced database connection manager with connection tracking."""

import logging
import uuid
from typing import Dict, Optional, List, Any
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool

from config.models import DatabaseSource
from database.factory import DatabaseEngineFactory

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Represents a database connection with metadata."""
    
    def __init__(self, connection_id: str, source: DatabaseSource, engine: Engine):
        self.connection_id = connection_id
        self.source = source
        self.engine = engine
        self.created_at = datetime.now()
        self.last_used = datetime.now()
        self.is_active = True
    
    def update_last_used(self):
        """Update the last used timestamp."""
        self.last_used = datetime.now()
    
    def close(self):
        """Close the connection."""
        try:
            self.engine.dispose()
            self.is_active = False
            logger.info(f"Closed connection {self.connection_id}")
        except Exception as e:
            logger.error(f"Error closing connection {self.connection_id}: {e}")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert connection to dictionary."""
        return {
            "connection_id": self.connection_id,
            "source_name": self.source.name,
            "database_type": self.source.kind,
            "host": self.source.host,
            "port": self.source.port,
            "database": self.source.database,
            "user": self.source.user,
            "created_at": self.created_at.isoformat(),
            "last_used": self.last_used.isoformat(),
            "is_active": self.is_active
        }


class EnhancedConnectionManager:
    """Enhanced connection manager with connection tracking and multi-database support."""
    
    def __init__(self):
        self.connections: Dict[str, DatabaseConnection] = {}
        self.engine_factory = DatabaseEngineFactory()
    
    def create_connection(self, source: DatabaseSource) -> str:
        """Create a new database connection and return its ID."""
        try:
            # Create database engine using factory
            db_engine = self.engine_factory.create_engine(source.kind, source)
            engine = db_engine.create_engine()
            
            # Test the connection
            if not db_engine.test_connection():
                raise Exception(f"Failed to connect to {source.kind} database at {source.host}:{source.port}")
            
            # Generate unique connection ID
            connection_id = str(uuid.uuid4())
            
            # Create connection object
            connection = DatabaseConnection(connection_id, source, engine)
            self.connections[connection_id] = connection
            
            logger.info(f"Created connection {connection_id} for {source.name} ({source.kind})")
            return connection_id
            
        except Exception as e:
            logger.error(f"Error creating connection for {source.name}: {e}")
            raise
    
    def get_connection(self, connection_id: str) -> Optional[DatabaseConnection]:
        """Get a connection by ID."""
        connection = self.connections.get(connection_id)
        if connection and connection.is_active:
            connection.update_last_used()
            return connection
        return None
    
    def list_connections(self) -> List[Dict[str, Any]]:
        """List all active connections."""
        return [conn.to_dict() for conn in self.connections.values() if conn.is_active]
    
    def close_connection(self, connection_id: str) -> bool:
        """Close a specific connection."""
        connection = self.connections.get(connection_id)
        if connection:
            connection.close()
            del self.connections[connection_id]
            return True
        return False
    
    def close_all_connections(self):
        """Close all connections."""
        for connection in self.connections.values():
            connection.close()
        self.connections.clear()
        logger.info("All database connections closed")
    
    def execute_sql(self, connection_id: str, sql: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute SQL on a specific connection."""
        connection = self.get_connection(connection_id)
        if not connection:
            return {"error": f"Connection {connection_id} not found or inactive"}
        
        try:
            # Use the existing engine directly
            result = connection.engine.execute_query(sql, params)
            return result
            
        except Exception as e:
            logger.error(f"Error executing SQL on connection {connection_id}: {e}")
            return {"error": str(e)}
    
    def list_tables(self, connection_id: str) -> Dict[str, Any]:
        """List tables for a specific connection."""
        connection = self.get_connection(connection_id)
        if not connection:
            return {"error": f"Connection {connection_id} not found or inactive"}
        
        try:
            # Use the existing engine directly
            result = connection.engine.list_tables()
            return result
            
        except Exception as e:
            logger.error(f"Error listing tables for connection {connection_id}: {e}")
            return {"error": str(e)}
    
    def get_database_info(self, connection_id: str) -> Dict[str, Any]:
        """Get database info for a specific connection."""
        connection = self.get_connection(connection_id)
        if not connection:
            return {"error": f"Connection {connection_id} not found or inactive"}
        
        try:
            # Use the existing engine directly
            result = connection.engine.get_database_info()
            return result
            
        except Exception as e:
            logger.error(f"Error getting database info for connection {connection_id}: {e}")
            return {"error": str(e)}
    
    def explain_plan(self, connection_id: str, sql: str) -> Dict[str, Any]:
        """Get execution plan for a SQL query on a specific connection."""
        connection = self.get_connection(connection_id)
        if not connection:
            return {"error": f"Connection {connection_id} not found or inactive"}
        
        try:
            # Use the existing engine directly
            result = connection.engine.explain_plan(sql)
            return result
            
        except Exception as e:
            logger.error(f"Error getting execution plan for connection {connection_id}: {e}")
            return {"error": str(e)}
    
    def cleanup_inactive_connections(self, max_age_hours: int = 24):
        """Clean up connections that haven't been used for a specified time."""
        from datetime import timedelta
        
        cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
        inactive_connections = []
        
        for conn_id, connection in self.connections.items():
            if connection.last_used < cutoff_time:
                inactive_connections.append(conn_id)
        
        for conn_id in inactive_connections:
            self.close_connection(conn_id)
            logger.info(f"Cleaned up inactive connection {conn_id}")
        
        return len(inactive_connections)


# Global connection manager instance
connection_manager = EnhancedConnectionManager()
