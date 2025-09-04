"""PostgreSQL database strategy implementation."""

from typing import Dict, Any
from sqlalchemy import text
from database.db_engine import DatabaseEngine


class PostgreSQLEngine(DatabaseEngine):
    """PostgreSQL-specific database engine implementation."""
    
    @property
    def db_type(self) -> str:
        return "postgres"
    
    @property
    def driver_name(self) -> str:
        return "postgresql"
    
    def build_connection_string(self) -> str:
        """Build PostgreSQL connection string."""
        connection_string = (
            f"postgresql://{self.config.user}:{self.config.password}"
            f"@{self.config.host}:{self.config.port}/{self.config.database}"
        )
        
        # Add SSL mode if specified
        if hasattr(self.config, 'sslmode') and self.config.sslmode:
            connection_string += f"?sslmode={self.config.sslmode}"
        
        # Add schema if specified
        if hasattr(self.config, 'schema') and self.config.schema:
            if '?' in connection_string:
                connection_string += f"&options=-csearch_path%3D{self.config.schema}"
            else:
                connection_string += f"?options=-csearch_path%3D{self.config.schema}"
        
        return connection_string
    
    def get_version_query(self) -> str:
        return "SELECT version()"
    
    def get_size_query(self) -> str:
        return """
            SELECT pg_size_pretty(pg_database_size(current_database())) as size
        """
    
    def get_tables_query(self) -> str:
        schema = getattr(self.config, 'schema', 'public')
        return f"""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = '{schema}' 
            ORDER BY table_name
        """
    
    def get_schemas(self) -> Dict[str, Any]:
        """Get list of available schemas."""
        try:
            engine = self.create_engine()
            
            with engine.connect() as connection:
                result = connection.execute(text(
                    "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name"
                ))
                schemas = [row[0] for row in result.fetchall()]
                
                return {
                    "success": True,
                    "schemas": schemas,
                    "count": len(schemas)
                }
                
        except Exception as e:
            return {"error": str(e)}
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get detailed information about a specific table."""
        try:
            engine = self.create_engine()
            schema = getattr(self.config, 'schema', 'public')
            
            with engine.connect() as connection:
                # Get column information
                columns_query = """
                    SELECT 
                        column_name, 
                        data_type, 
                        is_nullable, 
                        column_default,
                        character_maximum_length
                    FROM information_schema.columns 
                    WHERE table_schema = %s AND table_name = %s 
                    ORDER BY ordinal_position
                """
                
                result = connection.execute(text(columns_query), (schema, table_name))
                columns = [dict(row) for row in result.fetchall()]
                
                # Get table size
                size_query = """
                    SELECT 
                        pg_size_pretty(pg_total_relation_size(%s)) as size,
                        pg_total_relation_size(%s) as size_bytes
                    FROM pg_tables 
                    WHERE schemaname = %s AND tablename = %s
                """
                
                size_result = connection.execute(text(size_query), (table_name, table_name, schema, table_name))
                size_info = size_result.fetchone()
                
                return {
                    "success": True,
                    "table_name": table_name,
                    "schema": schema,
                    "columns": columns,
                    "size": size_info[0] if size_info else "Unknown",
                    "size_bytes": size_info[1] if size_info else 0
                }
                
        except Exception as e:
            return {"error": str(e)}
