"""MySQL database strategy implementation."""

from typing import Dict, Any
from sqlalchemy import text
from database.db_engine import DatabaseEngine


class MySQLEngine(DatabaseEngine):
    """MySQL-specific database engine implementation."""
    
    @property
    def db_type(self) -> str:
        return "mysql"
    
    @property
    def driver_name(self) -> str:
        return "mysql"
    
    def build_connection_string(self) -> str:
        """Build MySQL connection string."""
        connection_string = (
            f"mysql://{self.config.user}:{self.config.password}"
            f"@{self.config.host}:{self.config.port}/{self.config.database}"
        )
        
        # Add charset if specified
        if hasattr(self.config, 'charset') and self.config.charset:
            connection_string += f"?charset={self.config.charset}"
        
        # Add SSL mode if specified
        if hasattr(self.config, 'sslmode') and self.config.sslmode:
            if '?' in connection_string:
                connection_string += f"&ssl_mode={self.config.sslmode}"
            else:
                connection_string += f"?ssl_mode={self.config.sslmode}"
        
        return connection_string
    
    def get_version_query(self) -> str:
        return "SELECT VERSION() as version"
    
    def get_size_query(self) -> str:
        return """
            SELECT 
                ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) AS size_mb
            FROM information_schema.tables 
            WHERE table_schema = DATABASE()
        """
    
    def get_tables_query(self) -> str:
        return """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = DATABASE() 
            ORDER BY table_name
        """
    
    def get_databases(self) -> Dict[str, Any]:
        """Get list of available databases."""
        try:
            engine = self.create_engine()
            
            with engine.connect() as connection:
                result = connection.execute(text("SHOW DATABASES"))
                databases = [row[0] for row in result.fetchall()]
                
                return {
                    "success": True,
                    "databases": databases,
                    "count": len(databases)
                }
                
        except Exception as e:
            return {"error": str(e)}
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get detailed information about a specific table."""
        try:
            engine = self.create_engine()
            
            with engine.connect() as connection:
                # Get column information
                columns_query = """
                    SELECT 
                        COLUMN_NAME as column_name,
                        DATA_TYPE as data_type,
                        IS_NULLABLE as is_nullable,
                        COLUMN_DEFAULT as column_default,
                        CHARACTER_MAXIMUM_LENGTH as character_maximum_length
                    FROM information_schema.columns 
                    WHERE table_schema = DATABASE() AND table_name = %s 
                    ORDER BY ordinal_position
                """
                
                result = connection.execute(text(columns_query), (table_name,))
                columns = [dict(row) for row in result.fetchall()]
                
                # Get table size
                size_query = """
                    SELECT 
                        ROUND((data_length + index_length) / 1024 / 1024, 2) AS size_mb,
                        (data_length + index_length) AS size_bytes
                    FROM information_schema.tables 
                    WHERE table_schema = DATABASE() AND table_name = %s
                """
                
                size_result = connection.execute(text(size_query), (table_name,))
                size_info = size_result.fetchone()
                
                return {
                    "success": True,
                    "table_name": table_name,
                    "database": self.config.database,
                    "columns": columns,
                    "size_mb": size_info[0] if size_info else "Unknown",
                    "size_bytes": size_info[1] if size_info else 0
                }
                
        except Exception as e:
            return {"error": str(e)}
    
    def get_status(self) -> Dict[str, Any]:
        """Get MySQL server status information."""
        try:
            engine = self.create_engine()
            
            with engine.connect() as connection:
                result = connection.execute(text("SHOW STATUS LIKE 'Uptime'"))
                uptime = result.fetchone()
                
                result = connection.execute(text("SHOW VARIABLES LIKE 'max_connections'"))
                max_connections = result.fetchone()
                
                result = connection.execute(text("SHOW PROCESSLIST"))
                active_connections = len(result.fetchall())
                
                return {
                    "success": True,
                    "uptime": uptime[1] if uptime else "Unknown",
                    "max_connections": max_connections[1] if max_connections else "Unknown",
                    "active_connections": active_connections
                }
                
        except Exception as e:
            return {"error": str(e)}
    
    def explain_plan(self, sql: str) -> Dict[str, Any]:
        """Get MySQL execution plan using EXPLAIN."""
        try:
            engine = self.get_engine()
            if not engine:
                return {"error": "Database engine not available"}
            
            with engine.connect() as connection:
                # Use EXPLAIN FORMAT=JSON for detailed plan
                explain_sql = f"EXPLAIN FORMAT=JSON {sql}"
                result = connection.execute(text(explain_sql))
                plan_data = result.fetchone()[0]
                
                return {
                    "success": True,
                    "database_type": "mysql",
                    "sql": sql,
                    "execution_plan": plan_data,
                    "plan_type": "EXPLAIN FORMAT=JSON"
                }
                
        except Exception as e:
            return {"error": str(e)}
