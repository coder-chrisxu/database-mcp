"""Oracle database strategy implementation."""

from typing import Dict, Any
from sqlalchemy import text
from database.db_engine import DatabaseEngine


class OracleEngine(DatabaseEngine):
    """Oracle-specific database engine implementation."""
    
    @property
    def db_type(self) -> str:
        return "oracle"
    
    @property
    def driver_name(self) -> str:
        return "oracle+oracledb"
    
    def build_connection_string(self) -> str:
        """Build Oracle connection string."""
        if hasattr(self.config, 'service_name') and self.config.service_name:
            connection_string = (
                f"oracle+oracledb://{self.config.user}:{self.config.password}"
                f"@{self.config.host}:{self.config.port}/?service_name={self.config.service_name}"
            )
        elif hasattr(self.config, 'sid') and self.config.sid:
            connection_string = (
                f"oracle+oracledb://{self.config.user}:{self.config.password}"
                f"@{self.config.host}:{self.config.port}/{self.config.sid}"
            )
        else:
            connection_string = (
                f"oracle+oracledb://{self.config.user}:{self.config.password}"
                f"@{self.config.host}:{self.config.port}/{self.config.database}"
            )
        
        return connection_string
    
    def get_version_query(self) -> str:
        return "SELECT * FROM v$version WHERE ROWNUM = 1"
    
    def get_size_query(self) -> str:
        return """
            SELECT 
                ROUND(SUM(bytes)/1024/1024, 2) as size_mb
            FROM user_segments
        """
    
    def get_tables_query(self) -> str:
        return """
            SELECT table_name 
            FROM user_tables 
            ORDER BY table_name
        """
    
    def get_schemas(self) -> Dict[str, Any]:
        """Get list of available schemas (users)."""
        try:
            engine = self.create_engine()
            
            with engine.connect() as connection:
                result = connection.execute(text("SELECT username FROM all_users ORDER BY username"))
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
            
            with engine.connect() as connection:
                # Get column information
                columns_query = """
                    SELECT 
                        column_name, 
                        data_type, 
                        nullable as is_nullable, 
                        data_default as column_default,
                        data_length as character_maximum_length
                    FROM user_tab_columns 
                    WHERE table_name = :table_name 
                    ORDER BY column_id
                """
                
                result = connection.execute(text(columns_query), {"table_name": table_name.upper()})
                columns = [dict(row) for row in result.fetchall()]
                
                # Get table size
                size_query = """
                    SELECT 
                        ROUND(SUM(bytes)/1024/1024, 2) AS size_mb,
                        SUM(bytes) AS size_bytes
                    FROM user_segments 
                    WHERE segment_name = :table_name
                """
                
                size_result = connection.execute(text(size_query), {"table_name": table_name.upper()})
                size_info = size_result.fetchone()
                
                return {
                    "success": True,
                    "table_name": table_name,
                    "owner": self.config.user,
                    "columns": columns,
                    "size_mb": size_info[0] if size_info else "Unknown",
                    "size_bytes": size_info[1] if size_info else 0
                }
                
        except Exception as e:
            return {"error": str(e)}
    
    def get_instance_info(self) -> Dict[str, Any]:
        """Get Oracle instance information."""
        try:
            engine = self.create_engine()
            
            with engine.connect() as connection:
                # Get instance name
                result = connection.execute(text("SELECT instance_name FROM v$instance"))
                instance_name = result.fetchone()
                
                # Get database name
                result = connection.execute(text("SELECT name FROM v$database"))
                database_name = result.fetchone()
                
                # Get startup time
                result = connection.execute(text("SELECT startup_time FROM v$instance"))
                startup_time = result.fetchone()
                
                return {
                    "success": True,
                    "instance_name": instance_name[0] if instance_name else "Unknown",
                    "database_name": database_name[0] if database_name else "Unknown",
                    "startup_time": startup_time[0] if startup_time else "Unknown"
                }
                
        except Exception as e:
            return {"error": str(e)}
    
    def get_tablespaces(self) -> Dict[str, Any]:
        """Get list of tablespaces."""
        try:
            engine = self.create_engine()
            
            with engine.connect() as connection:
                result = connection.execute(text("SELECT tablespace_name FROM user_tablespaces"))
                tablespaces = [row[0] for row in result.fetchall()]
                
                return {
                    "success": True,
                    "tablespaces": tablespaces,
                    "count": len(tablespaces)
                }
                
        except Exception as e:
            return {"error": str(e)}
    
    def explain_plan(self, sql: str) -> Dict[str, Any]:
        """Get Oracle execution plan using the same approach as Oracle SQL Developer."""
        try:
            engine = self.get_engine()
            if not engine:
                return {"error": "Database engine not available"}
            
            with engine.connect() as connection:
                # Oracle SQL Developer approach: Use GATHER_PLAN_STATISTICS hint
                # This is the standard way Oracle SQL Developer gets execution plans
                
                # Add the GATHER_PLAN_STATISTICS hint to the SQL
                # This tells Oracle to collect detailed execution statistics
                sql_with_hint = f"SELECT /*+ GATHER_PLAN_STATISTICS */ * FROM ({sql})"
                
                try:
                    # Execute the query with statistics collection
                    connection.execute(text(sql_with_hint))
                    
                    # Get the execution plan with actual statistics using DBMS_XPLAN.DISPLAY_CURSOR
                    # This is exactly what Oracle SQL Developer does
                    plan_query = """
                        SELECT plan_table_output 
                        FROM TABLE(DBMS_XPLAN.DISPLAY_CURSOR(NULL, NULL, 'ALLSTATS LAST'))
                    """
                    
                    result = connection.execute(text(plan_query))
                    plan_output = [row[0] for row in result.fetchall()]
                    
                    return {
                        "success": True,
                        "database_type": "oracle",
                        "sql": sql,
                        "execution_plan": plan_output,
                        "plan_type": "Oracle SQL Developer Style (GATHER_PLAN_STATISTICS + DBMS_XPLAN.DISPLAY_CURSOR)",
                        "note": "This shows actual execution statistics including rows processed, time, and cost."
                    }
                    
                except Exception as e:
                    # If the query execution fails, try a simpler approach
                    # This handles cases where the query might not be executable (e.g., DDL statements)
                    try:
                        # For non-executable queries, use EXPLAIN PLAN with DBMS_XPLAN.DISPLAY
                        explain_sql = f"EXPLAIN PLAN FOR {sql}"
                        connection.execute(text(explain_sql))
                        connection.commit()
                        
                        # Get the plan using DBMS_XPLAN.DISPLAY (this is also used by SQL Developer)
                        plan_query = """
                            SELECT plan_table_output 
                            FROM TABLE(DBMS_XPLAN.DISPLAY(NULL, NULL, 'BASIC'))
                        """
                        
                        result = connection.execute(text(plan_query))
                        plan_output = [row[0] for row in result.fetchall()]
                        
                        return {
                            "success": True,
                            "database_type": "oracle",
                            "sql": sql,
                            "execution_plan": plan_output,
                            "plan_type": "Oracle SQL Developer Style (EXPLAIN PLAN + DBMS_XPLAN.DISPLAY)",
                            "note": "This shows estimated execution plan. For actual statistics, the query must be executable."
                        }
                        
                    except Exception as explain_error:
                        return {
                            "error": f"Unable to get execution plan: {str(explain_error)}"
                        }
                
        except Exception as e:
            return {"error": str(e)}
