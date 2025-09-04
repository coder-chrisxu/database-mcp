"""FastMCP-based database server implementation."""

import logging
import json
from typing import Dict, Any, Optional, List
from fastmcp import FastMCP

from config.loader import config_loader
from database.connection_manager import connection_manager

logger = logging.getLogger(__name__)


class DatabaseMCPServer:
    """FastMCP-based database MCP server."""
    
    def __init__(self, name: str = "Database MCP Server"):
        self.mcp = FastMCP(name)
        self._register_tools()
    
    def _register_tools(self):
        """Register all database tools with FastMCP."""
        
        @self.mcp.tool()
        def connect_db(source_name: str) -> str:
            """
            Connect to a database using a configured source.
            
            Args:
                source_name: Name of the database source from tools.yaml
                
            Returns:
                Connection ID if successful, error message if failed
            """
            try:
                # Get source configuration
                source = config_loader.get_source(source_name)
                if not source:
                    return json.dumps({
                        "success": False,
                        "error": f"Source '{source_name}' not found in configuration"
                    })
                
                # Create connection
                connection_id = connection_manager.create_connection(source)
                
                return json.dumps({
                    "success": True,
                    "connection_id": connection_id,
                    "source_name": source_name,
                    "database_type": source.kind,
                    "host": source.host,
                    "port": source.port,
                    "database": source.database
                })
                
            except Exception as e:
                logger.error(f"Error connecting to database: {e}")
                return json.dumps({
                    "success": False,
                    "error": str(e)
                })
        
        @self.mcp.tool()
        def execute_sql(connection_id: str, sql: str, parameters: Optional[Dict[str, Any]] = None) -> str:
            """
            Execute a SQL query on a database connection.
            
            Args:
                connection_id: ID of the database connection
                sql: SQL query to execute
                parameters: Optional query parameters
                
            Returns:
                Query results in JSON format
            """
            try:
                if parameters is None:
                    parameters = {}
                
                result = connection_manager.execute_sql(connection_id, sql, parameters)
                return json.dumps(result, indent=2)
                
            except Exception as e:
                logger.error(f"Error executing SQL: {e}")
                return json.dumps({
                    "success": False,
                    "error": str(e)
                })
        
        @self.mcp.tool()
        def close_connection(connection_id: str) -> str:
            """
            Close a database connection.
            
            Args:
                connection_id: ID of the connection to close
                
            Returns:
                Success status
            """
            try:
                success = connection_manager.close_connection(connection_id)
                return json.dumps({
                    "success": success,
                    "message": f"Connection {connection_id} {'closed' if success else 'not found'}"
                })
                
            except Exception as e:
                logger.error(f"Error closing connection: {e}")
                return json.dumps({
                    "success": False,
                    "error": str(e)
                })
        
        @self.mcp.tool()
        def list_connections() -> str:
            """
            List all active database connections.
            
            Returns:
                List of active connections in JSON format
            """
            try:
                connections = connection_manager.list_connections()
                return json.dumps({
                    "success": True,
                    "connections": connections,
                    "count": len(connections)
                }, indent=2)
                
            except Exception as e:
                logger.error(f"Error listing connections: {e}")
                return json.dumps({
                    "success": False,
                    "error": str(e)
                })
        
        @self.mcp.tool()
        def optimize_sql(sql: str, database_type: str = "postgres") -> str:
            """
            Optimize a SQL query for better performance.
            
            Args:
                sql: SQL query to optimize
                database_type: Type of database (postgres, mysql, oracle)
                
            Returns:
                Optimized SQL query and suggestions
            """
            try:
                # Basic SQL optimization suggestions
                suggestions = []
                optimized_sql = sql.strip()
                
                # Convert to lowercase for analysis
                sql_lower = optimized_sql.lower()
                
                # Check for common optimization opportunities
                if "select *" in sql_lower:
                    suggestions.append("Consider specifying column names instead of using SELECT *")
                
                if "order by" in sql_lower and "limit" not in sql_lower:
                    suggestions.append("Consider adding LIMIT clause when using ORDER BY")
                
                if "where" not in sql_lower and "join" not in sql_lower:
                    suggestions.append("Consider adding WHERE clause to filter results")
                
                # Database-specific optimizations
                if database_type.lower() == "postgres":
                    if "ilike" in sql_lower:
                        suggestions.append("Consider using LIKE with proper indexing instead of ILIKE for better performance")
                    if "distinct" in sql_lower:
                        suggestions.append("Consider if DISTINCT is necessary - it can be expensive")
                
                elif database_type.lower() == "mysql":
                    if "limit" not in sql_lower and "order by" in sql_lower:
                        suggestions.append("MySQL benefits from LIMIT clauses with ORDER BY")
                    if "group by" in sql_lower:
                        suggestions.append("Ensure GROUP BY columns are indexed")
                
                elif database_type.lower() == "oracle":
                    if "rownum" in sql_lower:
                        suggestions.append("Consider using ROW_NUMBER() window function instead of ROWNUM")
                    if "nvl" in sql_lower:
                        suggestions.append("Consider using COALESCE instead of NVL for better portability")
                
                return json.dumps({
                    "success": True,
                    "original_sql": sql,
                    "optimized_sql": optimized_sql,
                    "database_type": database_type,
                    "suggestions": suggestions,
                    "optimization_score": max(0, 100 - len(suggestions) * 20)
                }, indent=2)
                
            except Exception as e:
                logger.error(f"Error optimizing SQL: {e}")
                return json.dumps({
                    "success": False,
                    "error": str(e)
                })
        
        @self.mcp.tool()
        def list_tables(connection_id: str) -> str:
            """
            List all tables in a database connection.
            
            Args:
                connection_id: ID of the database connection
                
            Returns:
                List of tables in JSON format
            """
            try:
                result = connection_manager.list_tables(connection_id)
                return json.dumps(result, indent=2)
                
            except Exception as e:
                logger.error(f"Error listing tables: {e}")
                return json.dumps({
                    "success": False,
                    "error": str(e)
                })
        
        @self.mcp.tool()
        def get_database_info(connection_id: str) -> str:
            """
            Get information about a database connection.
            
            Args:
                connection_id: ID of the database connection
                
            Returns:
                Database information in JSON format
            """
            try:
                result = connection_manager.get_database_info(connection_id)
                return json.dumps(result, indent=2)
                
            except Exception as e:
                logger.error(f"Error getting database info: {e}")
                return json.dumps({
                    "success": False,
                    "error": str(e)
                })
        
        @self.mcp.tool()
        def list_sources() -> str:
            """
            List all available database sources from configuration.
            
            Returns:
                List of available sources in JSON format
            """
            try:
                sources = []
                for source_name, source in config_loader.sources.items():
                    sources.append({
                        "name": source_name,
                        "kind": source.kind,
                        "host": source.host,
                        "port": source.port,
                        "database": source.database,
                        "user": source.user
                    })
                
                return json.dumps({
                    "success": True,
                    "sources": sources,
                    "count": len(sources)
                }, indent=2)
                
            except Exception as e:
                logger.error(f"Error listing sources: {e}")
                return json.dumps({
                    "success": False,
                    "error": str(e)
                })
    
    def run_stdio(self):
        """Run the MCP server using stdio transport."""
        logger.info("Starting Database MCP Server with stdio transport")
        self.mcp.run_stdio_async()
    
    def run_http(self, host: str = "0.0.0.0", port: int = 8000):
        """Run the MCP server using HTTP transport."""
        logger.info(f"Starting Database MCP Server with HTTP transport on {host}:{port}")
        self.mcp.run_http_async(host=host, port=port)
    
    def run_sse(self, host: str = "0.0.0.0", port: int = 8000):
        """Run the MCP server using Server-Sent Events transport."""
        logger.info(f"Starting Database MCP Server with SSE transport on {host}:{port}")
        self.mcp.run_sse_async(host=host, port=port)
    
    def get_fastapi_app(self):
        """Get the underlying FastAPI app for custom deployment."""
        return self.mcp.http_app


# Global server instance
server = DatabaseMCPServer()
