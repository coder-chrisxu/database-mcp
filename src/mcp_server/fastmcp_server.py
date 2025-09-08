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
        def explain_plan(connection_id: str, sql: str) -> str:
            """
            Get execution plan for a SQL query on a database connection.
            
            Args:
                connection_id: ID of the database connection
                sql: SQL query to analyze
                
            Returns:
                Execution plan details in JSON format
            """
            try:
                result = connection_manager.explain_plan(connection_id, sql)
                return json.dumps(result, indent=2)
                
            except Exception as e:
                logger.error(f"Error getting execution plan: {e}")
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
