#!/usr/bin/env python3
"""Main entry point for the Database MCP Server."""

import argparse
import logging
import sys
from pathlib import Path

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

from config.loader import config_loader
from mcp_server.fastmcp_server import server


def setup_logging():
    """Setup logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("database-mcp.log")
        ]
    )


def run_fastmcp_server(transport: str, host: str, port: int):
    """Run the FastMCP server with specified transport."""
    try:
        if transport == "stdio":
            logging.info("Starting FastMCP Server with stdio transport...")
            server.run_stdio()
        elif transport == "http":
            logging.info(f"Starting FastMCP Server with HTTP transport on {host}:{port}...")
            server.run_http(host=host, port=port)
        elif transport == "sse":
            logging.info(f"Starting FastMCP Server with SSE transport on {host}:{port}...")
            server.run_sse(host=host, port=port)
        else:
            raise ValueError(f"Unsupported transport: {transport}")
    except KeyboardInterrupt:
        logging.info("FastMCP Server stopped by user")
    except Exception as e:
        logging.error(f"Error running FastMCP Server: {e}")
        raise


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Database MCP Server")
    parser.add_argument(
        "--transport",
        choices=["stdio", "http", "sse"],
        default="stdio",
        help="Transport method (default: stdio)"
    )
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Host to bind to (default: 0.0.0.0)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port to bind to (default: 8000)"
    )
    parser.add_argument(
        "--config",
        type=str,
        help="Path to configuration file (tools.yaml)"
    )
    parser.add_argument(
        "--validate-config",
        action="store_true",
        help="Validate configuration and exit"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging()
    
    logging.info(f"Starting Database MCP Server")
    logging.info(f"Transport: {args.transport}")
    if args.transport != "stdio":
        logging.info(f"Configuration: {args.host}:{args.port}")
    
    # Load configuration file if specified
    if args.config:
        try:
            config_loader.load_config(args.config)
            logging.info(f"Configuration loaded from: {args.config}")
        except Exception as e:
            logging.error(f"Error loading configuration file: {e}")
            sys.exit(1)
    
    # Validate configuration if requested
    if args.validate_config:
        try:
            errors = config_loader.validate_config()
            if errors:
                logging.error("Configuration validation failed:")
                for error in errors:
                    logging.error(f"  - {error}")
                sys.exit(1)
            else:
                logging.info("Configuration validation passed")
                config_summary = config_loader.get_config_summary()
                logging.info(f"Configuration summary: {config_summary}")
                sys.exit(0)
        except Exception as e:
            logging.error(f"Error validating configuration: {e}")
            sys.exit(1)
    
    try:
        # Run FastMCP server with specified transport
        run_fastmcp_server(args.transport, args.host, args.port)
                
    except KeyboardInterrupt:
        logging.info("Server stopped by user")
    except Exception as e:
        logging.error(f"Server error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
