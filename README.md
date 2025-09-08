# Database MCP Server

A Model Context Protocol (MCP) server for database operations with multi-database support.

## Features

- FastMCP-based server with stdio, HTTP, and SSE transport support
- Multi-database engine architecture supporting PostgreSQL, MySQL, and Oracle
- Database connection manager for handling multiple active connections
- Comprehensive database tools: connect, execute SQL, list tables, optimize queries
- Configuration-driven database sources via tools.yaml

## Usage

```bash
./start_server.sh --transport stdio
./start_server.sh --transport http --host 0.0.0.0 --port 8000
./start_server.sh --transport sse --host 0.0.0.0 --port 8000
```

## Configuration

Edit `config/tools.yaml` to configure your database sources.
