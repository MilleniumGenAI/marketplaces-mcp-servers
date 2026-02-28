import asyncio
import json
import logging
import os
import sys
import traceback

import mcp.types as types
from mcp.server import NotificationOptions, Server
from mcp.server.models import InitializationOptions
from mcp.server.stdio import stdio_server

from providers.base import BaseMarketplaceProvider

def create_mcp_server(provider: BaseMarketplaceProvider) -> Server:
    server = Server(f"{provider.provider_name}-shopper")
    
    _log = logging.getLogger(f"{provider.provider_name}-shopper")
    _log.setLevel(logging.DEBUG if os.environ.get(f"{provider.provider_name.upper()}_DEBUG") else logging.INFO)
    if not _log.handlers:
        _h = logging.StreamHandler(sys.stderr)
        _log.addHandler(_h)

    @server.list_tools()
    async def handle_list_tools() -> list[types.Tool]:
        return [
            types.Tool(
                name=f"search_{provider.provider_name}",
                description=f"Search {provider.provider_name.upper()} via Apify actor. Hard cap: 10 items/request.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "User text query."},
                        "limit": {"type": "number", "description": "Optional. Max results to return. Hard cap: 10."},
                        "compact": {
                            "type": "boolean",
                            "description": "If true (default), return compact payload with top_products.",
                        },
                        "fresh_only": {
                            "type": "boolean",
                            "description": "If true, skip cache.",
                        },
                    },
                    "required": ["query"],
                },
            ),
            types.Tool(
                name=f"get_{provider.provider_name}_runtime_config",
                description="Return effective runtime configuration.",
                inputSchema={"type": "object", "properties": {}},
            ),
            types.Tool(
                name=f"get_{provider.provider_name}_provider_status",
                description="Return provider call status and counters.",
                inputSchema={"type": "object", "properties": {}},
            ),
        ]

    @server.call_tool()
    async def handle_call_tool(name: str, arguments: dict | None) -> list[types.TextContent]:
        try:
            args = arguments or {}
            if name == f"get_{provider.provider_name}_runtime_config":
                return [types.TextContent(type="text", text=json.dumps(provider.get_runtime_config(), ensure_ascii=False, indent=2))]
            if name == f"get_{provider.provider_name}_provider_status":
                return [types.TextContent(type="text", text=json.dumps(provider.get_provider_status(), ensure_ascii=False, indent=2))]

            if name == f"search_{provider.provider_name}":
                query = str(args.get("query") or "")
                limit = args.get("limit")
                limit = int(limit) if limit is not None else None
                compact = bool(args.get("compact", True))
                fresh_only = bool(args.get("fresh_only", False))
                
                if not query.strip():
                    return [types.TextContent(type="text", text=provider.build_tool_response("", [], "No query provided", price_status="error", compact=compact))]

                try:
                    products, meta = await provider.fetch_products(query, limit, fresh_only)
                    return [
                        types.TextContent(
                            type="text",
                            text=provider.build_tool_response(
                                query,
                                products,
                                None,
                                price_status=meta.get("price_status", "live"),
                                cache_age_sec=meta.get("cache_age_sec"),
                                source_total=meta.get("source_total"),
                                compact=compact,
                            )
                        )
                    ]
                except Exception as e:
                    return [types.TextContent(type="text", text=provider.build_tool_response(query, [], str(e), price_status="error", compact=compact))]
            
            return [types.TextContent(type="text", text=provider.build_tool_response("", [], f"Tool {name} not found", price_status="error"))]
        except Exception as exc:
            traceback.print_exc(file=sys.stderr)
            q = args.get("query", "") if isinstance(args, dict) else ""
            return [types.TextContent(type="text", text=provider.build_tool_response(str(q), [], str(exc), price_status="error"))]

    return server

async def run_server(provider: BaseMarketplaceProvider):
    # Try to load local env just in case
    env_path = os.path.join(os.path.dirname(os.path.abspath(sys.argv[0])), ".env")
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line or line.startswith("#"): continue
                if "=" in line:
                    k, v = line.split("=", 1)
                    if k.strip() not in os.environ:
                        os.environ[k.strip()] = v.strip()

    server = create_mcp_server(provider)
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name=f"{provider.provider_name}-shopper",
                server_version="0.2.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )
