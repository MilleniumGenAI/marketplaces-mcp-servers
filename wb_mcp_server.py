import asyncio
import core.env # Load env first!
from providers.wb import WbProvider
from mcp_server import run_server

if __name__ == "__main__":
    provider = WbProvider()
    asyncio.run(run_server(provider))
