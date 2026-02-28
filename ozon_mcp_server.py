import asyncio
import core.env # Load env first!
from providers.ozon import OzonProvider
from mcp_server import run_server

if __name__ == "__main__":
    provider = OzonProvider()
    asyncio.run(run_server(provider))
