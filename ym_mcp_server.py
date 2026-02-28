import asyncio
import core.env # Load env first!
from providers.ym import YmProvider
from mcp_server import run_server

if __name__ == "__main__":
    provider = YmProvider()
    asyncio.run(run_server(provider))
