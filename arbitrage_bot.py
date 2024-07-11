from services.arbitrage import ArbitrageBot
import logging
import asyncio
from core.utils.logs import setup_logger
if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    setup_logger(global_logger_name="arbitrage")
    ab = ArbitrageBot()

    async def main():
        await ab.init()
        await ab.update_loop()

    asyncio.run(main())
