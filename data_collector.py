from patch_submod import dummy  # <- REQUIRED
from services.collector import DataCollector, ta_processor_client
import logging
import asyncio
from tc.core.utils.logs import setup_logger, add_traceback
import atexit
from multiprocessing import Process
from tc.config import Config

config = Config.load_from_env()
dummy()

if __name__ == "__main__":
    setup_logger()
    dc = DataCollector(config)

    ta_processor = Process(target=ta_processor_client, args=(config,))
    ta_processor.start()

    @atexit.register
    def cleanup():
        logging.info("Cleanup")
        ta_processor.join()
        ta_processor.close()
        asyncio.get_event_loop().close()


    async def main():
        try:
            await dc.init()
            await dc.update_loop()
        except Exception as e:
            logging.error(add_traceback(e))

    asyncio.run(main())
