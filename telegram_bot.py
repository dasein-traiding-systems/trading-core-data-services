from config import TELEGRAM_BOT_TOKEN
from core.utils.logs import setup_logger
from services import TradingSysTelegramBot

if __name__ == "__main__":
    setup_logger()
    tg_bot = TradingSysTelegramBot(telegram_bot_token=TELEGRAM_BOT_TOKEN).start()

    # async def main():
    #     try:
    #         await oracle.init()
    #         await oracle.update_loop()
    #     except Exception as e:
    #         logging.error(add_traceback(e))
    #

    # loop.create_task(main())
    # asyncio.run(tg_bot.start())
    # loop.run_forever()
