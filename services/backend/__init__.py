import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from tc.core.base import CoreBase
from services.market_prediction import MarketPredictionOracle
from services.telegram_bot import TradingSysTelegramBot
from tc.core.utils.logs import setup_logger
from tc.config import Config
import asyncio
setup_logger()

app = FastAPI()

origins = [
    "http://localhost",
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    # allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

config = Config.load_from_env(root_path="..")
oracle = MarketPredictionOracle(config=config)

tg_bot = TradingSysTelegramBot(oracle=oracle, config=config)


# async def send_signal(
#         self, symbol_tf: SymbolTf, signal_type: SignalCallbackType, **kwargs
# ):

@app.on_event("startup")
async def startup_event():
    try:
        CoreBase.get_request()
        # loop = asyncio.get_event_loop()
        CoreBase.loop.create_task(oracle.init())
        CoreBase.loop.create_task(oracle.update_loop())
        asyncio.create_task(tg_bot.start())
        logging.info("SERVER STARTED...")
    except Exception as e:
        logging.error(f"SERVER NOT STARTED {e}")
    # tasks.add_task(oracle.init)
    # tasks.add_task(oracle.update_loop)
    # asyncio.run_coroutine_threadsafe(oracle.init(), loop)
    # asyncio.run_coroutine_threadsafe(oracle.update_loop(), loop)
    # loop.create_task(periodic())
