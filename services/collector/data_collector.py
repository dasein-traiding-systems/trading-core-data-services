import asyncio
import logging
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Tuple, Union, Optional
from tc.core.base import CoreBase
import pandas as pd
from tc.config import ZMQ_CLUSTERS_PORT, Config
from tc.core.exchange.binance import PublicBinance, PublicFuturesBinance
from tc.core.utils.data import candles_to_data_frame
from tc.core.exchange.common.mappers import symbol_to_binance, binance_to_symbol

from tc.core.types import SymbolStr, Symbol, Tf, Singleton, TaLevels
from tc.core.utils.logs import setup_logger, add_traceback, get_stream_handler
from tc.core.db.timescaledb import TimesScaleDb
from tc.core.ta.clusters import get_clusters
from tc.core.ta.ta import get_volume_levels, get_price_levels, get_sup_resist_peaks
from tc.core.providers import TimescaleDataProvider
from multiprocessing import get_logger
# from loky import set_loky_pickler, Future
# from loky import get_reusable_executor
# from loky import wrap_non_picklable_objects
import zmq
import zmq.asyncio

DATA_COLLECTOR_FEEDS = ["aggTrade", "kline_15m", "kline_1h", "kline_4h",  "kline_1d"]
# DATA_COLLECTOR_FEEDS = ["aggTrade", "kline_1m"]
# set_loky_pickler('pickle')


def init_mp_logger():
    mp_logger = get_logger()
    mp_logger.addHandler(get_stream_handler())
    mp_logger.setLevel(logging.INFO)
    return mp_logger


init_mp_logger()


# @wrap_non_picklable_objects
def store_levels(symbol: SymbolStr, tf: Tf, symbol_tf_id: int, candles: pd.DataFrame):
    mp_logger = get_logger()
    try:
        timestamp = datetime.utcnow()
        levels = []
        v_levels = get_volume_levels(candles)
        if v_levels is not None and len(v_levels) >0:
            levels.append([TaLevels.Volume, v_levels[-1][2]])

        # _up, _down, _levels = get_sup_resist_peaks(candles)
        # price_levels = get_price_levels(_levels)
        #
        # for pl in price_levels:
        #     levels.append([TaLevels.Price, pl])

        # mp_logger.info(f"Levels {symbol} {tf} {timestamp} ready {levels}.")
        return levels

    except Exception as e:
        mp_logger.error(add_traceback(e))


def store_clusters(symbol: SymbolStr, tf: Tf, symbol_tf_id: int, timestamp: datetime, h_price: float, l_price: float,
                   trades: List[Any], step: float) -> pd.DataFrame:
    mp_logger = get_logger()
    try:
        df_trades = pd.DataFrame(trades, columns=["timestamp", "price", "volume", "is_buyer"])

        clusters_data, _ = get_clusters(df_trades, l_price, h_price, step=step)
        df_clusters = pd.DataFrame(data=clusters_data, columns=["price_from", "price_to", "volume"])

        # mp_logger.info(f"Clusters {symbol} {tf} {timestamp} ready.")

        return df_clusters
    except Exception as e:
        mp_logger.error(add_traceback(e))


def ta_processor_client(config: Config):
    mp_logger = get_logger()
    db = TimesScaleDb(**config.get_timescale_db_params(), use_pool=False)
    CoreBase.get_loop().run_until_complete(db.init(simple=True))

    context = zmq.Context()
    socket_pull = context.socket(zmq.PULL)
    socket_pull.connect("tcp://localhost:%s" % ZMQ_CLUSTERS_PORT)  # socket.setsockopt(zmq.SUBSCRIBE, b'camera_frame')
    mp_logger.info("TA Processor connected to server with port %s" % ZMQ_CLUSTERS_PORT)
    # Initialize poll set
    poller = zmq.Poller()
    poller.register(socket_pull, zmq.POLLIN)
    loop = CoreBase.get_loop()
    while True:
        try:
            socks = dict(poller.poll())
            if socket_pull in socks and socks[socket_pull] == zmq.POLLIN:
                topic = socket_pull.recv_string()
                frame = socket_pull.recv_pyobj()
                symbol = frame['symbol']
                tf = frame['tf']
                mp_logger.info(f"Recieved {topic} cmd for {symbol} {tf}")

                start = time.time()

                if topic == "clusters":
                    df = store_clusters(**frame)
                    loop.run_until_complete(db.save_clusters(frame['symbol_tf_id'], frame['timestamp'],
                                                             frame['step'], df))
                if topic == "levels":
                    levels = store_levels(**frame)
                    timestamp = datetime.utcnow()
                    for l in levels:
                        loop.run_until_complete(db.save_levels(frame['symbol_tf_id'], timestamp, l[0], l[1]))

                mp_logger.info(f"{topic} for {symbol} {tf} DONE in {time.time() - start}s")

        except Exception as e:
            mp_logger.error(add_traceback(e))


class DataCollector(object, metaclass=Singleton):
    def __init__(self, config: Config):
        self.db = TimesScaleDb(**config.get_timescale_db_params())
        self.api_client = PublicBinance(on_trade_callback=self.on_trade,
                                        on_candle_callback=self.on_candle_callback,
                                        data_provider=TimescaleDataProvider(db=self.db))
        self.symbols: Dict[SymbolStr, Dict[str, Any]] = {}
        self.trades: Dict[SymbolStr, List[Any]] = {}
        context = zmq.Context()
        self.socket = context.socket(zmq.PUSH)
        self.socket.bind("tcp://*:%s" % ZMQ_CLUSTERS_PORT)

    async def init_symbols(self):
        symbol_status = await self.db.get_symbol_status(active=True)
        self.symbols = {r['symbol']: r for r in symbol_status}
        self.trades = {s: [] for s in self.symbols.keys()}

    async def init_ws_subscriptions(self):
        logging.info(f"Initialize  DATA COLLECTOR...")
        symbols_to_subscribe = [binance_to_symbol(s) for s in self.symbols.keys()]
        await self.api_client.subscribe(symbols_to_subscribe, DATA_COLLECTOR_FEEDS)

    async def init(self):
        try:
            await self.db.init()
            await self.init_symbols()
            await self.api_client.async_init()
            await self.api_client.wait_for_connection()
            await self.init_ws_subscriptions()
            logging.info("DATA COLLECTOR INITIALIZED")
        except Exception as ex:
            logging.error(add_traceback(ex))

    async def on_trade(self, symbol: SymbolStr, price: float, volume: float, is_buyer: bool, timestamp: datetime):
        # logging.info(f"Trade: {timestamp} {symbol}-{price} {volume} {is_buyer}")
        self.trades[symbol].append([timestamp, price, volume, is_buyer])
        await self.db.add_trade(symbol, price, volume, is_buyer, timestamp)

    async def on_candle_callback(self, symbol: SymbolStr, tf: Tf, candle_closed: bool, candle_item: List[Any],
                                 close_time: datetime):
        if candle_closed:
            await self.db.save_candles(symbol, tf, candles=candles_to_data_frame([candle_item]))

            if tf == Tf("15m"):
                self.start_clusters_process(symbol, tf, candle_item, close_time)

            self.start_levels_process(symbol, tf)

            logging.info(f"Candle: {candle_item[0]} {symbol}_{tf} {candle_closed} done")

    def start_clusters_process(self, symbol: SymbolStr, tf: Tf, candle_item: List[Any], close_time: datetime):
        symbol_tf_id = self.db.symbol_tf[(symbol, tf)]
        trades = [t for t in self.trades[symbol] if t[0] <= close_time]  # <= close_time
        self.socket.send_string("clusters", zmq.SNDMORE)
        self.socket.send_pyobj(dict(symbol=symbol, tf=tf, symbol_tf_id=symbol_tf_id, timestamp=candle_item[0],
                                    h_price=candle_item[2], l_price=candle_item[3], trades=trades,
                                    step=self.symbols[symbol]["cluster_size"]))
        self.trades[symbol] = [t for t in self.trades[symbol] if candle_item[0] > t[0]]

    def start_levels_process(self, symbol: SymbolStr, tf: Tf):
        symbol_tf_id = self.db.symbol_tf[(symbol, tf)]
        symbol_ = binance_to_symbol(symbol)
        self.socket.send_string("levels", zmq.SNDMORE)
        self.socket.send_pyobj(dict(symbol=symbol, tf=tf, symbol_tf_id=symbol_tf_id,
                                    candles=self.api_client.candles[symbol_][tf]))

    async def update_loop(self):
        while True:
            await asyncio.sleep(5)


if __name__ == "__main__":
    setup_logger()
    data_collector = DataCollector()


    async def main():
        try:
            await data_collector.init()
            await data_collector.update_loop()
        except Exception as e:
            logging.error(add_traceback(e))


    # CoreBase.get_loop().create_task(main())
    asyncio.run(main())
