import asyncio
import logging
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Tuple, Union, Optional

import pandas as pd

from tc.core.db import TimesScaleDb
import cachetools.func
from tc.core.providers import TimescaleDataProvider
from tc.config import Config, ZMQ_ARBITRAGE_BOT_PORT, CMD_ARBITRAGE_SPREADS
from tc.core.base import CoreBase
from tc.core.exchange.binance.public import PublicBinance
from tc.core.exchange.common.mappers import binance_to_symbol, symbol_to_binance
from tc.core.types import Symbol, SymbolTf, Tf, TaLevels
from tc.core.utils.logs import setup_logger, add_traceback
import zmq
import zmq.asyncio


class SignalCallbackType(Enum):
    VOLUME_LEVEL = "VOLUME_LEVEL"
    PRICE_LEVEL = "PRICE_LEVEL"


class MarketPredictionOracle(object):
    def __init__(self, config: Config, signal_callback: Optional[Callable] = None):
        self.db = TimesScaleDb(**config.get_timescale_db_params())

        self.api_client = PublicBinance(on_candle_callback=self.callback_candle,
                                        data_provider=TimescaleDataProvider(db=self.db))
        self.dnv_levels: Dict[SymbolTf, Optional[float]] = {}
        self.price_levels: Dict[SymbolTf, List[float]] = {}
        self.symbols: List[Symbol] = []
        self.tfs: List[Tf] = []
        self.symbol_tfs: List[SymbolTf] = []
        self.signal_callback = signal_callback
        self.config = config
        self.done_signals: Dict[
            Tuple[SymbolTf, Union[float, None], SignalCallbackType], bool
        ] = {}
        self.initialized = False
        self.logger = setup_logger(name="oracle")
        self.mark_prices_: Dict[Symbol, float] = {}  # PREV
        self.update_levels_flag: bool = False
        self.arbitrage_spreads: pd.DataFrame = pd.DataFrame()

    async def init_symbols(self):
        symbol_status = await self.db.get_symbol_status(active=True)
        self.symbols = [binance_to_symbol(r['symbol']) for r in symbol_status]  # [:1] # DEBUG: [:1]
        self.tfs: List[Tf] = [Tf(tf) for tf in self.config.ORACLE_TFS]
        # self.price_levels
        # self.dnv_levels
        for tf in self.tfs:
            for s in self.symbols:
                self.symbol_tfs.append((s, tf))
                self.price_levels[(s, tf)] = []
                self.dnv_levels[(s, tf)] = None

    async def init_ws_subscriptions(self):
        feeds = [f"kline_{tf}" for tf in self.tfs]
        # feeds = [f"kline_1m"]
        self.logger.info(f"Initialize {self.api_client.logger_name} with {feeds}...")
        await self.api_client.subscribe(self.symbols, feeds)
        self.logger.info("Init oracle callbacks...")
        # for symbol, tf in self.symbol_tfs:
        #     feed = f"kline_{tf}"
        #     self.data_source.add_callback(
        #         id=f"cb-{symbol_to_binance(symbol)}-{feed}",
        #         channel=feed,
        #         symbol=symbol,
        #         callback=self.callback_candle,
        #     )

    async def init(self):
        try:
            await self.db.init()
            await self.init_symbols()

            await self.api_client.async_init()
            self.mark_prices_ = self.api_client.mark_prices
            await self.api_client.wait_for_connection()
            await self.init_ws_subscriptions()
            await self.init_oracle_data()
            CoreBase.get_loop().create_task(self.arbitrage_zmq_loop())
            self.initialized = True
            self.logger.info("Oracle initialized.")
        except Exception as ex:
            self.logger.error(add_traceback(ex))

    async def init_oracle_data(self):
        try:
            self.logger.info("Initialize signals")
            await self.update_levels()

            self.logger.info("Initialize signals - DONE.")
        except Exception as e:
            logging.info(add_traceback(e))
        await asyncio.sleep(0)

    @cachetools.func.ttl_cache(ttl=5)
    async def update_levels(self):
        logging.info("Update levels trigger")
        levels = await self.db.load_levels(symbol=[symbol_to_binance(s) for s in self.symbols],
                                           tf=self.tfs)
        new_price_levels: Dict[Any, List[float]] = {s: [] for s in self.symbol_tfs}
        for l in levels:
            symbol = binance_to_symbol(l['symbol'])
            tf = Tf(l['tf'])
            if l['level_type'] == TaLevels.Volume.value:
                self.dnv_levels[(symbol, tf)] = l['level_value']
            if l['level_type'] == TaLevels.Price.value:
                new_price_levels[(symbol, tf)].append(l['level_value'])

        self.price_levels = new_price_levels

    async def callback_candle(self, symbol: Symbol, tf: Tf, candle_closed: bool, *args, **kwargs):
        if candle_closed:
            self.logger.info(f"Callback: {symbol}-{tf}")
            self.update_levels_flag = True
            # await self.update_levels()

    def update_mark_price(self, symbol: Symbol):
        price_ = self.mark_prices_[symbol]
        price = self.api_client.get_mark_price(symbol)
        self.mark_prices_[symbol] = price
        return price_, price

    async def check_signals(self):
        try:
            for symbol, tf in self.symbol_tfs:
                current_dnv = self.api_client.get_dnv(symbol, tf)

                level_key = (symbol, tf)
                current_level = self.dnv_levels.get(level_key, None)

                for pl in self.price_levels[level_key]:
                    price_, price = self.update_mark_price(symbol)

                    if price is not None:
                        signal_key = (level_key, pl, SignalCallbackType.PRICE_LEVEL)
                        if (price_ < pl < price) or (price_ > pl > price):
                            if not self.done_signals.get(signal_key, False):
                                if self.signal_callback is not None:
                                    await self.signal_callback(
                                        level_key, SignalCallbackType.PRICE_LEVEL, level=pl
                                    )
                                self.done_signals.setdefault(signal_key, True)
                                # TODO: CLEAN DONE SIGNALS FOR PRICE LEVEL

                if current_dnv is not None and current_level is not None:
                    signal_key = (level_key, None, SignalCallbackType.VOLUME_LEVEL)

                    if current_dnv >= current_level:
                        if not self.done_signals.get(signal_key, False):
                            if self.signal_callback is not None:
                                await self.signal_callback(
                                    level_key,
                                    SignalCallbackType.VOLUME_LEVEL,
                                    level=current_level,
                                )
                            self.done_signals.setdefault(signal_key, True)
                    else:
                        self.done_signals.pop(signal_key, None)
        except Exception as e:
            logging.error(add_traceback(e))

    async def update_loop(self):
        while True:
            if self.initialized:
                if self.update_levels_flag:
                    await self.update_levels()
                    self.update_levels_flag = False
                await self.check_signals()
            await asyncio.sleep(5)

    async def arbitrage_zmq_loop(self):
        context = zmq.asyncio.Context()
        socket_pull = context.socket(zmq.PULL)
        uri = f"tcp://{self.config.MAIN_SERVER}:{ZMQ_ARBITRAGE_BOT_PORT}"
        socket_pull.connect(uri)
        logging.info("Arbitrage server connected @ %s" % uri)
        while True:
            try:
                topic = await socket_pull.recv_string()
                frame = await socket_pull.recv_pyobj()
                if topic == CMD_ARBITRAGE_SPREADS:
                    self.arbitrage_spreads = frame
            except Exception as e:
                logging.error(add_traceback(e))

    @cachetools.func.ttl_cache(ttl=10)
    def get_summary_by_symbol(self, symbol: Symbol) -> Dict[Union[Tf, str], Union[float, Dict[str, Any]]]:
        result: Dict[Union[Tf, str], Union[float, Dict[str, Any]]] = {}
        for tf in self.tfs:
            candles = self.api_client.candles[symbol][tf]
            candles["dnv"] = candles.c * candles.v
            dnv_avg100 = candles["dnv"].iloc[-100:].mean()
            dnv_current = self.api_client.candle_dnv.get(symbol, {}).get(tf, None)
            key_ = (symbol, tf)
            dnv_level = self.dnv_levels[key_]
            dnv_diff = 0
            if dnv_level is not None and dnv_current is not None and dnv_current > 0:
                dnv_diff = ((dnv_level - dnv_current) / dnv_current) * 100
            price_levels = self.price_levels[key_]

            result[tf] = dict(
                dnv_avg100=round(dnv_avg100),
                dnv_level=round(dnv_level),
                dnv_current=dnv_current,
                dnv_diff=dnv_diff,
                price_levels=price_levels,
            )
        last_1d = self.api_client.candles[symbol][Tf("1d")].iloc[-1]
        last_24h = self.api_client.candles[symbol][Tf("1h")].iloc[-24]
        atr_last = float(last_1d.h - last_1d.l)
        atr_24 = float(last_24h.h.mean() - last_24h.l.mean())
        result["atr"] = {"last": atr_last, "24h": atr_24}
        return result

    def get_summary(self) -> Dict[Symbol, Dict[Tf, Dict[str, Any]]]:
        result: Dict[Symbol, Dict[Tf, Dict[str, Any]]] = {}
        for s in self.symbols:
            result[s] = self.get_summary_by_symbol(s)

        return result


if __name__ == "__main__":

    # db = MongoDb()
    config = Config.load_from_env()


    def signal_callback(symbol_tf: SymbolTf, signal_type: SignalCallbackType, **kwargs):
        logging.warning(f"SIGNAL: {symbol_tf} - {signal_type}")


    oracle = MarketPredictionOracle(config=config, signal_callback=signal_callback)


    async def main():
        try:
            await oracle.init()
            await oracle.update_loop()
        except Exception as e:
            logging.error(add_traceback(e))


    CoreBase.get_loop().create_task(main())
    # asyncio.run(main())
    CoreBase.get_loop().run_forever()
