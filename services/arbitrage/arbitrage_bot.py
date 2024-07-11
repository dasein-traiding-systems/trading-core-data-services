import asyncio
import logging
from datetime import datetime
from typing import Any, List

import pandas as pd
import numpy as np

from constants import SKIP_ASSETS, HIST_INTERVAL
from core.db import TimesScaleDb
from core.base import CoreBase
from core.types import SymbolStr
from core.exchange.binance import PrivateBinance, PrivateFuturesBinance
from core.types import Tf
from core.utils.logs import add_traceback
from config import BINANCE_API_SECRET, BINANCE_API_KEY, ZMQ_ARBITRAGE_BOT_PORT, CMD_ARBITRAGE_SPREADS, IS_DEV
import zmq
import zmq.asyncio
from .arbitrage_trading_system import ArbitrageTradingSystem


class ArbitrageBot(object):
    collateral = "USDT"

    def __init__(self):
        self.spot = PrivateBinance(api_key=BINANCE_API_KEY, api_secret=BINANCE_API_SECRET)
        self.futures = PrivateFuturesBinance(api_key=BINANCE_API_KEY, api_secret=BINANCE_API_SECRET)
        self.symbols: List[SymbolStr] = []
        self.spreads = pd.DataFrame(columns=["spot_price", "futures_price", "delta", "delta_perc"])
        context = zmq.asyncio.Context()
        self.socket = context.socket(zmq.PUSH)
        self.socket.bind("tcp://*:%s" % ZMQ_ARBITRAGE_BOT_PORT)
        self.trading_system = ArbitrageTradingSystem(spot_api=self.spot, futures_api=self.futures)

        self.db = TimesScaleDb(use_pool=True)

    async def init(self):
        self.spot.public.on_all_price_callback = lambda data: self.on_price_change(is_futures=False, data=data)
        self.futures.public.on_all_price_callback = lambda data: self.on_price_change(is_futures=True, data=data)
        await self.db.init()
        await self.spot.async_init(with_public=True)
        await self.futures.async_init(with_public=True)
        await self.load_symbol_names()
        await self.load_historical_data()
        await self.spot.public.wait_for_connection()
        await self.spot.public.send_message(global_feeds=['!miniTicker@arr'], method="SUBSCRIBE")
        await self.futures.public.wait_for_connection()
        await self.futures.public.send_message(global_feeds=['!markPrice@arr@1s'], method="SUBSCRIBE")

        if not IS_DEV:
            CoreBase.get_loop().create_task(self.save_spread_snapshot_loop())
            CoreBase.get_loop().create_task(self.push_updates_to_oracle_loop())
        CoreBase.get_loop().create_task(self.update_trading_system())

    async def load_symbol_names(self):
        future_symbols = [symbol for symbol, info in self.futures.public.symbol_info.items()
                          if info.quote_asset == self.collateral and info.base_asset not in SKIP_ASSETS
                          and info.underlying_type == 'COIN']
        spot_symbols = [symbol for symbol, info in self.spot.public.symbol_info.items()
                        if info.quote_asset == self.collateral]

        self.symbols = [s for s in future_symbols if s in spot_symbols]
        self.spreads['symbol'] = self.symbols
        self.spreads.set_index("symbol", inplace=True)

        logging.info(f"ARBITRAGE SYMBOLS: {','.join(self.symbols)}")

        return self.symbols

    def on_price_change(self, is_futures: bool, data: List[Any]):
        if is_futures:
            futures_prices = {SymbolStr(item['s']): item['p'] for item in data if item['s'] in self.symbols}
            self.spreads['futures_price'] = pd.Series(futures_prices, dtype="float")
        else:
            spot_prices = {SymbolStr(item['s']): item['c'] for item in data if item['s'] in self.symbols}
            self.spreads['spot_price'].update(pd.Series(spot_prices, dtype="float"))

    async def update_spreads(self):
        self.spreads['delta'] = self.spreads['spot_price'] - self.spreads['futures_price']
        self.spreads['delta_perc'] = self.spreads['delta'] / self.spreads['futures_price'] * 100

    async def load_historical_data(self):
        now_ = datetime.utcnow()

        for name, time_shift in HIST_INTERVAL.items():
            data = await self.db.load_last_arbitrage_deltas_stats(start_time=now_ - time_shift, end_time=now_)
            self.spreads[f'delta_{name}'] = data["avg_delta"]
            self.spreads[f'delta_perc_{name}'] = data["avg_delta_perc"]
            self.spreads[f'delta_{name}_max'] = data["max_delta"]
            self.spreads[f'delta_perc_{name}_max'] = data["max_delta_perc"]
            self.spreads[f'delta_{name}_min'] = data["min_delta"]
            self.spreads[f'delta_perc_{name}_min'] = data["min_delta_perc"]

    def refresh_history_stats(self):
        for name in HIST_INTERVAL.keys():
            self.spreads[f'delta_{name}'] = self.spreads[['delta', f'delta_{name}']].mean(axis=1)
            self.spreads[f'delta_perc_{name}'] = self.spreads[['delta_perc', f'delta_perc_{name}']].mean(axis=1)
            self.spreads[f'delta_{name}_max'] = self.spreads[['delta', f'delta_{name}_max']].max(axis=1)
            self.spreads[f'delta_perc_{name}_max'] = self.spreads[['delta_perc', f'delta_perc_{name}_max']].max(axis=1)
            self.spreads[f'delta_{name}_min'] = self.spreads[['delta', f'delta_{name}_min']].max(axis=1)
            self.spreads[f'delta_perc_{name}_min'] = self.spreads[['delta_perc', f'delta_perc_{name}_min']].min(axis=1)

    async def save_spread_snapshot_loop(self):
        while True:
            await asyncio.sleep(60 * 10)
            try:
                snap = self.spreads[self.spreads['delta'].notna()].copy(deep=True)
                snap["symbol_tf_id"] = pd.Series(
                    {symbol_tf[0]: symbol_tf_id for symbol_tf, symbol_tf_id in self.db.symbol_tf.items()
                     if symbol_tf[1] == Tf("1d")})
                timestamp = datetime.utcnow()
                await self.db.save_arbitrage_deltas(timestamp=timestamp,
                                                    data=snap[["symbol_tf_id", "delta", "delta_perc"]])
                logging.info(f"Arbitrage snapshot done {timestamp}")
                self.refresh_history_stats()

            except Exception as e:
                logging.warning(add_traceback(e))

    async def push_updates_to_oracle_loop(self):
        while True:
            await asyncio.sleep(60)
            try:
                await self.socket.send_string(CMD_ARBITRAGE_SPREADS, zmq.SNDMORE)
                await self.socket.send_pyobj(self.spreads)
                logging.info(f"Arbitrage spreads pushed")
            except Exception as e:
                logging.warning(add_traceback(e))

    async def update_loop(self):
        while True:
            try:
                # print(len(self.spreads[self.spreads['delta'].notna()]))
                await self.update_spreads()
            except Exception as e:
                logging.warning(add_traceback(e))

            await asyncio.sleep(5)

    async def update_trading_system(self):
        while True:
            try:
                # print(len(self.spreads[self.spreads['delta'].notna()]))
                for symbol, row in self.spreads.iterrows():
                    if not (np.isnan(row.spot_price) or np.isnan(row.futures_price)):
                        await self.trading_system.process_spread(symbol=symbol,
                                                                 spot_price=row.spot_price,
                                                                 futures_price=row.futures_price,
                                                                 spread=row.delta_perc)
            except Exception as e:
                logging.warning(add_traceback(e))

            await asyncio.sleep(1)
