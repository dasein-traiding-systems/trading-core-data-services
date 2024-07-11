from services.backend import app, oracle
from tc.core.exchange.common.mappers import symbol_to_binance, binance_to_symbol
from fastapi import Path

from services.backend.models import SymbolItems, CandlesItem, CandlesBounds, SymbolCandles, \
    SummaryItem, SummaryItemByTf, AtrItem, LargeSummaryItem, ClustersItem, \
    ArbitrageStatsItem, ArbitrageHistoryStatsItem, ArbitragePriceDeltaItem, ArbitrageChartData
from services.backend.utils import check_is_oracle_initialized
from tc.core.ta.clusters import normalize_clusters_for_plot
from tc.config import Config
from typing import List, Dict
from services.backend.helpers import get_summary_by_symbol
from constants import HIST_INTERVAL
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
PD_DATE_TIME_FORMAT = '%Y-%m-%d %X'


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/symbols", response_model=SymbolItems)
async def symbols():
    check_is_oracle_initialized()
    symbols = oracle.symbols
    symbols.sort()
    return SymbolItems(symbols=oracle.symbols, tfs=Config().ORACLE_TFS)


@app.get("/candles/{symbol}/{tf}", response_model=SymbolCandles)
async def candles(symbol: str = Path(), tf: str = Path(), timestamp_from: int = 0, timestamp_to: int = 0):
    check_is_oracle_initialized()

    candles = oracle.api_client.get_candles(symbol.upper(), tf.lower())

    candles["dnv"] = candles["v"] * candles["c"]

    clusters_all = await oracle.db.load_clusters(symbol, start_time=candles.index[0].to_pydatetime(),
                                                 end_time=candles.index[-1].to_pydatetime())
    clusters_items = normalize_clusters_for_plot(clusters_all)

    clusters = {i: ClustersItem(timestamp=[d.to_pydatetime() for d in df.timestamp],
                                price=list(df.price), volume=list(df.volume)) for i, df in clusters_items.items()}

    candles_item = CandlesItem(o=list(candles.o), h=list(candles.h), l=list(candles.l), c=list(candles.c),
                               v=list(candles.dnv), timestamp=list(candles.index.to_pydatetime()))
    symbol_ts = (binance_to_symbol(symbol), tf)
    price_levels = oracle.price_levels[symbol_ts]
    volume_level = oracle.dnv_levels[symbol_ts]
    bounds = CandlesBounds(maxPrice=float(candles.h.max()), minPrice=float(candles.l.min()),
                           minVolume=float(candles.dnv.min()), maxVolume=float(candles.dnv.max()))
    result = SymbolCandles(symbol=symbol, tf=tf, candles=candles_item, bounds=bounds,
                           volumeLevel=float(volume_level), priceLevels=list(price_levels),
                           clusters=clusters)
    return result


@app.get("/market-summary", response_model=List[SummaryItem])
async def market_summary():
    check_is_oracle_initialized()
    summary = oracle.get_summary()
    result = []
    for symbol, tfs in summary.items():
        summary_item = SummaryItem(symbol=symbol_to_binance(symbol),
                                   atr=AtrItem(last=tfs['atr']['last'], last_24h=tfs['atr']['24h']))
        for tf, v in tfs.items():
            if tf == "atr":
                continue
            summary_item.__dict__[f'tf_{tf}'] = SummaryItemByTf(volumeLevel=round(v['dnv_level']),
                                                                volume=round(v['dnv_current']),
                                                                volumeDiff=round(v['dnv_diff'], 2),
                                                                volumeAvg100=round(v['dnv_avg100']),
                                                                tf=tf)

        result.append(summary_item)
    result = sorted(result, key=lambda item: item.tf_1d.volumeDiff, reverse=True)
    return result


@app.get("/long-summary", response_model=List[LargeSummaryItem])
async def market_summary():
    check_is_oracle_initialized()
    result = []
    for symbol in oracle.symbols:
        tfs = get_summary_by_symbol(symbol, oracle)
        result.append(LargeSummaryItem(symbol=symbol_to_binance(symbol), tfs=tfs))
    return result


@app.get("/arbitrage-stats", response_model=List[ArbitrageStatsItem])
async def arbitrage_stats():
    result = []
    spreads = oracle.arbitrage_spreads
    # ArbitrageStatsItem, ArbitrageHistoryStatsItem
    for symbol, data in spreads.fillna(0).iterrows():
        hist_items = []
        for name in HIST_INTERVAL:
            delta = round(float(data[f'delta_{name}']),3)
            delta_perc = round(float(data[f'delta_perc_{name}']),3)
            delta_perc_max = round(float(data[f'delta_perc_{name}_max']),3)
            delta_max = round(float(data[f'delta_{name}_max']),3)
            delta_perc_min = round(float(data[f'delta_perc_{name}_min']),3)
            delta_min = round(float(data[f'delta_{name}_min']),3)
            hist_items.append(ArbitrageHistoryStatsItem(tf=name, delta=delta, delta_perc=delta_perc,
                                                        delta_max=delta_max, delta_perc_max=delta_perc_max,
                                                        delta_min=delta_min, delta_perc_min=delta_perc_min
                                                        ))
        price_item = ArbitragePriceDeltaItem(spot=float(data["spot_price"]), futures=float(data["futures_price"]),
                                             delta=round(float(data.delta), 3),
                                             delta_perc=round(float(data.delta_perc),3))
        item = ArbitrageStatsItem(symbol=symbol, price=price_item, history=hist_items)
        result.append(item)
    return result


@app.get("/arbitrage-chart-data", response_model=ArbitrageChartData)
async def arbitrage_stats():
    now_ = datetime.utcnow()

    df = await oracle.db.load_arbitrage_deltas(start_time=now_ - timedelta(days=7), end_time=now_)
    symbols = df.symbol.unique()
    timestamps = [pd.to_datetime(i).to_pydatetime() for i in df.timestamp.unique()]
    delta_perc: Dict[str,List[float]] = {}
    for i, symbol in enumerate(symbols):
        data = df[df.symbol == symbol]
        if len(data) > 0 and np.max(data.delta_perc) > 0.23 and np.min(data.delta_perc) < -0.23:
            delta_perc[symbol] = [round(float(i),3) for i in data.delta_perc]

    data = ArbitrageChartData(timestamp=timestamps, delta_perc=delta_perc)
    return data
