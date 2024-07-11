import pydantic
from pydantic import BaseModel
from typing import Dict, List, Optional
from enum import Enum
from datetime import datetime


class ErrorType(Enum):
    ORACLE_INIT_ERR = "ORACLE_INIT_ERR"


class SymbolPriceItem(BaseModel):
    price: float


class SymbolItems(BaseModel):
    symbols: List[str]
    tfs: List[str]


class CandlesItem(BaseModel):
    timestamp: List[datetime]
    o: List[float]
    h: List[float]
    l: List[float]
    c: List[float]
    v: List[float]


class CandlesBounds(BaseModel):
    minPrice: float
    maxPrice: float
    maxVolume: float
    minVolume: float


class ClustersItem(BaseModel):
    timestamp: List[datetime]
    volume: List[float]
    price: List[float]


class SymbolCandles(BaseModel):
    symbol: str
    tf: str
    candles: CandlesItem
    bounds: CandlesBounds
    priceLevels: List[float]
    volumeLevel: float
    clusters: Dict[int, ClustersItem]


class SummaryItemByTf(BaseModel):
    tf: str
    volumeLevel: float
    volume: float
    volumeDiff: float
    volumeAvg100: float


class AtrItem(BaseModel):
    last: float
    last_24h: float


class LargeAtrItem(BaseModel):
    last: float
    last_5: float
    last_60: float
    volatility: float


class SummaryItem(BaseModel):
    symbol: str
    tf_1d: Optional[SummaryItemByTf]
    tf_4h: Optional[SummaryItemByTf]
    tf_1h: Optional[SummaryItemByTf]
    tf_15m: Optional[SummaryItemByTf]
    atr: AtrItem


class LargeSummaryItemByTf(BaseModel):
    tf: str
    volumeLevel: float
    volume: float
    volumeRatio: float
    volumeAvg_60d: float
    atr: LargeAtrItem
    price: float
    priceLevels: List[float]
    priceLevelsRatio: List[Optional[float]]


class LargeSummaryItem(BaseModel):
    symbol: str
    tfs: Dict[str, LargeSummaryItemByTf]
    # tf_1d: Optional[LargeSummaryItemByTf]
    # tf_4h: Optional[LargeSummaryItemByTf]
    # tf_1h: Optional[LargeSummaryItemByTf]
    # tf_15m: Optional[LargeSummaryItemByTf]


class ArbitrageHistoryStatsItem(BaseModel):
    tf: str
    delta: float
    delta_perc: float
    delta_max: float
    delta_perc_max: float
    delta_min: float
    delta_perc_min: float


class ArbitragePriceDeltaItem(BaseModel):
    spot: float
    futures: float
    delta: float
    delta_perc: float


class ArbitrageStatsItem(BaseModel):
    symbol: str

    price: ArbitragePriceDeltaItem
    history: List[ArbitrageHistoryStatsItem]


class ArbitrageChartData(BaseModel):
    timestamp: List[datetime]
    delta_perc: Dict[str, List[float]]
