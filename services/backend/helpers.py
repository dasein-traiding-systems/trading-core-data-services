from services.market_prediction import MarketPredictionOracle
import cachetools.func
from services.backend.models import LargeSummaryItem, LargeSummaryItemByTf, LargeAtrItem
from tc.core.types import Tf, Symbol
from typing import List, Dict, Union, Any, Optional, Tuple


def get_ratio(v1: Optional[float], v2: Optional[float]) -> Optional[float]:
    if v1 is not None and v1 is not None and v2 > 0:
        return round((v1 - v2) / v2, 2) * 100

    return None


def get_sup_resist_levels(price: float, levels: List[float]) -> Tuple[float, float]:
    sup_levels = [p for p in sorted(levels) if price >= p]
    resist_levels = [p for p in sorted(levels, reverse=True) if price <= p]

    def get_first(items: List[float]) -> float:
        return items[0] if len(items) > 0 else 0

    return get_first(sup_levels), get_first(resist_levels)


@cachetools.func.ttl_cache(ttl=60)
def get_summary_by_symbol(symbol: Symbol, oracle: MarketPredictionOracle) -> Dict[Tf, LargeSummaryItemByTf]:
    items = {}
    for tf in oracle.tfs:
        candles = oracle.api_client.candles[symbol][tf]
        candles["dnv"] = candles.c * candles.v
        dnv_avg_60d = candles["dnv"].iloc[-60:].mean()
        dnv_current = oracle.api_client.candle_dnv.get(symbol, {}).get(tf, None)
        key_ = (symbol, tf)
        dnv_level = oracle.dnv_levels[key_]
        price = oracle.api_client.mark_prices[symbol]
        price_levels = oracle.price_levels[key_]
        sup_resist_levels = get_sup_resist_levels(price, price_levels)
        price_ratios = [get_ratio(price, sup_resist_levels[0]), get_ratio(sup_resist_levels[1], price)]

        def get_atr_item() -> LargeAtrItem:
            last_5 = candles.iloc[-5:]
            last_60 = candles.iloc[-60:]
            last_ = candles.iloc[-1]
            atr_last = float(last_.h - last_.l)
            atr_5 = float(last_5.h.mean() - last_5.l.mean())
            atr_60 = float(last_60.h.mean() - last_60.l.mean())
            return LargeAtrItem(last=atr_last, last_5=atr_5, last_60=atr_60,
                                volatility=get_ratio(atr_last, atr_60))

        tf_item = LargeSummaryItemByTf(tf=tf, volumeLevel=dnv_level,
                                       volumeAvg_60d=dnv_avg_60d,
                                       volume=dnv_current,
                                       volumeRatio=get_ratio(dnv_current, dnv_level),
                                       atr=get_atr_item(),
                                       price=float(price),
                                       priceLevelsRatio=list(price_ratios),
                                       priceLevels=list(sup_resist_levels))
        items[tf] = tf_item

    return items
