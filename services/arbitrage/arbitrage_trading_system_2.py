import logging
from typing import Dict, List, Tuple, Optional, Union, Literal

import numpy as np

from core.exchange.common.utils import get_avg_price
from core.utils.telegram import send_to_telegram
from core.types import Symbol, OrderType, Side, SymbolStr, SideEffectType, OrderStatus, \
    ExchangeType
from core.exchange.binance import PrivateBinance, PrivateFuturesBinance
from core.exchange.binance.entities import Order
from core.exceptions import ShouldRetryApiException, NotAllowedApiException, BalanceApiException
from enum import Enum
from core.utils.logs import add_traceback

SPREAD_THRESHOLD_MIN = 0.23
SPREAD_THRESHOLD_OPEN = 0.23 * 2
SPREAD_THRESHOLD_CLOSE = 0.23
MAX_TOTAL_QUANTITY = 100
MAX_PAIR_QUANTITY = 25

IS_ISOLATED = False

EXCHANGE_TYPE = Literal["SPOT", "FUTURES"]

SPOT: EXCHANGE_TYPE = "SPOT"
FUTURES: EXCHANGE_TYPE = "FUTURES"


def opposite_exchange_type(exchange_type: EXCHANGE_TYPE) -> EXCHANGE_TYPE:
    return FUTURES if exchange_type == SPOT else SPOT


def get_sell_exchange_type(spread: float) -> EXCHANGE_TYPE:
    return SPOT if spread > 0 else FUTURES


def get_spread_diff(spread1: float, spread2: float) -> float:
    if spread1 > 0 and spread2 > 0:
        return abs(spread1 - spread2)
    elif spread1 < 0 and spread2 < 0:
        return abs(spread1 + spread2)

    return abs(spread1) + abs(spread2)


def get_pair_order_sides(sell_exchange_type: EXCHANGE_TYPE) -> Tuple[Side, Side]:
    """
    SPOT SIDE / FUTURES SIDE
    :rtype: object
    """
    if sell_exchange_type == SPOT:
        return Side.SELL, Side.BUY
    else:
        return Side.BUY, Side.SELL


async def send_msg(msg):
    logging.info(msg)
    await send_to_telegram(msg)


class OrdersSet(object):
    def __init__(self):
        self.orders: List[Order] = []

    def add_order(self, order: Order):
        self.orders.append(order)

    @property
    def has_orders(self):
        return len(self.orders) > 0

    @property
    def quantity(self):
        return float(np.sum([o.executed_quantity for o in self.orders])) if self.has_orders else 0

    @property
    def avg_price(self) -> Optional[float]:
        return get_avg_price(self.orders)

    def get_quoted_price(self, side: Optional[Side] = None):
        return sum([o.price * o.executed_quantity for o in self.orders
                    if (side is None or o.side == side) and o.is_filled])


class ArbitragePairAtom(object):
    def __init__(self, symbol: SymbolStr, spot_api: PrivateBinance, futures_api: PrivateFuturesBinance):
        self.symbol = symbol
        self.spot = spot_api
        self.futures = futures_api
        self.orders: Dict[str, OrdersSet] = {FUTURES: OrdersSet(), SPOT: OrdersSet()}
        self.sell_exchange_type: Optional[EXCHANGE_TYPE] = None
        self.spread_open: Optional[float] = None

    @property
    def title(self):
        return f"<b>{self.symbol} {self.sell_exchange_type} SELL</b>"

    @property
    def asset_name(self):
        return self.symbol.replace("USDT", "")

    def get_spread(self):
        price_spot = self.orders[SPOT].avg_price
        price_futures = self.orders[FUTURES].avg_price
        return (price_spot - price_futures) / price_futures * 100

    def get_spread_diff(self, spread: float):
        pair_spread = self.get_spread()

        return round(pair_spread - spread if pair_spread > 0 and spread > 0 else pair_spread + spread, 3)

    def is_full(self):
        return self.orders[SPOT].quantity >= MAX_PAIR_QUANTITY

    def set_sell_exchange_type(self, spread: float):
        self.spread_open = spread
        self.sell_exchange_type = get_sell_exchange_type(spread)

    def get_net_profit(self, source: str):
        return self.orders[source].get_quoted_price(Side.BUY) - self.orders[source].get_quoted_price(Side.SELL)

    async def open(self, price_spot: Optional[float] = None, price_futures: Optional[float] = None):
        spot_side, futures_side = get_pair_order_sides(self.sell_exchange_type)
        quantity_usd = MAX_PAIR_QUANTITY
        quantity = self.spot.public.get_asset_quantity(self.symbol, price_spot, quantity_usd)
        order_s = await self.spot.place_order(symbol=self.symbol, side=spot_side, order_type=OrderType.MARKET,
                                              is_isolated=IS_ISOLATED, side_effect_type=SideEffectType.MARGIN_BUY,
                                              quantity=quantity)

        self.orders[SPOT].add_order(order=order_s)

        order_f = await self.futures.place_order(symbol=self.symbol, side=futures_side,
                                                 order_type=OrderType.MARKET,
                                                 quantity=quantity_usd)

        self.orders[FUTURES].add_order(order=order_f)

        await send_msg(f"Opened {self.title} with \r\n<b>SPOT: {order_s}</b>\r\n<b>FUTURES: {order_f}</b>")

    async def safe_close(self, price_spot: Optional[float] = None, price_futures: Optional[float] = None):
        sell_side = opposite_exchange_type(self.sell_exchange_type)
        spot_side, futures_side = get_pair_order_sides(sell_side)

        quantity = self.orders[SPOT].quantity
        params = dict(symbol=self.symbol, side=spot_side, order_type=OrderType.MARKET,
                      is_isolated=IS_ISOLATED, side_effect_type=SideEffectType.AUTO_REPAY)
        try:
            order_s = await self.spot.place_order(**params, quantity=quantity)
        except BalanceApiException as e:
            asset = await self.spot.get_cross_margin_asset_balance(self.asset_name)
            await send_msg(f"FIX {self.symbol} REPAY AMOUNT {quantity} TO {asset['netAsset']}")
            order_s = await self.spot.place_order(**params, quantity=asset['netAsset'])

        self.orders[SPOT].add_order(order=order_s)

        quantity = self.orders[FUTURES].quantity

        order_f = await self.futures.place_order(symbol=self.symbol, side=futures_side,
                                                 order_type=OrderType.MARKET,
                                                 quantity=quantity,
                                                 reduce_only=True)

        self.orders[FUTURES].add_order(order=order_f)

        spot_profit = self.get_net_profit(SPOT)
        futures_profit = self.get_net_profit(FUTURES)
        total = spot_profit - futures_profit
        profit_msg = f"PROFIT: spot {f'{spot_profit}:.4f'}$ futures {f'{futures_profit}:.4f'}$ = {f'{total}:.4f'}$ "
        await send_msg(f"Closed {self.title} with \r\n<b>SPOT: {order_s}</b>\r\n<b>FUTURES: {order_f}</b>"
                       f"\r\n{profit_msg}")


class ArbitrageTradingSystem(object):
    def __init__(self, spot_api: PrivateBinance, futures_api: PrivateFuturesBinance):
        self.spot = spot_api
        self.futures = futures_api
        self.pair: Dict[SymbolStr, ArbitragePairAtom] = {}
        self.ban: List[Symbol] = []
        self.stop: bool = False

    def get_pair_atom(self, symbol: SymbolStr):
        pair = self.pair.get(symbol, None)
        if pair is None:
            pair = self.pair[symbol] = ArbitragePairAtom(symbol, futures_api=self.futures, spot_api=self.spot)

        return pair

    def get_amount_total(self):
        return sum([p.orders[SPOT].get_quoted_price() for p in self.pair.values()])

    def is_full_depo(self):
        return self.get_amount_total() >= MAX_TOTAL_QUANTITY

    async def halt_and_recover_balance(self):
        pass

    async def process_spread(self, symbol: SymbolStr, spot_price: float, futures_price: float, spread: float):
        try:

            if self.stop or symbol in self.ban:
                return

            pair = self.get_pair_atom(symbol)
            is_full = pair.is_full()

            if not is_full and abs(spread) >= SPREAD_THRESHOLD_OPEN and not self.is_full_depo():
                pair.set_sell_exchange_type(spread)
                await pair.open(price_spot=spot_price, price_futures=futures_price)

            elif is_full and pair.get_spread_diff(spread) >= SPREAD_THRESHOLD_CLOSE:
                await pair.safe_close(price_spot=spot_price, price_futures=futures_price)
                del self.pair[symbol]

        except (ShouldRetryApiException, NotAllowedApiException) as e:
            if type(e) is ShouldRetryApiException and e.code != -3045:
                self.stop = True
                logging.error(add_traceback(e))
                await self.halt_and_recover_balance()

            logging.warning(f"{symbol} BAN due {e.message}")
            self.ban.append(symbol)

        except Exception as e:
            self.stop = True
            logging.error(add_traceback(e))
            await self.halt_and_recover_balance()
