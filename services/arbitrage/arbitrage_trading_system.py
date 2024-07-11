import logging
from typing import Dict, List, Tuple, Optional

import numpy as np
from core.utils.telegram import send_to_telegram
from core.types import Symbol, OrderType, Side, SymbolStr, SideEffectType, OrderStatus, \
    ExchangeType
from core.exchange.binance import PrivateBinance, PrivateFuturesBinance
from core.exchange.binance.entities import Order
from core.exceptions import ShouldRetryApiException, NotAllowedApiException, BalanceApiException
from enum import Enum
from core.utils.logs import add_traceback


def get_avg_price(lst: List[Order], by_side: bool = False) -> Optional[float]:
    if len(lst) > 0:
        if by_side:
            q_price_total = sum([o.executed_quantity_by_side * o.price for o in lst if o.is_filled])
        else:
            q_price_total = sum([o.executed_quantity * o.price for o in lst if o.is_filled])

        a_total = sum([o.executed_quantity for o in lst])

        if a_total > 0:
            return q_price_total / a_total

    return None


class AmountType(Enum):
    TOTAL = "TOTAL"
    FILLED = "FILLED"


SPREAD_THRESHOLD_MIN = 0.23
SPREAD_THRESHOLD_OPEN = 0.23 * 2
SPREAD_THRESHOLD_CLOSE = 0.23
MAX_TOTAL_QUANTITY = 100
MAX_PAIR_QUANTITY = 25

IS_ISOLATED = False


def opposite_side(side: Side) -> Side:
    return Side.SELL if side == Side.BUY else Side.BUY


def opposite_sell_pair_side(sell_pair_side: ExchangeType) -> ExchangeType:
    return ExchangeType.FUTURES if sell_pair_side == ExchangeType.SPOT else ExchangeType.SPOT


def generate_paper_order(symbol: SymbolStr, side: Side, price: float,
                         quantity: float, quantity_filled: Optional[float]):
    return Order().from_paper(raw={"symbol": symbol, "side": side.value, "origQty": quantity,
                                   "executedQty": quantity_filled, "price": price, "status": OrderStatus.FILLED,
                                   "type": OrderType.MARKET})


def get_sell_pair_side(spread: float) -> ExchangeType:
    return ExchangeType.SPOT if spread > 0 else ExchangeType.FUTURES


def get_spread_diff(spread1: float, spread2: float) -> float:
    if spread1 > 0 and spread2 > 0:
        return abs(spread1 - spread2)
    elif spread1 < 0 and spread2 < 0:
        return abs(spread1 + spread2)

    return abs(spread1) + abs(spread2)


def get_pair_order_sides(sell_pair_side: ExchangeType) -> Tuple[Side, Side]:
    """
    SPOT SIDE / FUTURES SIDE
    :rtype: object
    """
    spot_side = Side.SELL if sell_pair_side == ExchangeType.SPOT else Side.BUY
    futures_side = Side.BUY if sell_pair_side == ExchangeType.SPOT else Side.SELL

    return spot_side, futures_side


async def send_msg(msg):
    logging.info(msg)
    await send_to_telegram(msg)


class ArbitragePairStateBase(object):
    def __init__(self, symbol: SymbolStr):
        self.symbol = symbol
        self.orders: Dict[ExchangeType, List[Order]] = {ExchangeType.FUTURES: [], ExchangeType.SPOT: []}
        self.sell_side: Optional[ExchangeType] = None

    def get_quantity(self, pair_side: ExchangeType, amount_type: AmountType):

        if not self.has_orders(pair_side):
            return 0

        orders = self.orders[pair_side]

        if amount_type == AmountType.TOTAL:
            return float(np.sum([o.quantity for o in orders]))
        else:
            return float(np.sum([o.executed_quantity for o in orders]))

    def get_avg_price(self, pair_side: ExchangeType, by_side: bool = False) -> Optional[float]:
        return get_avg_price(self.orders[pair_side], by_side=by_side)

    def get_spread_perc(self):
        price_spot = self.get_avg_price(ExchangeType.SPOT)
        price_futures = self.get_avg_price(ExchangeType.FUTURES)
        return (price_spot - price_futures) / price_futures * 100

    def get_spread_diff(self, spread: float):
        pair_spread = self.get_spread_perc()

        return round(pair_spread - spread if pair_spread > 0 and spread > 0 else pair_spread + spread, 3)

    def add_order(self, pair_side: ExchangeType, order: Order):
        self.orders[pair_side].append(order)

    def has_orders(self, pair_side: ExchangeType):
        return len(self.orders[pair_side]) > 0

    def is_full(self):
        return self.get_quantity(ExchangeType.SPOT, AmountType.FILLED) >= MAX_PAIR_QUANTITY

    def set_sell_side(self, pair_side: ExchangeType):
        self.sell_side = pair_side

    def get_quoted_price(self, pair_side: ExchangeType, side: Optional[Side] = None):
        return sum([o.price * o.executed_quantity for o in self.orders[pair_side]
                    if (side is None or o.side == side) and o.is_filled])

    def get_net_profit(self, pair_side: ExchangeType):
        return self.get_quoted_price(pair_side, Side.BUY) - self.get_quoted_price(pair_side, Side.SELL)


class ArbitragePairStatePaper(ArbitragePairStateBase):
    def __init__(self, symbol: SymbolStr):
        super(ArbitragePairStatePaper, self).__init__(symbol)


class ArbitrageTradingSystem(object):
    def __init__(self, spot_api: PrivateBinance, futures_api: PrivateFuturesBinance):
        self.spot = spot_api
        self.futures = futures_api
        self.pair: Dict[SymbolStr, ArbitragePairStateBase] = {}
        self.ban: List[Symbol] = []
        self.stop: bool = False

    def get_pair(self, symbol: SymbolStr):
        pair = self.pair.get(symbol, None)
        if pair is None:
            pair = self.pair[symbol] = ArbitragePairStatePaper(symbol)

        return pair

    def get_amount_total(self):
        return sum([p.get_quoted_price(ExchangeType.SPOT) for p in self.pair.values()])

    async def place_spot_order(self, open_mode: bool, symbol: SymbolStr, side: Side,
                               price: Optional[float] = None, amount: Optional[float] = None) -> Order:
        if open_mode:
            quantity = self.spot.public.get_asset_quantity(symbol, price, amount)
            order = await self.spot.place_order(symbol=symbol, side=side, order_type=OrderType.MARKET,
                                                is_isolated=IS_ISOLATED, side_effect_type=SideEffectType.MARGIN_BUY,
                                                quantity=quantity)
        else:
            quantity = self.pair[symbol].get_quantity(ExchangeType.SPOT, AmountType.FILLED)
            params = dict(symbol=symbol, side=side, order_type=OrderType.MARKET,
                          is_isolated=IS_ISOLATED, side_effect_type=SideEffectType.AUTO_REPAY)
            try:
                order = await self.spot.place_order(**params, quantity=quantity)
            except BalanceApiException as e:
                if e.code == -2010:  # fix asset quantity ??
                    asset = await self.spot.get_cross_margin_asset_balance(symbol.replace("USDT", ""))
                    await send_msg(f"FIX {symbol} REPAY AMOUNT {quantity} TO {asset['netAsset']}")
                    order = await self.spot.place_order(**params, quantity=asset['netAsset'])
                else:
                    raise e

        self.pair[symbol].add_order(pair_side=ExchangeType.SPOT, order=order)

        return order

    async def place_futures_order(self, open_mode: bool, symbol: SymbolStr, side: Side,
                                  price: Optional[float] = None, amount: Optional[float] = None) -> Order:

        if not open_mode:
            quantity = self.pair[symbol].get_quantity(ExchangeType.FUTURES, AmountType.FILLED)
        else:
            quantity = amount

        order = await self.futures.place_order(symbol=symbol, side=side,
                                               order_type=OrderType.MARKET,
                                               quantity=quantity,
                                               reduce_only=not open_mode)

        self.pair[symbol].add_order(pair_side=ExchangeType.FUTURES, order=order)

        return order

    # async def make_arbitrage_orders(self, open_mode: bool, symbol: SymbolStr, sell_pair_side: ExchangeType,
    #                                 spot_price: float, futures_price: float,
    #                                 amount: Optional[float] = None) -> [Order, Order]:
    #     spot_order = futures_order = None
    #     try:
    #         spot_side, futures_side = get_pair_order_sides(sell_pair_side)
    #         side_effect = SideEffectType.MARGIN_BUY if open_mode else SideEffectType.AUTO_REPAY
    #
    #         if open_mode:
    #             spot_quantity = self.spot.public.get_asset_quantity(symbol, spot_price, amount)
    #         else:
    #             spot_quantity = self.pair[symbol].get_quantity(ExchangeType.SPOT, AmountType.FILLED)
    #
    #         try:
    #             spot_order = await self.spot.place_order(symbol=symbol, side=spot_side, order_type=OrderType.MARKET,
    #                                                      quantity=spot_quantity, is_isolated=IS_ISOLATED,
    #                                                      side_effect_type=side_effect)
    #         except BalanceApiException as e:
    #             if e.code == -2010 and not open_mode:  # fix asset quantity ??
    #                 asset_name = symbol.replace("USDT", "")
    #                 user_assets = await self.spot.query_cross_margin_balance()
    #                 asset = user_assets[asset_name]
    #                 msg = f"FIX {symbol} REPAY AMOUNT {spot_quantity} ASSET {asset} FIX TO {asset['netAsset']}"
    #                 logging.warning(msg)
    #                 await send_to_telegram(msg)
    #                 spot_order = await self.spot.place_order(symbol=symbol, side=spot_side,
    #                                                          order_type=OrderType.MARKET,
    #                                                          quantity=asset['netAsset'], is_isolated=IS_ISOLATED,
    #                                                          side_effect_type=side_effect)
    #             else:
    #                 raise e
    #
    #         if open_mode:
    #             futures_quantity = spot_order.quantity
    #         else:
    #             futures_quantity = self.pair[symbol].get_quantity(ExchangeType.FUTURES, AmountType.FILLED)
    #
    #         futures_order = await self.futures.place_order(symbol=symbol, side=futures_side,
    #                                                        order_type=OrderType.MARKET,
    #                                                        quantity=futures_quantity,
    #                                                        reduce_only=not open_mode)
    #         return spot_order, futures_order
    #     except (ShouldRetryApiException, NotAllowedApiException) as e:
    #         logging.warning(e)
    #         raise e
    #     except Exception as e:
    #         self.stop = True
    #         logging.error(add_traceback(e))
    #         if open_mode:
    #             if futures_order is None and spot_order is not None:
    #                 side_effect = SideEffectType.AUTO_REPAY
    #
    #                 order = await self.spot.place_order(symbol=symbol, side=opposite_side(spot_order.side),
    #                                                     order_type=OrderType.MARKET, quantity=spot_order.quantity,
    #                                                     is_isolated=IS_ISOLATED, side_effect_type=side_effect)
    #
    #                 logging.warning(f"RELEASE SPOT {spot_order} with {order} AND STOP")
    #         else:
    #             logging.warning(f" SOME PROBLEM {spot_order} with {futures_order} !!!!!")
    #
    #         await send_to_telegram(f"ERROR: {e} SPOT {spot_order} FUTURES {futures_order}")
    #
    #         raise e

    async def process_spread(self, symbol: SymbolStr, spot_price: float, futures_price: float, spread: float):
        futures_order = spot_order = None
        futures_side = spot_side = pair = None
        try:

            if self.stop or symbol in self.ban:
                return

            pair = self.get_pair(symbol)

            if not pair.is_full() and abs(spread) >= SPREAD_THRESHOLD_OPEN and not \
                    self.get_amount_total() >= MAX_TOTAL_QUANTITY:
                sell_side = get_sell_pair_side(spread)
                spot_side, futures_side = get_pair_order_sides(sell_side)
                pair.set_sell_side(sell_side)

                msg = f"Open arbitrage <b>{symbol}</b> with <b>{round(spread, 3)}</b>\r\n" \
                      f"{spot_side.value} SPOT @ {spot_price}\r\n " \
                      f"{futures_side.value} FUTURES @ {futures_price}"
                await send_msg(msg)
                spot_order = await self.place_spot_order(open_mode=True, symbol=symbol, side=spot_side,
                                                         price=spot_price, amount=MAX_PAIR_QUANTITY)

                futures_order = await self.place_futures_order(open_mode=True, symbol=symbol, side=futures_side,
                                                               price=futures_price, amount=MAX_PAIR_QUANTITY)

                await send_msg(f"Opened arbitrage <b>{symbol} sell {sell_side.value}</b> with \r\n"
                               f"<b>SPOT: {spot_order}</b>\r\n"
                               f"<b>FUTURES: {futures_order}</b>")
            elif pair.is_full() and get_spread_diff(spread, pair.get_spread_perc()) >= SPREAD_THRESHOLD_CLOSE:
                sell_side = opposite_sell_pair_side(pair.sell_side)
                spot_side, futures_side = get_pair_order_sides(sell_side)
                spread_diff = pair.get_spread_diff(spread)
                price_open_spot = pair.get_avg_price(ExchangeType.SPOT)
                price_open_futures = pair.get_avg_price(ExchangeType.FUTURES)
                await send_msg(f"Close arbitrage <b>{symbol}</b> with <b>{round(spread, 3)}"
                               f"(diff: {round(spread_diff, 6)})</b>\r\n "
                               f"{spot_side.value} SPOT @ {spot_price} open. "
                               f"(diff. {spot_price - price_open_spot}), \r\n"
                               f"{futures_side.value} FUTURES @ {futures_price} "
                               f"(diff. {futures_price - price_open_futures})")

                spot_order = await self.place_spot_order(open_mode=False, symbol=symbol, side=spot_side,
                                                         price=spot_price, amount=MAX_PAIR_QUANTITY)

                futures_order = await self.place_futures_order(open_mode=False, symbol=symbol, side=futures_side,
                                                               price=futures_price)

                spot_profit = pair.get_net_profit(ExchangeType.SPOT)
                futures_profit = pair.get_net_profit(ExchangeType.FUTURES)
                msg = f"Cosed arbitrage <b>{symbol} sell {sell_side.value}</b> with <b>SPOT: {spot_order}</b> " \
                      f"<b>FUTURES: {futures_order}</b>\r\n" \
                      f"PROFIT: spot {spot_profit}$ futures {futures_profit}$ = {spot_profit + futures_profit}$ "
                await send_msg(msg)

                del self.pair[symbol]
        except NotAllowedApiException as e:
            logging.warning(f"{symbol} BAN due {e.message}")
            self.ban.append(symbol)
        except ShouldRetryApiException as e:
            if e.code == -3045:
                logging.warning(f"{symbol} BAN2 due {e.message}")
                self.ban.append(symbol)
        except Exception as e:  # UNCOVERED EXCEPTION
            self.stop = True
            logging.error(add_traceback(e))
            if pair is not None and pair.is_full():
                futures_order_place_problem = futures_order is None and spot_order is not None
                if futures_order_place_problem:
                    await send_msg(f"FUTURES CLOSE PROBLEM {symbol}")
                    # f_order = await self.futures.place_order(symbol=symbol, side=futures_side,
                    #                                order_type=OrderType.MARKET,
                    #                                close_position=True, quantity=1)
                    # logging.warning(f"FORCE FUTURES CLOSE {symbol} {futures_side} - {f_order}")
