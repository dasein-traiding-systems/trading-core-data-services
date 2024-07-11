import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional

from config import (
    API_KEY,
    API_SECRET,
    GOOGLE_SERVICE_KEY_FILE_NAME,
    TRADING_DIARY_SPREEDSHEET_ID,
)
from core.base import CoreBase
from core.exchange.binance.entities import Order, Position
from core.exchange.binance.private_futures import PrivateFuturesBinance
from core.exchange.common.mappers import symbol_to_binance
from core.types import Side
from core.utils.logs import setup_logger
from services import GoogleSheets

DIARY_SHEET_NAME = "trading"


class TradingDiary(object):
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        spreadsheet_id: str,
        google_service_key_file_name: str,
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.logger = setup_logger("trading_diary")
        self.binance_futures = PrivateFuturesBinance(
            api_key=api_key, api_secret=api_secret
        )
        self.gs = GoogleSheets(
            spreadsheet_id=spreadsheet_id,
            google_docs_creds_file_name=f'./secrets/{google_service_key_file_name}',
        )

    async def init(self):
        self.logger.info("Trading diary init...")
        await self.gs.init()
        self.binance_futures.add_callback(
            id="position", channel="position", callback=self.position_callback
        )

        await self.binance_futures.async_init(with_public=False)

    async def position_callback(
        self, position: Position, order: Optional[Order] = None
    ):
        self.logger.info(f"Exec: {position} {order}")

        # finalize by zero amount AND following next order
        # is_close_order_finalized = (
        #     order is not None and order.trade_update_time == position.trade_update_time
        # )
        if position.closed:
            await self.export_to_sheets(position)

        await asyncio.sleep(0)

    async def export_to_sheets(self, position: Position):
        symbol = position.symbol
        side = position.side_by_amount
        entry_price = position.entry_price
        close_price = position.close_price
        amount = position.amount_total
        pnl = position.pnl
        price_change = (
            (close_price - entry_price)
            if side == Side.BUY
            else (entry_price - close_price)
        )
        profit = price_change * amount
        commissions = position.commissions
        duration = str(position.duration)
        tp_price = position.get_tp_price()
        sl_price = position.get_sl_price()
        date_completed = datetime.fromtimestamp(
            position.trade_update_time / 1000
        ).strftime("%m/%d/%Y, %H:%M:%S")
        item = [
            date_completed,
            symbol_to_binance(symbol).upper(),
            side.value,
            amount,
            entry_price,
            close_price,
            pnl,
            "%.2f" % profit,
            "%.2f" % commissions,
            duration,
            tp_price,
            sl_price,
        ]
        await self.gs.add_sheet_rows(DIARY_SHEET_NAME, [item])

    async def export_to_chart(self, position):
        tf = "1h"
        start_time = datetime.utcnow()

        end_time = start_time - timedelta(hours=499)

        candles = await self.binance_futures.public.load_candles(
            position.symbol, tf=tf, start_time=start_time, end_time=end_time
        )


if __name__ == "__main__":

    setup_logger()

    logging.info("Starting Trading Diary.")

    diary = TradingDiary(
        API_KEY, API_SECRET, TRADING_DIARY_SPREEDSHEET_ID, GOOGLE_SERVICE_KEY_FILE_NAME
    )

    async def main():
        await diary.init()

        while True:
            await asyncio.sleep(5)

    CoreBase.get_loop().create_task(main())
    # asyncio.run(main())
    CoreBase.get_loop().run_forever()

    # gs = GoogleSheets(spreadsheet_id=spreadsheet_id,
    #                   google_docs_creds_file_name="google-service-key.json")
    # gs.add_sheet_rows("trading", [["test"]])
