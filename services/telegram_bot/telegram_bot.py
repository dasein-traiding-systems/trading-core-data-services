import logging
from enum import Enum

from telegram import (
    ReplyKeyboardMarkup,
    Update,
    constants,
)
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from tc.config import Config
from tc.core.base import CoreBase
from tc.core.db import MongoDb
from tc.core.db.helpers import from_tg_user
from tc.core.exchange.common.mappers import symbol_to_binance
from tc.core.types import SymbolTf
from tc.core.utils.logs import add_traceback
from services.market_prediction import MarketPredictionOracle, SignalCallbackType
from services.telegram_bot.utils import get_user_familiar
from typing import Callable
GREETINGS_TEMPLATE = "{name} ({id}),\r\n welcome to Dasein Trading Systems! 🚀"


class ReplyButtons(Enum):
    VOLUME_LEVELS_ALL_NAME = "🚩 Volume Levels(spot)"
    # RISK_CALCULATOR = "➗ Risk Calculator"

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_


class TradingSysTelegramBot(object):
    def __init__(self, oracle: MarketPredictionOracle, config: Config):
        self.application = Application.builder().token(config.TELEGRAM_BOT_TOKEN).build()
        # self.trading_diary = trading_diary
        self.mongodb = MongoDb(config)
        self.oracle = oracle

    async def send_signal(
        self, symbol_tf: SymbolTf, signal_type: SignalCallbackType, **kwargs
    ):
        logging.warning(f"SIGNAL: {symbol_tf} - {signal_type}")

        users = await self.mongodb.list_users()
        symbol_str = symbol_to_binance(symbol_tf[0]).upper()
        tf_str = symbol_tf[1]

        text = f"Unknown signal {symbol_str} {tf_str} - {signal_type.value} {kwargs}"

        if signal_type == SignalCallbackType.VOLUME_LEVEL:
            text = f"<b>{symbol_str} {tf_str}</b> Volume rush over <code>{round(kwargs['level'])}</code> 🔥"
        elif signal_type == SignalCallbackType.PRICE_LEVEL:
            text = f"<b>{symbol_str} {tf_str}</b> Level break <code>{round(kwargs['level'])}</code> ⚡️"

        for u in users:
            await self.application.bot.send_message(
                chat_id=u["telegram_id"], parse_mode=constants.ParseMode.HTML, text=text
            )

        # async def oracle_loops(self):
        #     try:
        #         await self.oracle.init()
        #         await self.oracle.update_loop()
        #     except Exception as e:
        #         logging.error(add_traceback(e))

    async def start(self):
        try:
            self.application.add_handler(CommandHandler("start", self._start_command))
            self.application.add_handler(
                MessageHandler(filters.TEXT, self._message_handler)
            )

            self.oracle.signal_callback = self.send_signal

            # self.application.add_handler(CallbackQueryHandler(self._section_item_click_callback))
            # CoreBase.get_loop().create_task(self.oracle_loops())
            # await self.application.run_polling()
            await self.application.initialize()
            await self.application.start()
            await self.application.updater.start_polling()
        except Exception as e:
            logging.error(f"TG BOT NOT STARTED: {e}")

    def _get_reply_markup(self):
        reply_keyboard = [[ReplyButtons.VOLUME_LEVELS_ALL_NAME.value]]
        # input_field_placeholder=" 👉 Выберите что вас интересует...",
        return ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True)

    async def _start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        from_user = update.message.from_user

        user = from_tg_user(from_user)
        await self.mongodb.add_user(user)

        name = get_user_familiar(from_user)

        await update.message.reply_text(
            GREETINGS_TEMPLATE.format(name=name, id=from_user.id),
            parse_mode=constants.ParseMode.HTML,
            reply_markup=self._get_reply_markup(),
        )

    async def _message_handler(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        text = update.message.text
        if ReplyButtons.has_value(text):
            reply_button = ReplyButtons(text)
            if reply_button == ReplyButtons.VOLUME_LEVELS_ALL_NAME:
                if not self.oracle.initialized:
                    text = "⏳ Wait for Oracle initialization..."
                else:
                    summary = self.oracle.get_summary()
                    for symbol, tfs in summary.items():
                        lines = [f"▪️ <b>{symbol_to_binance(symbol)}</b>"]

                        for tf, v in tfs.items():
                            prec = self.oracle.api_client.symbol_info[
                                symbol
                            ].price_precision
                            price = self.oracle.api_client.mark_prices[symbol]
                            p_levels = v["price_levels"]
                            up_levels = p_levels[p_levels >= price]
                            up_level = (
                                round(up_levels[-1], prec)
                                if len(up_levels) > 0
                                else "-"
                            )
                            down_levels = p_levels[p_levels <= price]
                            down_level = (
                                round(down_levels[-1], prec)
                                if len(down_levels) > 0
                                else "-"
                            )

                            lines.append(
                                f"{' '*3}▫️ {tf} level: {round(v['dnv_level'])}/{round(v['dnv_current'])}"
                                f"({round(v['dnv_diff'])}%) avg100. {round(v['dnv_avg100'])} "
                                f"\r\n{' '*6}p.levels: [{up_level} > <code>{price}</code> > {down_level}"
                            )

                        text = "\r\n".join(lines)

                        await update.message.reply_text(
                            text,
                            parse_mode=constants.ParseMode.HTML,
                            reply_markup=self._get_reply_markup(),
                        )
