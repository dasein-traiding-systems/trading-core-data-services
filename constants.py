from datetime import timedelta

SKIP_ASSETS = ['BTC', "ETH"]
HIST_INTERVAL = {"1h": timedelta(hours=1),
                 "24h": timedelta(hours=24),
                 "7d": timedelta(days=7)}
