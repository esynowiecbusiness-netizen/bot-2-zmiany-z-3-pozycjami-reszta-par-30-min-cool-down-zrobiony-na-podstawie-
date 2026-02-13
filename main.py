import os
import time
from dotenv import load_dotenv

from config import default_config
from exchange import make_mt5
from datafeed import DataFeed
from scanner import score_pairs, pick_top_pairs
from portfolio_bot import PortfolioStatArbBot
from mt5_bridge import RiskManager, RiskConfig

def main():
    load_dotenv()
    cfg = default_config()

    env_candidates = os.getenv("CANDIDATES")
    if env_candidates:
        cfg.candidates = [x.strip() for x in env_candidates.split(",") if x.strip()]

    mode = os.getenv("MODE", "LIVE").upper().strip()
    if mode != "LIVE":
        print("Ustaw MODE=LIVE w .env")
        return

    mt5c = make_mt5()
    feed = DataFeed(mt5c)

    risk_pair = float(os.getenv("RISK_PER_PAIR_USD", "10"))
    rm = RiskManager(
        mt5c,
        RiskConfig(
            risk_usd_per_leg=max(0.01, risk_pair / 2.0),
            max_daily_loss_usd=float(os.getenv("MAX_DAILY_LOSS_USD", "250")),
            max_total_drawdown_usd=float(os.getenv("MAX_TOTAL_DD_USD", "500")),
            timezone_name="Europe/Warsaw",
            enforce_kill_switch=True,
        ),
    )
    rm.refresh_start_equity()

    print("Scanning pairs...")
    scored = score_pairs(
        feed=feed,
        candidates=cfg.candidates,
        timeframe=cfg.timeframe,
        lookback=cfg.lookback,
        beta_window=cfg.beta_window,
        min_corr=cfg.min_corr,
        max_vol_ratio=cfg.max_vol_ratio,
    )

    top = pick_top_pairs(scored, cfg.top_pairs)
    print("Top pairs:")
    for a, b in top:
        print(" -", a, "<->", b)

    bot = PortfolioStatArbBot(cfg, feed, mt5c, rm)

    last_report = 0.0
    while True:
        bot.step(top)

        if time.time() - last_report > 15 * 60:
            bot.report()
            last_report = time.time()

        time.sleep(cfg.poll_seconds)

if __name__ == "__main__":
    main()
