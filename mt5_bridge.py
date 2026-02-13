from __future__ import annotations
import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Optional, Literal, Tuple

import MetaTrader5 as mt5

Side = Literal["BUY", "SELL"]

@dataclass
class RiskConfig:
    risk_usd_per_leg: float = 5.0
    max_daily_loss_usd: float = 50.0
    max_total_drawdown_usd: float = 500.0
    timezone_name: str = "Europe/Warsaw"
    enforce_kill_switch: bool = True

class MT5Connector:
    def __init__(self, login=None, password=None, server=None, path=None, magic=777001, default_deviation=20):
        self.login = login
        self.password = password
        self.server = server
        self.path = path
        self.magic = magic
        self.default_deviation = default_deviation

    def initialize(self) -> None:
        ok = mt5.initialize(self.path) if self.path else mt5.initialize()
        if not ok:
            raise RuntimeError(f"MT5 initialize failed: {mt5.last_error()}")
        if self.login and self.password and self.server:
            if not mt5.login(self.login, password=self.password, server=self.server):
                raise RuntimeError(f"MT5 login failed: {mt5.last_error()}")

    def shutdown(self) -> None:
        mt5.shutdown()

    def ensure_symbol(self, symbol: str) -> None:
        info = mt5.symbol_info(symbol)
        if info is None:
            raise ValueError(f"Symbol not found: {symbol}")
        if not info.visible:
            if not mt5.symbol_select(symbol, True):
                raise RuntimeError(f"symbol_select failed: {mt5.last_error()}")

    def get_ohlcv(self, symbol: str, timeframe: int, n: int):
        self.ensure_symbol(symbol)
        rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, n)
        if rates is None:
            raise RuntimeError(f"copy_rates_from_pos failed: {mt5.last_error()}")
        return rates

    def get_tick(self, symbol: str):
        self.ensure_symbol(symbol)
        t = mt5.symbol_info_tick(symbol)
        if t is None:
            raise RuntimeError(f"symbol_info_tick failed: {mt5.last_error()}")
        return t

    def account_info(self):
        info = mt5.account_info()
        if info is None:
            raise RuntimeError(f"account_info failed: {mt5.last_error()}")
        return info

    def positions(self, symbol: Optional[str] = None):
        if symbol:
            return mt5.positions_get(symbol=symbol) or ()
        return mt5.positions_get() or ()

    def place_market_order(self, symbol: str, side: Side, volume: float, sl: float | None, tp: float | None, comment: str):
        self.ensure_symbol(symbol)
        tick = self.get_tick(symbol)
        order_type = mt5.ORDER_TYPE_BUY if side == "BUY" else mt5.ORDER_TYPE_SELL
        price = float(tick.ask if side == "BUY" else tick.bid)

        req = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": float(volume),
            "type": order_type,
            "price": price,
            "sl": float(sl) if sl else 0.0,
            "tp": float(tp) if tp else 0.0,
            "deviation": int(self.default_deviation),
            "magic": int(self.magic),
            "comment": comment[:31],
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_FOK,
        }
        res = mt5.order_send(req)
        if res is None:
            return {"ok": False, "error": mt5.last_error(), "request": req}
        return {"ok": res.retcode == mt5.TRADE_RETCODE_DONE, "retcode": res.retcode, "result": res._asdict(), "request": req}

    def close_position_by_ticket(self, ticket: int) -> dict:
        """
        Zamyka konkretną pozycję po jej numerze TICKET.
        To jest kluczowe dla kont typu Hedging, aby nie otwierać odwrotnej pozycji.
        """
        # 1. Sprawdzamy czy pozycja istnieje
        positions = mt5.positions_get(ticket=ticket)
        if not positions:
            return {"ok": False, "error": f"Position {ticket} not found in MT5"}
        
        pos = positions[0]
        symbol = pos.symbol
        self.ensure_symbol(symbol)
        
        # 2. Ustalamy typ zlecenia zamykającego (odwrotny do obecnego)
        order_type = mt5.ORDER_TYPE_SELL if pos.type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY
        
        # 3. Cena zamknięcia
        tick = self.get_tick(symbol)
        price = float(tick.bid if order_type == mt5.ORDER_TYPE_SELL else tick.ask)

        req = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": pos.volume,
            "type": order_type,
            "position": ticket,   # <--- KLUCZOWE: ID pozycji zamykanej
            "price": price,
            "deviation": int(self.default_deviation),
            "magic": int(self.magic),
            "comment": "bot_close",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_FOK,
        }
        
        res = mt5.order_send(req)
        if res is None:
            return {"ok": False, "error": mt5.last_error(), "request": req}
        
        return {"ok": res.retcode == mt5.TRADE_RETCODE_DONE, "retcode": res.retcode, "result": res._asdict(), "request": req}

class RiskManager:
    def __init__(self, mt5c: MT5Connector, cfg: RiskConfig):
        self.mt5c = mt5c
        self.cfg = cfg
        self.tz = ZoneInfo(cfg.timezone_name)
        self.start_equity: float | None = None

    def refresh_start_equity(self):
        self.start_equity = float(self.mt5c.account_info().equity)

    def _today_range(self) -> Tuple[datetime, datetime]:
        now = datetime.now(self.tz)
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
        return start.replace(tzinfo=None), end.replace(tzinfo=None)

    def daily_pnl_realized(self) -> float:
        start, end = self._today_range()
        deals = mt5.history_deals_get(start, end)
        if deals is None:
            return 0.0
        return float(sum(float(getattr(d, "profit", 0.0)) for d in deals))

    def floating_pnl(self) -> float:
        return float(sum(float(getattr(p, "profit", 0.0)) for p in self.mt5c.positions()))

    def is_trading_allowed(self) -> tuple[bool, str]:
        info = self.mt5c.account_info()

        if self.cfg.enforce_kill_switch:
            if self.start_equity is None:
                self.refresh_start_equity()
            dd = float(info.equity) - float(self.start_equity)
            if dd <= -abs(self.cfg.max_total_drawdown_usd):
                return False, f"KILL_SWITCH equity DD={dd:.2f}"

        total_today = self.daily_pnl_realized() + self.floating_pnl()
        if total_today <= -abs(self.cfg.max_daily_loss_usd):
            return False, f"DAILY_LIMIT pnl_today={total_today:.2f}"

        return True, "OK"

    def calc_volume_for_risk(self, symbol: str, entry_price: float, sl_price: float) -> float:
        self.mt5c.ensure_symbol(symbol)
        info = mt5.symbol_info(symbol)
        if info is None:
            raise ValueError(f"symbol_info is None: {symbol}")

        dist = abs(entry_price - sl_price)
        if dist <= 0:
            raise ValueError("SL distance is zero")

        vol_min = float(info.volume_min)
        vol_max = float(info.volume_max)
        vol_step = float(info.volume_step)

        tick_size = float(info.trade_tick_size) if info.trade_tick_size else float(info.tick_size)
        tick_value = float(info.trade_tick_value) if info.trade_tick_value else float(info.tick_value)
        if tick_size <= 0 or tick_value <= 0:
            raise RuntimeError(f"Bad tick params: size={tick_size}, value={tick_value}")

        ticks_to_sl = dist / tick_size
        loss_per_lot = ticks_to_sl * tick_value
        raw_vol = self.cfg.risk_usd_per_leg / max(1e-9, loss_per_lot)

        vol = math.floor(raw_vol / vol_step) * vol_step
        vol = max(vol_min, min(vol_max, vol))
        return float(round(vol, 4))

