import time
from dataclasses import dataclass
from typing import Dict, Tuple, List

import pandas as pd

from utils import log_prices, log_returns, corr, ols_beta, zscore, realized_vol
from mt5_bridge import MT5Connector, RiskManager

@dataclass
class Position:
    a: str
    b: str
    side_a: str
    side_b: str
    vol_a: float
    vol_b: float
    entry_pa: float
    entry_pb: float
    entry_ts: float
    # --- ZMIANA: Dodano pola na numery biletów (Ticket ID) ---
    ticket_a: int = 0
    ticket_b: int = 0

class PortfolioStatArbBot:
    def __init__(self, cfg, feed, mt5c: MT5Connector, rm: RiskManager):
        self.cfg = cfg
        self.feed = feed
        self.mt5c = mt5c
        self.rm = rm
        self.positions: Dict[Tuple[str,str], Position] = {}
        self.cooldown_until_ts: float = 0.0
        self.halted: bool = False
        self.consecutive_errors: int = 0

    def _can_trade(self, checking_entry: bool = False) -> bool:
        if self.halted:
            return False
            
        # Sprawdzamy cooldown tylko przy próbie OTWARCIA (checking_entry=True)
        if checking_entry and time.time() < self.cooldown_until_ts:
            remaining = int((self.cooldown_until_ts - time.time()) / 60)
            print(f"!!! Wejście zablokowane. Cooldown aktywny jeszcze przez {remaining} min.")
            return False
            
        ok, reason = self.rm.is_trading_allowed()
        if not ok:
            self.halted = True
            print("RISK HALT:", reason)
            return False
        return True

    def _regime_ok(self, la: pd.Series, lb: pd.Series) -> bool:
        ra = log_returns(la).tail(400)
        rb = log_returns(lb).tail(400)
        if corr(ra, rb) < self.cfg.min_corr:
            return False
        vola = realized_vol(ra)
        volb = realized_vol(rb)
        base = (vola + volb) / 2.0 if (vola and volb) else max(vola, volb)
        if base == 0:
            return True
        ratio = max(vola, volb) / max(1e-9, min(vola, volb) if min(vola, volb) > 0 else base)
        return ratio <= self.cfg.max_vol_ratio

    def _compute_signal(self, a: str, b: str):
        ca = self.feed.fetch_close(a, self.cfg.timeframe, self.cfg.lookback)
        cb = self.feed.fetch_close(b, self.cfg.timeframe, self.cfg.lookback)

        la = log_prices(ca).tail(self.cfg.lookback)
        lb = log_prices(cb).tail(self.cfg.lookback)
        if len(la) < 120 or len(lb) < 120:
            return ("SKIP", 0.0, 1.0, "not enough data")

        if not self._regime_ok(la, lb) and (a,b) not in self.positions:
            return ("SKIP", 0.0, 1.0, "regime filter")

        beta = ols_beta(la.tail(self.cfg.beta_window), lb.tail(self.cfg.beta_window))
        spread = (la - beta * lb).dropna()
        z = zscore(spread.tail(400))

        key = (a,b)
        if key not in self.positions:
            if z > self.cfg.z_entry:
                return ("ENTER_LONGB_SHORTA", z, beta, "z high")
            if z < -self.cfg.z_entry:
                return ("ENTER_LONGA_SHORTB", z, beta, "z low")
            return ("HOLD", z, beta, "no entry")
        else:
            pos = self.positions[key]
            held_min = (time.time() - pos.entry_ts) / 60.0
            if held_min > self.cfg.max_hold_minutes:
                return ("EXIT", z, beta, f"max hold {held_min:.0f}m")
            if abs(z) < self.cfg.z_exit:
                return ("EXIT", z, beta, "mean reversion")
            return ("HOLD", z, beta, "in position")

    def _sl_for_leg(self, side: str, entry: float) -> float:
        # Oryginalna logika SL: 0.6% ceny
        sl_pct = 0.006
        return entry * (1.0 - sl_pct) if side == "LONG" else entry * (1.0 + sl_pct)

    def _live_enter(self, a: str, b: str, action: str, pa: float, pb: float):
        side_a = "BUY" if action == "ENTER_A_LONG" else "SELL"
        side_b = "SELL" if action == "ENTER_A_LONG" else "BUY"

        vol_a = self.rm.calc_volume_for_risk(a, pa, pa * 0.95)
        vol_b = self.rm.calc_volume_for_risk(b, pb, pb * 0.95)

        ticket_a, ticket_b = 0, 0
        max_retries = 3

        for attempt in range(1, max_retries + 1):
            res_a = self.mt5c.open_position(a, side_a, vol_a)
            if res_a.get("ok"):
                ticket_a = res_a["result"].order
                
                # Próba otwarcia nogi B
                res_b = self.mt5c.open_position(b, side_b, vol_b)
                if res_b.get("ok"):
                    ticket_b = res_b["result"].order
                    break # Sukces!
                else:
                    # Błąd nogi B - musimy zamknąć nogę A i spróbować ponownie całą parę
                    self.mt5c.close_position_by_ticket(ticket_a)
                    ticket_a = 0
                    print(f"Błąd nogi B w próbie {attempt}. Retry...")
            
            time.sleep(1) # sekunda przerwy między próbami

        if ticket_a and ticket_b:
            self.positions[(a, b)] = Position(
                a=a, b=b, side_a=side_a, side_b=side_b,
                vol_a=vol_a, vol_b=vol_b, entry_pa=pa, entry_pb=pb,
                entry_ts=time.time(), ticket_a=ticket_a, ticket_b=ticket_b
            )
        else:
            print(f"!!! Nie udało się otworzyć pary {a}/{b} po {max_retries} próbami.")

    def _live_exit(self, key: Tuple[str,str]):
        pos = self.positions[key]
        print(f"Zamykanie pary {pos.a}/{pos.b}...")

        # Zamykamy nogi i przechwytujemy wyniki z MT5
        res_a = self.mt5c.close_position_by_ticket(pos.ticket_a)
        res_b = self.mt5c.close_position_by_ticket(pos.ticket_b)

        # Pobieramy profit z rezultatów transakcji
        pnl_a = float(res_a.get("result", {}).get("profit", 0.0)) if res_a.get("ok") else 0.0
        pnl_b = float(res_b.get("result", {}).get("profit", 0.0)) if res_b.get("ok") else 0.0
        total_pnl = pnl_a + pnl_b

        # Sprawdzenie straty i aktywacja cooldownu
        if total_pnl < 0:
            self.cooldown_until_ts = time.time() + (30 * 60)
            print(f"!!! POZYCJA ZAMKNIĘTA ZE STRATĄ: {total_pnl:.2f} USD. Start cooldown 30 min.")
        else:
            print(f"Zamknięto z zyskiem: {total_pnl:.2f} USD. Brak blokady.")

        # Usunięcie z portfela (zwalnia miejsce na nową pozycję po cooldownie)
        if key in self.positions:
            del self.positions[key]

    def step(self, pairs: List[Tuple[str,str]]):
        # Ogólne sprawdzenie (np. Daily Loss), które nie blokuje zamykania pozycji
        if not self._can_trade(checking_entry=False):
            return

        # Jeśli mamy już maksymalną liczbę pozycji, sprawdzamy tylko te otwarte (pod kątem EXIT)
        # W przeciwnym razie sprawdzamy listę par ze skanera
        pairs_to_check = list(self.positions.keys()) if len(self.positions) >= self.cfg.max_open_positions else pairs

        for (a, b) in pairs_to_check:
            try:
                action, z, beta, reason = self._compute_signal(a, b)
                pa = self.feed.fetch_last(a)
                pb = self.feed.fetch_last(b)

                stamp = time.strftime("%H:%M:%S")
                print(f"[{stamp}] {a} vs {b} z={z:.2f} act={action} open={len(self.positions)}")

                key = (a, b)
                
                # 1. LOGIKA WYJŚCIA (Zawsze dozwolona, ignoruje cooldown)
                if action == "EXIT" and key in self.positions:
                    self._live_exit(key)
                    print("  <<< LIVE EXIT")

                # 2. LOGIKA WEJŚCIA (Blokowana przez cooldown 30 min po stracie)
                elif action.startswith("ENTER") and key not in self.positions:
                    # Tutaj dodaliśmy kluczowe sprawdzenie cooldownu:
                    if self._can_trade(checking_entry=True): 
                        self._live_enter(a, b, action, pa, pb)
                        print("  >>> LIVE ENTER", action)

                self.consecutive_errors = 0

            except Exception as e:
                self.consecutive_errors += 1
                print("ERROR w pętli step:", repr(e))
                if self.consecutive_errors >= 8:
                    self.halted = True
                    print("RISK HALT: zbyt wiele błędów pod rząd")
                time.sleep(20)

    def report(self):
        print("==== REPORT ====")
        print("Open positions:", len(self.positions))
        ok, reason = self.rm.is_trading_allowed()
        print("Trading allowed:", ok, reason)
        print("===============")
