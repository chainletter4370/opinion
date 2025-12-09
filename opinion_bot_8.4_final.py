"""
Opinion Volume Bot v8.4 - Final Working Version
===============================================
APPROACH:
- SDK methods ONLY (no direct API for private endpoints)
- Markets: Direct API (works)
- Positions/Orders/Trading: SDK methods (authenticated)

This should work now that SDK is properly installed.
"""

import os
import json
import time
import threading
import logging
import requests
from dataclasses import dataclass, field, asdict
from decimal import Decimal, ROUND_DOWN
from enum import Enum
from typing import Optional, Dict, Any, List
from queue import Queue, Empty

# ============================================================
# 0. Environment & Config
# ============================================================
try:
    from dotenv import load_dotenv
    load_dotenv()
except:
    pass

try:
    import nest_asyncio
    nest_asyncio.apply()
except:
    pass

CONFIG = {
    "HOST": "https://proxy.opinion.trade:8443",
    "API_KEY": os.getenv("OPINION_API_KEY"),
    "CHAIN_ID": 56,
    "RPC_URL": os.getenv("OPINION_RPC_URL", "https://bsc-dataseed.binance.org"),
    "PRIVATE_KEY": os.getenv("OPINION_PRIVATE_KEY"),
    "MULTI_SIG_ADDR": os.getenv("OPINION_MULTI_SIG_ADDR"),
    "CONDITIONAL_TOKENS_ADDR": "0xAD1a38cEc043e70E83a3eC30443dB285ED10D774",
    "MULTISEND_ADDR": "0x998739BFdAAdde7C933B942a68053933098f9EDa",
    "TELEGRAM_BOT_TOKEN": os.getenv("TELEGRAM_BOT_TOKEN"),
    "TELEGRAM_CHAT_ID": os.getenv("TELEGRAM_CHAT_ID"),

    "CHECK_INTERVAL": 5.0,
    "MIN_ORDER_SIZE": Decimal("1"),
    "MIN_VALUE_USDT": Decimal("1.5"),
    "PRICE_QUANT": Decimal("0.001"),
    "SPREAD_OFFSET": Decimal("0.001"),

    "STATE_FILE": "opinion_bot_v8_state.json",
}

logging.basicConfig(level=logging.ERROR)

# SDK
try:
    from opinion_clob_sdk import Client
    from opinion_clob_sdk.model import TopicStatusFilter
    from opinion_clob_sdk.chain.py_order_utils.model.order import PlaceOrderDataInput
    from opinion_clob_sdk.chain.py_order_utils.model.sides import OrderSide
    from opinion_clob_sdk.chain.py_order_utils.model.order_type import LIMIT_ORDER
    SDK_AVAILABLE = True
except ImportError as e:
    print(f"❌ SDK import failed: {e}")
    SDK_AVAILABLE = False

try:
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Bot
    from telegram.ext import Updater, CommandHandler, CallbackQueryHandler
    TELEGRAM_AVAILABLE = True
except ImportError:
    TELEGRAM_AVAILABLE = False


# ============================================================
# 1. Utilities
# ============================================================
def q_price(p: Decimal) -> Decimal:
    return p.quantize(CONFIG["PRICE_QUANT"], rounding=ROUND_DOWN)

def safe_decimal(x) -> Optional[Decimal]:
    if x is None:
        return None
    try:
        s = str(x).replace(",", "")
        return Decimal(s)
    except:
        return None

def obj_to_dict(obj: Any) -> Any:
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "model_dump"):
        try:
            return obj.model_dump()
        except:
            pass
    if hasattr(obj, "__dict__"):
        return obj.__dict__
    return obj


class TelegramLogger:
    def __init__(self):
        self._bot = None
        self._chat_id = None
        self._queue = Queue()
        self._running = False

    def setup(self, token, chat_id):
        if TELEGRAM_AVAILABLE and token and chat_id:
            self._bot = Bot(token=token)
            self._chat_id = chat_id
            self._running = True
            threading.Thread(target=self._sender_loop, daemon=True).start()

    def log(self, msg: str):
        print(msg)
        if self._running:
            self._queue.put(msg)

    def _sender_loop(self):
        buf = []
        last_send = time.time()
        while self._running:
            try:
                try:
                    msg = self._queue.get(timeout=1.0)
                    buf.append(msg)
                except Empty:
                    pass

                if buf and (time.time() - last_send >= 2.0 or len(buf) >= 5):
                    text = "\n".join(buf[-15:])
                    try:
                        self._bot.send_message(chat_id=self._chat_id, text=text[:4000])
                    except:
                        pass
                    buf = []
                    last_send = time.time()
            except:
                pass

    def stop(self):
        self._running = False

tg_log = TelegramLogger()


# ============================================================
# 2. Models
# ============================================================
class Phase(Enum):
    BUY = "BUY"
    SELL = "SELL"
    IDLE = "IDLE"

@dataclass
class MarketInfo:
    market_id: int
    title: str
    yes_token_id: str
    no_token_id: str
    child_markets: List[Dict[str, Any]] = field(default_factory=list)
    volume: Decimal = Decimal("0")

@dataclass
class OrderInfo:
    order_id: Optional[str] = None
    price: Optional[Decimal] = None
    amount: Optional[Decimal] = None

@dataclass
class SessionState:
    market: MarketInfo
    total_budget: Decimal = Decimal("0")
    phase: Phase = Phase.IDLE

    yes_position: Decimal = Decimal("0")
    no_position: Decimal = Decimal("0")
    yes_spent: Decimal = Decimal("0")
    no_spent: Decimal = Decimal("0")

    yes_order: OrderInfo = field(default_factory=OrderInfo)
    no_order: OrderInfo = field(default_factory=OrderInfo)

    is_running: bool = False
    force_sell_mode: bool = False

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["phase"] = self.phase.value
        d["market"] = asdict(self.market)
        for k in ["total_budget", "yes_position", "no_position", "yes_spent", "no_spent"]:
            d[k] = str(d[k])
        if d.get("yes_order") and d["yes_order"].get("price"):
            d["yes_order"]["price"] = str(d["yes_order"]["price"])
        if d.get("no_order") and d["no_order"].get("price"):
            d["no_order"]["price"] = str(d["no_order"]["price"])
        if d.get("yes_order") and d["yes_order"].get("amount"):
            d["yes_order"]["amount"] = str(d["yes_order"]["amount"])
        if d.get("no_order") and d["no_order"].get("amount"):
            d["no_order"]["amount"] = str(d["no_order"]["amount"])
        if d.get("market") and d["market"].get("volume"):
            d["market"]["volume"] = str(d["market"]["volume"])
        return d

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "SessionState":
        market_data = d.get("market", {})
        market = MarketInfo(
            market_id=market_data.get("market_id", 0),
            title=market_data.get("title", ""),
            yes_token_id=market_data.get("yes_token_id", ""),
            no_token_id=market_data.get("no_token_id", ""),
            child_markets=market_data.get("child_markets", []),
            volume=Decimal(str(market_data.get("volume", 0)))
        )

        yes_order_data = d.get("yes_order", {})
        yes_order = OrderInfo(
            order_id=yes_order_data.get("order_id"),
            price=Decimal(str(yes_order_data["price"])) if yes_order_data.get("price") else None,
            amount=Decimal(str(yes_order_data["amount"])) if yes_order_data.get("amount") else None
        )

        no_order_data = d.get("no_order", {})
        no_order = OrderInfo(
            order_id=no_order_data.get("order_id"),
            price=Decimal(str(no_order_data["price"])) if no_order_data.get("price") else None,
            amount=Decimal(str(no_order_data["amount"])) if no_order_data.get("amount") else None
        )

        return cls(
            market=market,
            total_budget=Decimal(str(d.get("total_budget", 0))),
            phase=Phase(d.get("phase", "IDLE")),
            yes_position=Decimal(str(d.get("yes_position", 0))),
            no_position=Decimal(str(d.get("no_position", 0))),
            yes_spent=Decimal(str(d.get("yes_spent", 0))),
            no_spent=Decimal(str(d.get("no_spent", 0))),
            yes_order=yes_order,
            no_order=no_order,
            is_running=d.get("is_running", False),
            force_sell_mode=d.get("force_sell_mode", False)
        )


# ============================================================
# 3. Hybrid Client (Direct API + SDK)
# ============================================================
class OpinionClient:
    def __init__(self):
        """Initialize with SDK + Direct API for markets"""
        if not SDK_AVAILABLE:
            raise RuntimeError("SDK required but not available")

        # Direct API session for public endpoints
        self.session = requests.Session()
        self.session.headers.update({
            "accept": "application/json",
            "apikey": CONFIG["API_KEY"]
        })

        # SDK for authenticated operations
        tg_log.log("🔧 Initializing SDK...")
        try:
            self.sdk = Client(
                host=CONFIG["HOST"],
                apikey=CONFIG["API_KEY"],
                chain_id=CONFIG["CHAIN_ID"],
                rpc_url=CONFIG["RPC_URL"],
                private_key=CONFIG["PRIVATE_KEY"],
                multi_sig_addr=CONFIG["MULTI_SIG_ADDR"],
                conditional_tokens_addr=CONFIG["CONDITIONAL_TOKENS_ADDR"],
                multisend_addr=CONFIG["MULTISEND_ADDR"]
            )
            self.sdk.enable_trading()
            tg_log.log("✅ SDK initialized and trading enabled")
        except Exception as e:
            tg_log.log(f"❌ SDK init failed: {e}")
            raise

    def get_markets(self, page=1, limit=20):
        """Get markets via direct API (faster)"""
        try:
            url = f"{CONFIG['HOST']}/api/bsc/api/v2/topic"
            params = {"page": page, "limit": limit, "chainId": CONFIG["CHAIN_ID"], "status": "2"}

            resp = self.session.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            if data.get("errno") != 0:
                return []

            raw_list = data.get("result", {}).get("list", [])
            markets = []

            for r in raw_list:
                try:
                    market_id = r.get("topicId") or 0
                    if not market_id:
                        continue

                    markets.append(MarketInfo(
                        market_id=int(market_id),
                        title=str(r.get("title", "Unknown")),
                        yes_token_id=str(r.get("yesPos", "")),
                        no_token_id=str(r.get("noPos", "")),
                        child_markets=r.get("childList", []),
                        volume=safe_decimal(r.get("volume", 0)) or Decimal("0")
                    ))
                except:
                    continue

            return markets
        except Exception as e:
            tg_log.log(f"❌ get_markets error: {e}")
            return []

    def get_market(self, mid: int):
        """Get market via direct API"""
        try:
            url = f"{CONFIG['HOST']}/api/bsc/api/v2/topic/{mid}"
            params = {"chainId": CONFIG["CHAIN_ID"]}

            resp = self.session.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            if data.get("errno") != 0:
                return None

            r = data.get("result", {})
            return MarketInfo(
                market_id=int(r.get("topicId", mid)),
                title=str(r.get("title", "Unknown")),
                yes_token_id=str(r.get("yesPos", "")),
                no_token_id=str(r.get("noPos", "")),
                child_markets=r.get("childList", []),
                volume=safe_decimal(r.get("volume", 0)) or Decimal("0")
            )
        except:
            return None

    def get_best_prices(self, tid):
        """Get orderbook via SDK"""
        try:
            resp = self.sdk.get_orderbook(tid)
            if hasattr(resp, "errno") and resp.errno != 0:
                return None, None

            result = resp.result if hasattr(resp, "result") else resp
            bids = getattr(result, "bids", []) or []
            asks = getattr(result, "asks", []) or []

            bb = Decimal(str(bids[0].price)) if bids else None
            ba = Decimal(str(asks[0].price)) if asks else None

            return bb, ba
        except:
            return None, None

    def get_my_positions(self, limit=100):
        """Get positions via SDK (authenticated)"""
        try:
            tg_log.log(f"📡 SDK: get_my_positions(limit={limit})")
            resp = self.sdk.get_my_positions(limit=limit)

            if hasattr(resp, "errno") and resp.errno != 0:
                tg_log.log(f"❌ SDK error: {resp.errmsg}")
                return []

            result = resp.result if hasattr(resp, "result") else resp

            # Extract list
            if isinstance(result, list):
                positions = result
            else:
                d = obj_to_dict(result)
                positions = []
                for key in ["list", "data", "items"]:
                    if isinstance(d.get(key), list):
                        positions = d[key]
                        break

            tg_log.log(f"✅ Got {len(positions)} positions")
            return [obj_to_dict(p) for p in positions]

        except Exception as e:
            tg_log.log(f"❌ get_my_positions error: {e}")
            return []

    def get_my_open_orders(self, mid):
        """Get open orders via SDK"""
        try:
            all_orders = []
            page = 1
            limit = 50

            while True:
                resp = self.sdk.get_my_orders(market_id=mid, page=page, limit=limit)
                if hasattr(resp, "errno") and resp.errno != 0:
                    break

                result = resp.result if hasattr(resp, "result") else resp
                d = obj_to_dict(result)

                raw = []
                if isinstance(d, list):
                    raw = d
                elif isinstance(d, dict):
                    for k in ["list", "data"]:
                        if d.get(k):
                            raw = d[k]
                            break

                if not raw:
                    break

                for r in raw:
                    st = str(r.get("status") or "").upper()
                    if "OPEN" in st or "PENDING" in st:
                        all_orders.append(r)

                if len(raw) < limit:
                    break
                page += 1

            return all_orders
        except:
            return []

    def place_order(self, mid, tid, is_buy, price, amount, is_quote):
        """Place order via SDK"""
        try:
            order = PlaceOrderDataInput(
                marketId=mid,
                tokenId=tid,
                side=OrderSide.BUY if is_buy else OrderSide.SELL,
                orderType=LIMIT_ORDER,
                price=str(price)
            )

            if is_quote:
                order.makerAmountInQuoteToken = float(amount)
            else:
                order.makerAmountInBaseToken = float(amount)

            resp = self.sdk.place_order(order, check_approval=True)

            if hasattr(resp, "errno") and resp.errno != 0:
                return None, resp.errmsg

            result = resp.result if hasattr(resp, "result") else resp
            d = obj_to_dict(result)

            if not isinstance(d, dict):
                return str(resp), None

            oid = d.get("order_id") or d.get("orderId") or (d.get("orderData") or {}).get("orderId")
            return oid, None

        except Exception as e:
            return None, str(e)

    def cancel_order(self, oid):
        """Cancel order via SDK"""
        try:
            self.sdk.cancel_order(oid)
            return True
        except:
            return False

    def cancel_all_orders(self, mid):
        """Cancel all orders via SDK"""
        try:
            self.sdk.cancel_all_orders(mid)
            return True
        except:
            return False


# ============================================================
# 4. Strategy Manager (Simplified - no auto trading for now)
# ============================================================
class StrategyManager:
    def __init__(self):
        self.client = None
        self.sessions: Dict[int, SessionState] = {}
        self._lock = threading.RLock()
        self._running = False

    def initialize(self):
        try:
            self.client = OpinionClient()
            self._load_state()
            return True
        except Exception as e:
            tg_log.log(f"❌ Init failed: {e}")
            import traceback
            tg_log.log(traceback.format_exc())
            return False

    def _save_state(self):
        try:
            with self._lock:
                data = {mid: s.to_dict() for mid, s in self.sessions.items()}
            with open(CONFIG["STATE_FILE"], "w") as f:
                json.dump(data, f, indent=2)
        except:
            pass

    def _load_state(self):
        try:
            if os.path.exists(CONFIG["STATE_FILE"]):
                with open(CONFIG["STATE_FILE"], "r") as f:
                    data = json.load(f)
                with self._lock:
                    for mid_str, s_dict in data.items():
                        mid = int(mid_str)
                        self.sessions[mid] = SessionState.from_dict(s_dict)
                tg_log.log(f"✅ Loaded {len(self.sessions)} sessions")
        except:
            pass

    def start_loop(self):
        self._running = True
        threading.Thread(target=self._main_loop, daemon=True).start()

    def _main_loop(self):
        """Background monitoring loop"""
        while self._running:
            try:
                # Periodic position sync
                all_positions = self.client.get_my_positions(limit=100)

                with self._lock:
                    for mid in list(self.sessions.keys()):
                        s = self.sessions[mid]
                        if not s.is_running:
                            continue

                        # Update positions
                        for p in all_positions:
                            p_mid = int(p.get("topicId") or p.get("topic_id") or 0)
                            if p_mid == mid:
                                tid = str(p.get("tokenId") or p.get("token_id"))
                                amt = safe_decimal(p.get("amount") or 0) or Decimal(0)

                                if tid == s.market.yes_token_id:
                                    s.yes_position = amt
                                elif tid == s.market.no_token_id:
                                    s.no_position = amt

                self._save_state()
                time.sleep(CONFIG["CHECK_INTERVAL"])

            except Exception as e:
                tg_log.log(f"❌ Loop error: {e}")
                time.sleep(10)


# ============================================================
# 5. Telegram Bot
# ============================================================
class TelegramBot:
    def __init__(self, mgr: StrategyManager):
        self.mgr = mgr
        tg_log.setup(CONFIG["TELEGRAM_BOT_TOKEN"], CONFIG["TELEGRAM_CHAT_ID"])
        self.updater = Updater(token=CONFIG["TELEGRAM_BOT_TOKEN"], use_context=True)
        dp = self.updater.dispatcher

        dp.add_handler(CommandHandler("start", self.start))
        dp.add_handler(CommandHandler("help", self.help))
        dp.add_handler(CommandHandler("markets", self.markets))
        dp.add_handler(CommandHandler("market", self.market))
        dp.add_handler(CommandHandler("position", self.position))
        dp.add_handler(CallbackQueryHandler(self.page_callback, pattern=r"^p_"))

        self._markets_cache = []
        self.updater.start_polling()

    def _auth(self, u):
        return str(u.effective_chat.id) == str(CONFIG["TELEGRAM_CHAT_ID"])

    def start(self, u, c):
        if self._auth(u):
            u.message.reply_text("🤖 Opinion Bot v8.4\n\n✅ Monitoring active\n⚠️ Auto-trading disabled (manual /run required)")

    def help(self, u, c):
        if not self._auth(u):
            return
        msg = """📚 Commands:

✅ Working:
/markets - List markets
/market <id> - Market details
/position - Your positions

⚠️ Manual only:
/run <id> <budget> - Start trading (coming soon)

Use /markets to browse and /position to check holdings"""
        u.message.reply_text(msg)

    def markets(self, u, c):
        if not self._auth(u):
            return
        u.message.reply_text("🔄 Fetching...")

        self._markets_cache = []
        for page in range(1, 11):  # Max 10 pages
            ms = self.mgr.client.get_markets(page=page, limit=20)
            if not ms:
                break
            self._markets_cache.extend(ms)
            if len(ms) < 20:
                break

        if not self._markets_cache:
            return u.message.reply_text("❌ No markets found")

        text, kb = self._format_page(0)
        u.message.reply_text(text, reply_markup=kb)

    def _format_page(self, page):
        per = 10
        total = len(self._markets_cache)
        max_p = (total + per - 1) // per
        start = page * per
        end = min(start + per, total)

        txt = f"📊 Markets (Page {page+1}/{max_p}, Total: {total})\n\n"
        for m in self._markets_cache[start:end]:
            txt += f"[{m.market_id}] {m.title[:35]}\n"
            txt += f"   Vol: ${m.volume:,.0f}\n\n"

        btns = []
        if page > 0:
            btns.append(InlineKeyboardButton("⬅️", callback_data=f"p_{page-1}"))
        if page < max_p - 1:
            btns.append(InlineKeyboardButton("➡️", callback_data=f"p_{page+1}"))

        return txt, InlineKeyboardMarkup([btns]) if btns else None

    def page_callback(self, u, c):
        q = u.callback_query
        if not self._auth(u):
            return
        q.answer()
        try:
            page = int(q.data.split("_")[1])
            t, k = self._format_page(page)
            q.edit_message_text(t, reply_markup=k)
        except:
            pass

    def market(self, u, c):
        if not self._auth(u):
            return
        try:
            mid = int(c.args[0])
            m = self.mgr.client.get_market(mid)
            if not m:
                return u.message.reply_text("❌ Not found")

            txt = f"📝 Market #{m.market_id}\n\n"
            txt += f"{m.title}\n\n"
            txt += f"Volume: ${m.volume:,.2f}\n"
            txt += f"YES: {m.yes_token_id[:16]}...\n"
            txt += f"NO: {m.no_token_id[:16]}...\n"

            if m.child_markets:
                txt += f"\n🔗 Children: {len(m.child_markets)}\n"
                for idx, child in enumerate(m.child_markets[:3], 1):
                    if isinstance(child, dict):
                        txt += f"  {idx}. [{child.get('topicId')}] {child.get('title', '?')[:25]}\n"

            u.message.reply_text(txt)
        except:
            u.message.reply_text("Usage: /market <id>")

    def position(self, u, c):
        if not self._auth(u):
            return
        u.message.reply_text("🔍 Fetching...")

        pos = self.mgr.client.get_my_positions(limit=100)
        if not pos:
            return u.message.reply_text("✅ No positions")

        msg = "🎒 Your Positions:\n\n"
        for p in pos:
            try:
                mid = int(p.get("topicId") or p.get("topic_id") or 0)
                if mid == 0:
                    continue

                amt = safe_decimal(p.get("amount") or 0) or Decimal(0)
                val = safe_decimal(p.get("currentValue") or 0) or Decimal(0)

                if val < 0.1:
                    continue

                outcome = p.get("outcome") or "?"
                title = p.get('topicTitle', '')[:20]
                msg += f"[#{mid}] {title}\n"
                msg += f"  {outcome}: {amt:.2f} (${val:.2f})\n\n"
            except:
                pass

        u.message.reply_text(msg[:4000])


# ============================================================
# 6. Main
# ============================================================
if __name__ == "__main__":
    if not CONFIG["API_KEY"]:
        print("❌ Missing OPINION_API_KEY")
    elif not CONFIG["TELEGRAM_BOT_TOKEN"]:
        print("❌ Missing TELEGRAM_BOT_TOKEN")
    elif not CONFIG["TELEGRAM_CHAT_ID"]:
        print("❌ Missing TELEGRAM_CHAT_ID")
    else:
        print("🚀 Opinion Bot v8.4 Starting...")

        mgr = StrategyManager()
        if mgr.initialize():
            mgr.start_loop()
            tg = TelegramBot(mgr)
            tg_log.log("✅ Bot v8.4 started - Monitoring active")

            try:
                while True:
                    time.sleep(10)
            except KeyboardInterrupt:
                print("\n🛑 Shutting down...")
                tg_log.stop()
        else:
            print("❌ Initialization failed")
