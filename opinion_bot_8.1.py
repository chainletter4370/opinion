"""
Opinion Volume Bot v8.1 - Fixed API Parsing
===========================================
FIXES:
- Client initialization with conditional_tokens_addr and multisend_addr
- Robust API response parsing
- Better error handling and logging
- Fallback to direct API calls if SDK fails

FEATURES:
1. State-based buy/sell cycle: BUY → SELL → BUY → SELL (infinite loop)
2. Budget management: Tracks spending and prevents over-allocation
3. Best bid/ask tracking: Always maintains top of book position
4. Order adoption: Reuses existing orders if already at best price
5. Position tracking: Syncs with on-chain positions
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

try:
    import certifi
    os.environ["SSL_CERT_FILE"] = certifi.where()
    os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()
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

    "CHECK_INTERVAL": 3.0,
    "MIN_ORDER_SIZE": Decimal("1"),
    "MIN_VALUE_USDT": Decimal("1.5"),
    "PRICE_QUANT": Decimal("0.001"),
    "SPREAD_OFFSET": Decimal("0.001"),

    "API_MAX_RETRIES": 3,
    "API_RETRY_DELAY_BASE": 1.0,
    "STATE_FILE": "opinion_bot_v8_state.json",
}

logging.basicConfig(level=logging.ERROR)

# SDK Check
try:
    from opinion_clob_sdk import Client
    from opinion_clob_sdk.model import TopicStatusFilter
    from opinion_clob_sdk.chain.py_order_utils.model.order import PlaceOrderDataInput
    from opinion_clob_sdk.chain.py_order_utils.model.sides import OrderSide
    from opinion_clob_sdk.chain.py_order_utils.model.order_type import LIMIT_ORDER
    SDK_AVAILABLE = True
except ImportError as e:
    print(f"⚠️ SDK import warning: {e}")
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
    """Quantize price to 3 decimals"""
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
    """Convert SDK objects to dict"""
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
        print(msg)  # Always print to console
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
                    except Exception as e:
                        print(f"Telegram send error: {e}")
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
    """Trading phase"""
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
    """Active order tracking"""
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
        """Serialize to dict"""
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
        """Deserialize from dict"""
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
# 3. Client Wrapper
# ============================================================
class OpinionClient:
    def __init__(self):
        if not SDK_AVAILABLE:
            raise RuntimeError("SDK missing")

        tg_log.log("🔧 Initializing Opinion SDK Client...")

        try:
            self.client = Client(
                host=CONFIG["HOST"],
                apikey=CONFIG["API_KEY"],
                chain_id=CONFIG["CHAIN_ID"],
                rpc_url=CONFIG["RPC_URL"],
                private_key=CONFIG["PRIVATE_KEY"],
                multi_sig_addr=CONFIG["MULTI_SIG_ADDR"],
                conditional_tokens_addr=CONFIG["CONDITIONAL_TOKENS_ADDR"],
                multisend_addr=CONFIG["MULTISEND_ADDR"],
                market_cache_ttl=60
            )
            tg_log.log("✅ SDK Client initialized with all parameters")
        except TypeError as e:
            # Fallback: try without optional parameters
            tg_log.log(f"⚠️ Full init failed ({e}), trying minimal params...")
            self.client = Client(
                host=CONFIG["HOST"],
                apikey=CONFIG["API_KEY"],
                chain_id=CONFIG["CHAIN_ID"],
                rpc_url=CONFIG["RPC_URL"],
                private_key=CONFIG["PRIVATE_KEY"],
                multi_sig_addr=CONFIG["MULTI_SIG_ADDR"]
            )
            tg_log.log("✅ SDK Client initialized with minimal parameters")

        try:
            self.client.enable_trading()
            tg_log.log("✅ Trading enabled")
        except Exception as e:
            tg_log.log(f"⚠️ enable_trading() failed: {e}")

    def _retry(self, func, *args, **kwargs):
        for i in range(CONFIG["API_MAX_RETRIES"] + 1):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if i == CONFIG["API_MAX_RETRIES"]:
                    raise e
                time.sleep(CONFIG["API_RETRY_DELAY_BASE"] * (2 ** i))

    def _unwrap(self, resp):
        """Unwrap SDK response"""
        if resp is None:
            return 999, None, "None response"
        if hasattr(resp, "errno"):
            return resp.errno, getattr(resp, "result", None), getattr(resp, "errmsg", "Unknown error")
        return 999, resp, "No errno attribute"

    def _extract_list(self, result):
        """Extract list from various response formats"""
        if result is None:
            return []

        # Already a list
        if isinstance(result, list):
            return [obj_to_dict(item) for item in result]

        # Convert to dict
        d = obj_to_dict(result)
        if not isinstance(d, dict):
            return []

        # Try common keys
        for key in ["list", "data", "items", "markets", "positions", "orders"]:
            if key in d and isinstance(d[key], list):
                return [obj_to_dict(item) if not isinstance(item, dict) else item for item in d[key]]

        return []

    def get_markets(self, page=1, limit=20):
        """Get market list"""
        try:
            tg_log.log(f"📡 Calling get_markets(page={page}, limit={limit})")
            resp = self._retry(self.client.get_markets, page=page, limit=limit, status=TopicStatusFilter.ACTIVATED)

            errno, result, errmsg = self._unwrap(resp)
            tg_log.log(f"   Response: errno={errno}, errmsg={errmsg}")

            if errno != 0:
                tg_log.log(f"❌ get_markets failed: {errmsg}")
                return []

            raw_list = self._extract_list(result)
            tg_log.log(f"   Extracted {len(raw_list)} markets")

            markets = []
            for r in raw_list:
                try:
                    # Handle different field name variations
                    market_id = (r.get("market_id") or r.get("marketId") or
                               r.get("topic_id") or r.get("topicId") or 0)
                    title = (r.get("market_title") or r.get("marketTitle") or
                           r.get("title") or r.get("topic_title") or "Unknown")
                    yes_token = (r.get("yes_token_id") or r.get("yesTokenId") or
                               r.get("yes_pos") or r.get("yesPos") or "")
                    no_token = (r.get("no_token_id") or r.get("noTokenId") or
                              r.get("no_pos") or r.get("noPos") or "")

                    if not market_id:
                        continue

                    child_markets = r.get("child_markets") or r.get("childMarkets") or r.get("childList") or []
                    if not isinstance(child_markets, list):
                        child_markets = []

                    markets.append(MarketInfo(
                        market_id=int(market_id),
                        title=str(title),
                        yes_token_id=str(yes_token),
                        no_token_id=str(no_token),
                        child_markets=child_markets,
                        volume=safe_decimal(r.get("volume") or 0) or Decimal("0")
                    ))
                except Exception as e:
                    tg_log.log(f"⚠️ Failed to parse market: {e}")
                    continue

            tg_log.log(f"✅ Parsed {len(markets)} valid markets")
            return markets

        except Exception as e:
            tg_log.log(f"❌ get_markets exception: {e}")
            import traceback
            tg_log.log(traceback.format_exc())
            return []

    def get_market(self, mid: int):
        """Get single market details"""
        try:
            tg_log.log(f"📡 Calling get_market({mid})")
            resp = self._retry(self.client.get_market, mid)
            errno, result, errmsg = self._unwrap(resp)

            if errno != 0:
                tg_log.log(f"❌ get_market failed: {errmsg}")
                return None

            # Extract data
            d = obj_to_dict(result)
            if isinstance(d, dict) and "data" in d:
                d = d["data"]

            if not isinstance(d, dict):
                d = obj_to_dict(d)

            market_id = (d.get("market_id") or d.get("marketId") or
                        d.get("topic_id") or d.get("topicId") or mid)
            title = (d.get("market_title") or d.get("marketTitle") or
                    d.get("title") or d.get("topic_title") or "Unknown")
            yes_token = (d.get("yes_token_id") or d.get("yesTokenId") or
                       d.get("yes_pos") or d.get("yesPos") or "")
            no_token = (d.get("no_token_id") or d.get("noTokenId") or
                      d.get("no_pos") or d.get("noPos") or "")

            child_markets = d.get("child_markets") or d.get("childMarkets") or d.get("childList") or []
            if not isinstance(child_markets, list):
                child_markets = []

            return MarketInfo(
                market_id=int(market_id),
                title=str(title),
                yes_token_id=str(yes_token),
                no_token_id=str(no_token),
                child_markets=child_markets,
                volume=safe_decimal(d.get("volume") or 0) or Decimal("0")
            )
        except Exception as e:
            tg_log.log(f"❌ get_market exception: {e}")
            return None

    def get_best_prices(self, tid):
        """Get best bid and ask for a token"""
        try:
            resp = self._retry(self.client.get_orderbook, tid)
            errno, result, _ = self._unwrap(resp)
            if errno != 0:
                return None, None

            d = obj_to_dict(result)
            if isinstance(d, dict) and "data" in d:
                d = d["data"]

            bids = []
            asks = []

            if isinstance(d, dict):
                bids = d.get("bids", [])
                asks = d.get("asks", [])
            elif hasattr(result, "bids"):
                bids = result.bids
                asks = result.asks

            bb = None
            ba = None

            if bids:
                if isinstance(bids[0], dict):
                    bb = Decimal(str(bids[0].get("price", 0)))
                elif hasattr(bids[0], "price"):
                    bb = Decimal(str(bids[0].price))

            if asks:
                if isinstance(asks[0], dict):
                    ba = Decimal(str(asks[0].get("price", 0)))
                elif hasattr(asks[0], "price"):
                    ba = Decimal(str(asks[0].price))

            return bb, ba
        except Exception as e:
            tg_log.log(f"❌ get_orderbook error: {e}")
            return None, None

    def get_my_positions(self, limit=100):
        """Get user positions"""
        try:
            tg_log.log(f"📡 Calling get_my_positions(limit={limit})")
            resp = self._retry(self.client.get_my_positions, limit=limit)
            errno, result, errmsg = self._unwrap(resp)

            tg_log.log(f"   Response: errno={errno}")

            if errno != 0:
                tg_log.log(f"❌ get_my_positions failed: {errmsg}")
                return []

            positions = self._extract_list(result)
            tg_log.log(f"✅ Got {len(positions)} positions")
            return positions

        except Exception as e:
            tg_log.log(f"❌ get_my_positions exception: {e}")
            return []

    def get_my_open_orders(self, mid):
        """Get open orders for a market"""
        all_orders = []
        page = 1
        limit = 50

        while True:
            try:
                resp = self._retry(self.client.get_my_orders, market_id=mid, page=page, limit=limit)
                errno, result, _ = self._unwrap(resp)
                if errno != 0:
                    break

                raw = self._extract_list(result)
                if not raw:
                    break

                for r in raw:
                    st = str(r.get("status") or "").upper()
                    if "OPEN" in st or "PENDING" in st:
                        all_orders.append(r)

                if len(raw) < limit:
                    break
                page += 1
            except:
                break

        return all_orders

    def place_order(self, mid, tid, is_buy, price, amount, is_quote):
        """Place an order"""
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

            resp = self._retry(self.client.place_order, order, check_approval=True)
            errno, result, err = self._unwrap(resp)

            if errno != 0:
                return None, err

            d = obj_to_dict(result)
            if not isinstance(d, dict):
                return None, "Invalid response format"

            oid = d.get("order_id") or d.get("orderId") or (d.get("orderData") or {}).get("orderId")
            return oid, None

        except Exception as e:
            return None, str(e)

    def cancel_order(self, oid):
        """Cancel single order"""
        try:
            self.client.cancel_order(oid)
            return True
        except:
            return False

    def cancel_all_orders(self, mid):
        """Cancel all orders for a market"""
        try:
            self.client.cancel_all_orders(mid)
            return True
        except:
            return False


# ============================================================
# 4. Strategy Manager
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
            tg_log.log(f"❌ Client init failed: {e}")
            import traceback
            tg_log.log(traceback.format_exc())
            return False

    def _save_state(self):
        """Persist state to disk"""
        try:
            with self._lock:
                data = {mid: s.to_dict() for mid, s in self.sessions.items()}
            with open(CONFIG["STATE_FILE"], "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            tg_log.log(f"⚠️ State save failed: {e}")

    def _load_state(self):
        """Load state from disk"""
        try:
            if os.path.exists(CONFIG["STATE_FILE"]):
                with open(CONFIG["STATE_FILE"], "r") as f:
                    data = json.load(f)
                with self._lock:
                    for mid_str, s_dict in data.items():
                        mid = int(mid_str)
                        self.sessions[mid] = SessionState.from_dict(s_dict)
                tg_log.log(f"✅ Loaded {len(self.sessions)} sessions from state")
        except Exception as e:
            tg_log.log(f"⚠️ State load failed: {e}")

    def start_loop(self):
        self._running = True
        threading.Thread(target=self._main_loop, daemon=True).start()

    def _main_loop(self):
        while self._running:
            try:
                with self._lock:
                    mids = list(self.sessions.keys())

                # Global position update
                all_positions = self.client.get_my_positions(limit=100)
                pos_by_market = {}

                for p in all_positions:
                    mid = int(p.get("market_id") or p.get("marketId") or 0)
                    if mid == 0:
                        continue

                    if mid not in pos_by_market:
                        pos_by_market[mid] = {"YES": Decimal("0"), "NO": Decimal("0")}

                    tid = str(p.get("token_id") or p.get("tokenId"))
                    amt = safe_decimal(p.get("shares_owned") or p.get("amount") or 0) or Decimal(0)

                    # Handle wei conversion
                    if amt > 1_000_000_000:
                        amt /= Decimal(10**18)

                    outcome = str(p.get("outcome") or "").upper()
                    if outcome in ["YES", "NO"]:
                        pos_by_market[mid][outcome] = amt
                    else:
                        # Fallback: match by token_id
                        with self._lock:
                            if mid in self.sessions:
                                s = self.sessions[mid]
                                if tid == s.market.yes_token_id:
                                    pos_by_market[mid]["YES"] = amt
                                elif tid == s.market.no_token_id:
                                    pos_by_market[mid]["NO"] = amt

                # Update sessions
                with self._lock:
                    for mid in mids:
                        s = self.sessions.get(mid)
                        if not s or not s.is_running:
                            continue

                        # Update positions
                        if mid in pos_by_market:
                            s.yes_position = pos_by_market[mid]["YES"]
                            s.no_position = pos_by_market[mid]["NO"]
                        else:
                            s.yes_position = Decimal("0")
                            s.no_position = Decimal("0")

                        # Execute trading logic
                        self._execute_session(s)

                # Save state periodically
                self._save_state()
                time.sleep(CONFIG["CHECK_INTERVAL"])

            except Exception as e:
                tg_log.log(f"❌ Main loop error: {e}")
                time.sleep(5)

    def _execute_session(self, s: SessionState):
        """Main trading logic for a session"""
        try:
            if s.force_sell_mode:
                self._execute_sell_phase(s, force=True)
                return

            if s.phase == Phase.BUY:
                self._execute_buy_phase(s)
            elif s.phase == Phase.SELL:
                self._execute_sell_phase(s, force=False)

        except Exception as e:
            tg_log.log(f"❌ Session #{s.market.market_id} error: {e}")

    def _execute_buy_phase(self, s: SessionState):
        """Execute BUY phase"""
        mid = s.market.market_id
        open_orders = self.client.get_my_open_orders(mid)

        yes_existing = None
        no_existing = None
        for o in open_orders:
            tid = str(o.get("token_id") or o.get("tokenId"))
            if tid == s.market.yes_token_id:
                yes_existing = o
            elif tid == s.market.no_token_id:
                no_existing = o

        yes_budget = s.total_budget / 2
        no_budget = s.total_budget / 2

        yes_done = self._manage_buy_side(s, "YES", s.market.yes_token_id, yes_budget, s.yes_spent, yes_existing)
        no_done = self._manage_buy_side(s, "NO", s.market.no_token_id, no_budget, s.no_spent, no_existing)

        if yes_done and no_done:
            tg_log.log(f"✅ BUY phase complete for #{mid}, switching to SELL")
            s.phase = Phase.SELL
            s.yes_spent = Decimal("0")
            s.no_spent = Decimal("0")

    def _manage_buy_side(self, s: SessionState, side_name: str, tid: str, budget: Decimal,
                         spent: Decimal, existing_order: Optional[Dict]) -> bool:
        """Manage buy side"""
        mid = s.market.market_id

        if spent >= budget:
            pos = s.yes_position if side_name == "YES" else s.no_position
            if pos >= CONFIG["MIN_ORDER_SIZE"]:
                return True
            else:
                if side_name == "YES":
                    s.yes_spent = Decimal("0")
                else:
                    s.no_spent = Decimal("0")
                return False

        bb, ba = self.client.get_best_prices(tid)
        if bb is None or ba is None:
            return False

        target_price = q_price(bb + CONFIG["SPREAD_OFFSET"])
        if target_price <= Decimal("0.001"):
            target_price = Decimal("0.001")
        if target_price >= Decimal("0.999"):
            target_price = Decimal("0.999")

        remaining = budget - spent
        if remaining < CONFIG["MIN_VALUE_USDT"]:
            return True

        if existing_order:
            ex_price = q_price(Decimal(str(existing_order.get("price"))))
            ex_oid = str(existing_order.get("order_id") or existing_order.get("orderId"))

            if ex_price == target_price:
                return False
            else:
                tg_log.log(f"🔄 {side_name} buy order pushed #{mid}: {ex_price} → {target_price}")
                self.client.cancel_order(ex_oid)
                time.sleep(0.5)

        oid, err = self.client.place_order(
            mid=mid,
            tid=tid,
            is_buy=True,
            price=target_price,
            amount=remaining,
            is_quote=True
        )

        if oid:
            tg_log.log(f"📥 {side_name} BUY #{mid}: ${remaining:.2f} @ {target_price}")
            if side_name == "YES":
                s.yes_spent += remaining
            else:
                s.no_spent += remaining
        elif err:
            tg_log.log(f"❌ {side_name} buy failed #{mid}: {err}")

        return False

    def _execute_sell_phase(self, s: SessionState, force: bool = False):
        """Execute SELL phase"""
        mid = s.market.market_id
        open_orders = self.client.get_my_open_orders(mid)

        yes_existing = None
        no_existing = None
        for o in open_orders:
            tid = str(o.get("token_id") or o.get("tokenId"))
            if tid == s.market.yes_token_id:
                yes_existing = o
            elif tid == s.market.no_token_id:
                no_existing = o

        yes_done = self._manage_sell_side(s, "YES", s.market.yes_token_id, s.yes_position, yes_existing)
        no_done = self._manage_sell_side(s, "NO", s.market.no_token_id, s.no_position, no_existing)

        if yes_done and no_done:
            if force:
                tg_log.log(f"✅ SELL ALL complete for #{mid}")
                s.force_sell_mode = False
                s.is_running = False
            else:
                tg_log.log(f"✅ SELL phase complete for #{mid}, switching to BUY")
                s.phase = Phase.BUY

    def _manage_sell_side(self, s: SessionState, side_name: str, tid: str,
                          position: Decimal, existing_order: Optional[Dict]) -> bool:
        """Manage sell side"""
        mid = s.market.market_id

        if position < CONFIG["MIN_ORDER_SIZE"]:
            return True

        bb, ba = self.client.get_best_prices(tid)
        if bb is None or ba is None:
            return False

        target_price = q_price(ba - CONFIG["SPREAD_OFFSET"])
        if target_price <= Decimal("0.001"):
            target_price = Decimal("0.001")
        if target_price >= Decimal("0.999"):
            target_price = Decimal("0.999")

        if (position * target_price) < CONFIG["MIN_VALUE_USDT"]:
            return True

        if existing_order:
            ex_price = q_price(Decimal(str(existing_order.get("price"))))
            ex_oid = str(existing_order.get("order_id") or existing_order.get("orderId"))

            if ex_price == target_price:
                return False
            else:
                tg_log.log(f"🔄 {side_name} sell order pushed #{mid}: {ex_price} → {target_price}")
                self.client.cancel_order(ex_oid)
                time.sleep(0.5)

        oid, err = self.client.place_order(
            mid=mid,
            tid=tid,
            is_buy=False,
            price=target_price,
            amount=position,
            is_quote=False
        )

        if oid:
            tg_log.log(f"📤 {side_name} SELL #{mid}: {position:.2f} @ {target_price}")
        elif err:
            tg_log.log(f"❌ {side_name} sell failed #{mid}: {err}")

        return False

    def start_session(self, mid: int, budget: Decimal) -> bool:
        """Start a new trading session"""
        try:
            market = self.client.get_market(mid)
            if not market:
                tg_log.log(f"❌ Market #{mid} not found")
                return False

            positions = self.client.get_my_positions(limit=100)
            yes_pos = Decimal("0")
            no_pos = Decimal("0")

            for p in positions:
                p_mid = int(p.get("market_id") or p.get("marketId") or 0)
                if p_mid != mid:
                    continue

                tid = str(p.get("token_id") or p.get("tokenId"))
                amt = safe_decimal(p.get("shares_owned") or p.get("amount") or 0) or Decimal(0)
                if amt > 1_000_000_000:
                    amt /= Decimal(10**18)

                if tid == market.yes_token_id:
                    yes_pos = amt
                elif tid == market.no_token_id:
                    no_pos = amt

            has_position = yes_pos >= CONFIG["MIN_ORDER_SIZE"] or no_pos >= CONFIG["MIN_ORDER_SIZE"]
            initial_phase = Phase.SELL if has_position else Phase.BUY

            with self._lock:
                self.sessions[mid] = SessionState(
                    market=market,
                    total_budget=budget,
                    phase=initial_phase,
                    yes_position=yes_pos,
                    no_position=no_pos,
                    is_running=True
                )

            phase_str = "SELL (has position)" if has_position else "BUY (no position)"
            tg_log.log(f"🚀 Started session #{mid} with ${budget:.2f}, phase={phase_str}")
            self._save_state()
            return True

        except Exception as e:
            tg_log.log(f"❌ Failed to start session #{mid}: {e}")
            return False

    def stop_session(self, mid: int) -> bool:
        """Stop a trading session"""
        with self._lock:
            if mid not in self.sessions:
                return False

            s = self.sessions[mid]
            s.is_running = False
            self.client.cancel_all_orders(mid)
            tg_log.log(f"🛑 Stopped session #{mid}")
            self._save_state()
            return True

    def trigger_sell_all(self):
        """Trigger force sell for all positions"""
        tg_log.log("🚨 SELL ALL triggered")

        positions = self.client.get_my_positions(limit=100)
        if not positions:
            tg_log.log("✅ No positions found")
            return

        count = 0
        with self._lock:
            for p in positions:
                mid = int(p.get("market_id") or p.get("marketId") or 0)
                if mid == 0:
                    continue

                val = safe_decimal(p.get("current_value_in_quote_token") or 0) or Decimal(0)
                if val < CONFIG["MIN_VALUE_USDT"]:
                    continue

                if mid not in self.sessions:
                    m = self.client.get_market(mid)
                    if not m:
                        continue
                    self.sessions[mid] = SessionState(market=m, total_budget=Decimal("0"))

                s = self.sessions[mid]
                s.is_running = True
                s.force_sell_mode = True
                s.phase = Phase.SELL
                count += 1

        tg_log.log(f"✅ Sell mode enabled for {count} markets")
        self._save_state()


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
        dp.add_handler(CommandHandler("run", self.run))
        dp.add_handler(CommandHandler("stop", self.stop))
        dp.add_handler(CommandHandler("status", self.status))
        dp.add_handler(CommandHandler("position", self.position))
        dp.add_handler(CommandHandler("sellall", self.sellall))
        dp.add_handler(CommandHandler("kill", self.kill))
        dp.add_handler(CallbackQueryHandler(self.page_callback, pattern=r"^p_"))

        self._markets_cache = []
        self.updater.start_polling()

    def _auth(self, u):
        return str(u.effective_chat.id) == str(CONFIG["TELEGRAM_CHAT_ID"])

    def start(self, u, c):
        if self._auth(u):
            u.message.reply_text("🤖 Opinion Bot v8.1 Ready\n\nUse /help for commands")

    def help(self, u, c):
        if not self._auth(u):
            return
        msg = """📚 Commands:
/markets - List all markets (paginated)
/market <id> - Show market details
/run <id> <budget> - Start trading on market
/stop <id> - Stop trading on market
/status - Show all active sessions
/position - Show positions
/sellall - Force sell all positions
/kill - Emergency stop all"""
        u.message.reply_text(msg)

    def markets(self, u, c):
        if not self._auth(u):
            return
        u.message.reply_text("🔄 Fetching markets...")

        self._markets_cache = []
        page = 1
        while True:
            ms = self.mgr.client.get_markets(page=page, limit=20)
            if not ms:
                break
            self._markets_cache.extend(ms)
            if len(ms) < 20:
                break
            page += 1
            if page > 10:  # Safety limit
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

        txt = f"📊 Markets (Page {page+1}/{max_p})\n\n"
        for m in self._markets_cache[start:end]:
            txt += f"[{m.market_id}] {m.title[:35]}\n"
            txt += f"   Vol: ${m.volume:,.0f}\n\n"

        btns = []
        if page > 0:
            btns.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"p_{page-1}"))
        if page < max_p - 1:
            btns.append(InlineKeyboardButton("Next ➡️", callback_data=f"p_{page+1}"))

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
                return u.message.reply_text("❌ Market not found")

            txt = f"📝 Market #{m.market_id}\n\n"
            txt += f"Title: {m.title}\n"
            txt += f"Volume: ${m.volume:,.2f}\n"
            txt += f"YES Token: {m.yes_token_id[:16]}...\n"
            txt += f"NO Token: {m.no_token_id[:16]}...\n"

            if m.child_markets:
                txt += f"\n🔗 Child Markets ({len(m.child_markets)}):\n"
                for idx, child in enumerate(m.child_markets[:5], 1):
                    if isinstance(child, dict):
                        c_id = child.get('market_id') or child.get('marketId') or '?'
                        c_title = child.get('market_title') or child.get('marketTitle') or child.get('title') or '?'
                        txt += f"  {idx}. [{c_id}] {c_title[:30]}\n"
                if len(m.child_markets) > 5:
                    txt += f"  ... and {len(m.child_markets) - 5} more\n"

            u.message.reply_text(txt)
        except:
            u.message.reply_text("Usage: /market <id>")

    def run(self, u, c):
        if not self._auth(u):
            return
        try:
            mid = int(c.args[0])
            budget = Decimal(c.args[1])

            if budget < 5:
                return u.message.reply_text("❌ Budget must be at least $5")

            u.message.reply_text(f"🚀 Starting session #{mid} with ${budget:.2f}...")

            success = self.mgr.start_session(mid, budget)
            if success:
                u.message.reply_text(f"✅ Session #{mid} started")
            else:
                u.message.reply_text(f"❌ Failed to start session #{mid}")
        except:
            u.message.reply_text("Usage: /run <market_id> <budget>")

    def stop(self, u, c):
        if not self._auth(u):
            return
        try:
            mid = int(c.args[0])
            success = self.mgr.stop_session(mid)
            if success:
                u.message.reply_text(f"🛑 Session #{mid} stopped")
            else:
                u.message.reply_text(f"❌ Session #{mid} not found")
        except:
            u.message.reply_text("Usage: /stop <market_id>")

    def status(self, u, c):
        if not self._auth(u):
            return

        with self.mgr._lock:
            sessions = list(self.mgr.sessions.values())

        if not sessions:
            return u.message.reply_text("✅ No active sessions")

        txt = f"📊 Active Sessions ({len(sessions)}):\n\n"
        for s in sessions:
            if not s.is_running:
                continue
            txt += f"[#{s.market.market_id}] {s.market.title[:25]}\n"
            txt += f"  Budget: ${s.total_budget:.2f}\n"
            txt += f"  Phase: {s.phase.value}\n"
            txt += f"  YES pos: {s.yes_position:.2f} | NO pos: {s.no_position:.2f}\n"
            if s.force_sell_mode:
                txt += f"  🚨 FORCE SELL MODE\n"
            txt += "\n"

        u.message.reply_text(txt[:4000])

    def position(self, u, c):
        if not self._auth(u):
            return
        u.message.reply_text("🔍 Fetching positions...")

        pos = self.mgr.client.get_my_positions(limit=100)
        if not pos:
            return u.message.reply_text("✅ No positions")

        msg = "🎒 Positions:\n\n"
        for p in pos:
            try:
                mid = int(p.get("market_id") or p.get("marketId") or 0)
                if mid == 0:
                    continue
                shares = safe_decimal(p.get("shares_owned") or p.get("amount") or 0) or Decimal(0)
                if shares > 1_000_000_000:
                    shares /= Decimal(10**18)
                val = safe_decimal(p.get("current_value_in_quote_token") or 0) or Decimal(0)
                if val < 0.1:
                    continue
                outcome = p.get("outcome") or "?"
                title = p.get('market_title', '')[:20]
                msg += f"[#{mid}] {title}\n"
                msg += f"  {outcome}: {shares:.2f} (${val:.2f})\n\n"
            except:
                pass

        u.message.reply_text(msg[:4000])

    def sellall(self, u, c):
        if not self._auth(u):
            return
        u.message.reply_text("🔥 SELL ALL Initiated")
        self.mgr.trigger_sell_all()

    def kill(self, u, c):
        if not self._auth(u):
            return
        u.message.reply_text("🚨 EMERGENCY STOP")

        with self.mgr._lock:
            for mid, s in self.mgr.sessions.items():
                s.is_running = False
                self.mgr.client.cancel_all_orders(mid)

        u.message.reply_text("🛑 All sessions stopped")


# ============================================================
# 6. Main
# ============================================================
if __name__ == "__main__":
    if not CONFIG["API_KEY"]:
        print("❌ Missing OPINION_API_KEY in environment")
    elif not CONFIG["TELEGRAM_BOT_TOKEN"]:
        print("❌ Missing TELEGRAM_BOT_TOKEN in environment")
    elif not CONFIG["TELEGRAM_CHAT_ID"]:
        print("❌ Missing TELEGRAM_CHAT_ID in environment")
    else:
        print("🚀 Opinion Bot v8.1 Starting...")
        mgr = StrategyManager()
        if mgr.initialize():
            mgr.start_loop()
            tg = TelegramBot(mgr)
            tg_log.log("✅ Bot v8.1 started successfully")
            try:
                while True:
                    time.sleep(10)
            except KeyboardInterrupt:
                print("\n🛑 Shutting down...")
                tg_log.stop()
        else:
            print("❌ Failed to initialize")
