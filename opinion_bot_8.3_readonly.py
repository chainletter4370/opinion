"""
Opinion Volume Bot v8.3 - SDK-Free Read-Only Version
====================================================
APPROACH:
- Skip SDK entirely for now (urllib3 conflict)
- Direct REST API for all read operations
- Trading temporarily disabled until SDK issue resolved
- Focus on markets, orderbook, and monitoring

TODO: Enable trading once SDK/urllib3 issue is resolved
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
    "MULTI_SIG_ADDR": os.getenv("OPINION_MULTI_SIG_ADDR"),
    "TELEGRAM_BOT_TOKEN": os.getenv("TELEGRAM_BOT_TOKEN"),
    "TELEGRAM_CHAT_ID": os.getenv("TELEGRAM_CHAT_ID"),

    "CHECK_INTERVAL": 5.0,
    "PRICE_QUANT": Decimal("0.001"),
}

logging.basicConfig(level=logging.ERROR)

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
@dataclass
class MarketInfo:
    market_id: int
    title: str
    yes_token_id: str
    no_token_id: str
    child_markets: List[Dict[str, Any]] = field(default_factory=list)
    volume: Decimal = Decimal("0")
    yes_buy_price: Decimal = Decimal("0")
    no_buy_price: Decimal = Decimal("0")


# ============================================================
# 3. Direct API Client
# ============================================================
class OpinionClient:
    def __init__(self):
        """Direct REST API client"""
        self.api_key = CONFIG["API_KEY"]
        self.host = CONFIG["HOST"]
        self.chain_id = CONFIG["CHAIN_ID"]
        self.wallet = CONFIG["MULTI_SIG_ADDR"]

        self.session = requests.Session()
        self.session.headers.update({
            "accept": "application/json",
            "apikey": self.api_key
        })
        tg_log.log("✅ Direct API client initialized")

    def _api_call(self, method: str, endpoint: str, params: Dict = None) -> Dict:
        """Make REST API call"""
        url = f"{self.host}{endpoint}"
        try:
            if method == "GET":
                resp = self.session.get(url, params=params, timeout=30)
            else:
                raise ValueError(f"Unsupported method: {method}")

            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            tg_log.log(f"❌ API error: {endpoint} - {e}")
            return {"errno": 999, "errmsg": str(e)}

    def get_markets(self, page=1, limit=20):
        """Get market list"""
        tg_log.log(f"📡 API: get_markets(page={page}, limit={limit})")

        result = self._api_call("GET", "/api/bsc/api/v2/topic", params={
            "page": page,
            "limit": limit,
            "chainId": self.chain_id,
            "status": "2"
        })

        if result.get("errno") != 0:
            return []

        raw_list = result.get("result", {}).get("list", [])
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
                    volume=safe_decimal(r.get("volume", 0)) or Decimal("0"),
                    yes_buy_price=safe_decimal(r.get("yesBuyPrice", 0)) or Decimal("0"),
                    no_buy_price=safe_decimal(r.get("noBuyPrice", 0)) or Decimal("0")
                ))
            except Exception as e:
                tg_log.log(f"⚠️ Parse error: {e}")
                continue

        tg_log.log(f"✅ Got {len(markets)} markets")
        return markets

    def get_market(self, mid: int):
        """Get single market"""
        tg_log.log(f"📡 API: get_market({mid})")

        result = self._api_call("GET", f"/api/bsc/api/v2/topic/{mid}", params={
            "chainId": self.chain_id
        })

        if result.get("errno") != 0:
            return None

        r = result.get("result", {})
        try:
            return MarketInfo(
                market_id=int(r.get("topicId", mid)),
                title=str(r.get("title", "Unknown")),
                yes_token_id=str(r.get("yesPos", "")),
                no_token_id=str(r.get("noPos", "")),
                child_markets=r.get("childList", []),
                volume=safe_decimal(r.get("volume", 0)) or Decimal("0"),
                yes_buy_price=safe_decimal(r.get("yesBuyPrice", 0)) or Decimal("0"),
                no_buy_price=safe_decimal(r.get("noBuyPrice", 0)) or Decimal("0")
            )
        except Exception as e:
            tg_log.log(f"❌ Parse error: {e}")
            return None

    def get_orderbook(self, tid):
        """Get orderbook"""
        result = self._api_call("GET", "/api/bsc/api/v2/orderbook", params={
            "tokenId": tid,
            "chainId": self.chain_id
        })

        if result.get("errno") != 0:
            return None

        data = result.get("result", {})
        return data


# ============================================================
# 4. Telegram Bot (Read-Only)
# ============================================================
class TelegramBot:
    def __init__(self, client: OpinionClient):
        self.client = client
        tg_log.setup(CONFIG["TELEGRAM_BOT_TOKEN"], CONFIG["TELEGRAM_CHAT_ID"])
        self.updater = Updater(token=CONFIG["TELEGRAM_BOT_TOKEN"], use_context=True)
        dp = self.updater.dispatcher

        dp.add_handler(CommandHandler("start", self.start))
        dp.add_handler(CommandHandler("help", self.help))
        dp.add_handler(CommandHandler("markets", self.markets))
        dp.add_handler(CommandHandler("market", self.market))
        dp.add_handler(CommandHandler("orderbook", self.orderbook))
        dp.add_handler(CallbackQueryHandler(self.page_callback, pattern=r"^p_"))

        self._markets_cache = []
        self.updater.start_polling()

    def _auth(self, u):
        return str(u.effective_chat.id) == str(CONFIG["TELEGRAM_CHAT_ID"])

    def start(self, u, c):
        if self._auth(u):
            u.message.reply_text("🤖 Opinion Bot v8.3 (Read-Only)\n\n⚠️ Trading temporarily disabled due to SDK issue")

    def help(self, u, c):
        if not self._auth(u):
            return
        msg = """📚 Available Commands:

/markets - List all markets (paginated)
/market <id> - Show market details
/orderbook <token_id> - Show orderbook

⚠️ Disabled (SDK issue):
- /position
- /run
- /sellall
- Trading operations

Need to resolve urllib3 conflict with SDK"""
        u.message.reply_text(msg)

    def markets(self, u, c):
        if not self._auth(u):
            return
        u.message.reply_text("🔄 Fetching markets...")

        self._markets_cache = []
        page = 1
        while True:
            ms = self.client.get_markets(page=page, limit=20)
            if not ms:
                break
            self._markets_cache.extend(ms)
            if len(ms) < 20:
                break
            page += 1
            if page > 10:
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
            txt += f"   Vol: ${m.volume:,.0f}\n"
            txt += f"   YES: {m.yes_buy_price:.3f} | NO: {m.no_buy_price:.3f}\n\n"

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
            m = self.client.get_market(mid)
            if not m:
                return u.message.reply_text("❌ Market not found")

            txt = f"📝 Market #{m.market_id}\n\n"
            txt += f"Title: {m.title}\n"
            txt += f"Volume: ${m.volume:,.2f}\n"
            txt += f"YES Buy: {m.yes_buy_price:.3f}\n"
            txt += f"NO Buy: {m.no_buy_price:.3f}\n"
            txt += f"\nYES Token: {m.yes_token_id[:16]}...\n"
            txt += f"NO Token: {m.no_token_id[:16]}...\n"

            if m.child_markets:
                txt += f"\n🔗 Child Markets ({len(m.child_markets)}):\n"
                for idx, child in enumerate(m.child_markets[:5], 1):
                    if isinstance(child, dict):
                        c_id = child.get('topicId') or '?'
                        c_title = child.get('title') or '?'
                        txt += f"  {idx}. [{c_id}] {c_title[:30]}\n"
                if len(m.child_markets) > 5:
                    txt += f"  ... and {len(m.child_markets) - 5} more\n"

            u.message.reply_text(txt)
        except:
            u.message.reply_text("Usage: /market <id>")

    def orderbook(self, u, c):
        if not self._auth(u):
            return
        try:
            tid = c.args[0]
            book = self.client.get_orderbook(tid)

            if not book:
                return u.message.reply_text("❌ Orderbook not found")

            bids = book.get("bids", [])[:5]
            asks = book.get("asks", [])[:5]

            txt = f"📚 Orderbook for {tid[:16]}...\n\n"

            txt += "📈 Asks (Sell):\n"
            for ask in reversed(asks):
                txt += f"  {ask['price']:.3f} × {float(ask['amount']):.2f}\n"

            txt += "\n"

            txt += "📉 Bids (Buy):\n"
            for bid in bids:
                txt += f"  {bid['price']:.3f} × {float(bid['amount']):.2f}\n"

            u.message.reply_text(txt)
        except Exception as e:
            u.message.reply_text(f"Usage: /orderbook <token_id>\n\nError: {e}")


# ============================================================
# 5. Main
# ============================================================
if __name__ == "__main__":
    if not CONFIG["API_KEY"]:
        print("❌ Missing OPINION_API_KEY")
    elif not CONFIG["TELEGRAM_BOT_TOKEN"]:
        print("❌ Missing TELEGRAM_BOT_TOKEN")
    elif not CONFIG["TELEGRAM_CHAT_ID"]:
        print("❌ Missing TELEGRAM_CHAT_ID")
    else:
        print("🚀 Opinion Bot v8.3 Starting (Read-Only Mode)...")
        print("⚠️ Trading disabled due to SDK urllib3 conflict")
        print("   Markets and orderbook queries available")

        client = OpinionClient()
        tg = TelegramBot(client)
        tg_log.log("✅ Bot v8.3 started (Read-Only)")
        tg_log.log("⚠️ Trading requires SDK fix")

        try:
            while True:
                time.sleep(10)
        except KeyboardInterrupt:
            print("\n🛑 Shutting down...")
            tg_log.stop()
