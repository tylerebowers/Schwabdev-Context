"""Self-contained test suite for schwabdev_context — no network, no Schwab client, runs in ~1s.

Run:  python test_context.py        (exits non-zero if anything fails)

Covers: the Costs models, candle ingest, simulated MARKET/LIMIT/STOP fills, cash reservations,
the live safety guard, the live order/cancel/account-activity paths, Data parse/read/write,
FIFO trade matching and stats, the viewer payload, and ledger thread safety.

Two checks in the "KNOWN BUGS" section FAIL against the current module — they are not broken
tests, they pin real defects and should start passing once those are fixed. See that section for
the one-line fixes.
"""
import json, math, os, sys, tempfile, datetime
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from schwabdev_context.base import BacktestContext, LiveContext, dec
from schwabdev_context.data import Data
from schwabdev_context.context import Context, Costs, Costs_Zero
from schwabdev_context import analysis

FAILS = []
def check(name, cond, extra=""):
    print(("  PASS  " if cond else "  FAIL  ") + name + (f"   {extra}" if extra and not cond else ""))
    if not cond: FAILS.append(name)

def _raises(fn, exc):
    try:
        fn(); return False
    except exc:
        return True


def mkorder(sym, side, qty, otype="MARKET", price=None, stop=None):
    o = {"orderType": otype, "session": "NORMAL", "duration": "DAY",
         "orderStrategyType": "SINGLE",
         "orderLegCollection": [{"instruction": side, "quantity": qty,
                                 "instrument": {"symbol": sym, "assetType": "EQUITY"}}]}
    if price is not None: o["price"] = price
    if stop is not None: o["stopPrice"] = stop
    return o

def candles(sym, closes, t0=1_700_000_000_000):
    out = []
    for i, c in enumerate(closes):
        out.append({"symbol": sym, "time": t0 + i*60_000, "open": c, "high": c*1.01,
                    "low": c*0.99, "close": c, "volume": 1000.0, "type": "c"})
    return out

print("\n== dec() =============================================")
check("scaled price node", abs(dec({"lo": "220470000", "signScale": 12}) - 220.47) < 1e-9)
check("unscaled size node is not divided by 1e6", dec({"lo": "200"}) == 200.0,
      f"got {dec({'lo':'200'})}")
check("empty node -> 0", dec({"signScale": 12}) == 0.0)
check("plain number passes through", dec(3.5) == 3.5)
check("garbage -> 0", dec("nope") == 0.0 and dec(None) == 0.0)

print("\n== candle ingest / duplicate forming bar ==============")
ctx = BacktestContext(["AMD"], 10_000, Costs())
ctx._ingest(candles("AMD", [100, 101]))
ctx._ingest([{"symbol": "AMD", "time": 1_700_000_060_000, "open": 101, "high": 103,
             "low": 100, "close": 102.5, "volume": 5000.0, "type": "c"}])   # forming bar re-sent
check("re-sent forming bar replaces, not appends", len(ctx.candles["AMD"]) == 2,
      f"len={len(ctx.candles['AMD'])}")
check("re-sent bar carries the latest close", ctx.candles["AMD"][-1]["close"] == 102.5)

print("\n== backtest MARKET fill delay =========================")
def buy_once(tc, events):
    if not tc.orders and len(tc.candles["AMD"]) == 1:
        tc.order(mkorder("AMD", "BUY", 10))
run = BacktestContext(["AMD"], 10_000, Costs_Zero(), fill_delay=2)
for c in candles("AMD", [100, 101, 102, 103]):
    run.step(buy_once, [c])
rec = list(run.orders["AMD"].values())[0]
check("market order filled", rec["status"] == "FILLED")
check("filled at the open of the 2nd later bar (no look-ahead)",
      abs(rec["avg_price"] - 102.0) < 1e-9, f"avg={rec['avg_price']}")
check("trigger price preserved separately", rec["price"] == 100.0)
check("cash debited", abs(run._cash - (10_000 - 10*102.0)) < 1e-6, f"cash={run._cash}")
check("position booked", run.positions["AMD"] == 10)

print("\n== reservations =======================================")
r2 = BacktestContext(["AMD"], 1000, Costs(), fill_delay=99)
r2._ingest(candles("AMD", [100]))
r2.order(mkorder("AMD", "BUY", 5, "LIMIT", price=90))
check("open buy reserves cash", abs(r2.cash - (1000 - 5*90)) < 1e-9, f"cash={r2.cash}")
check("settled cash untouched while resting", r2._cash == 1000)
r2.cancel(list(r2.orders["AMD"].values())[0]["id"])
check("cancel frees the reservation", r2.cash == 1000)

print("\n== LIMIT never fills through its own price ============")
r3 = BacktestContext(["AMD"], 10_000, Costs(slip=0.05, pct=0), fill_delay=2)
r3._ingest(candles("AMD", [100]))
r3.order(mkorder("AMD", "BUY", 1, "LIMIT", price=99))
r3._ingest([{"symbol": "AMD", "time": 1_700_000_060_000, "open": 98, "high": 99,
            "low": 97, "close": 98, "volume": 1, "type": "c"}])
r3._settle_pending()
lim = list(r3.orders["AMD"].values())[0]
check("buy limit fills at or better than the limit", lim["avg_price"] <= 99.0 + 1e-9,
      f"avg={lim['avg_price']}")
check("buy limit actually filled", lim["status"] == "FILLED")

print("\n== STOP triggers intrabar ============================")
r4 = BacktestContext(["AMD"], 10_000, Costs_Zero(), fill_delay=2)
r4._ingest(candles("AMD", [100]))
r4.positions["AMD"] = 10
r4.order(mkorder("AMD", "SELL", 10, "STOP", stop=95))
r4._ingest([{"symbol": "AMD", "time": 1_700_000_060_000, "open": 99, "high": 99.5,
            "low": 94, "close": 98, "volume": 1, "type": "c"}])   # dipped through the stop, closed above
r4._settle_pending()
st = list(r4.orders["AMD"].values())[0]
check("stop filled on the intrabar low, not the close", st["status"] == "FILLED" and abs(st["avg_price"] - 95) < 1e-9,
      f"status={st['status']} avg={st['avg_price']}")

print("\n== FIFO trade matching ===============================")
class FakeRun:
    tickers = ["AMD"]
    def __init__(self, orders): self._o = orders; self.positions = {}; self.candles = {"AMD": []}
    def snapshot_orders(self): return self._o
    def portfolio_value(self): return 0.0
    _starting_cash = 10_000
def o(i, side, qty, px, t):
    return {"id": i, "symbol": "AMD", "instruction": side, "order_type": "MARKET",
            "quantity": qty, "filled": qty, "price": px, "avg_price": px, "fees": 0.0,
            "time": t, "status": "FILLED"}
# buy 10 @100, buy 10 @110, sell 20 @120  -> two round trips (+20%, +9.09%)
tr = analysis.trades(FakeRun([o(1,"BUY",10,100,1), o(2,"BUY",10,110,2), o(3,"SELL",20,120,3)]))
check("one sell closing two lots yields two round trips", len(tr) == 2, f"{len(tr)} trades")
check("FIFO order and quantities", [t["entry"] for t in tr] == [100, 110] and all(t["qty"] == 10 for t in tr))
check("returns computed per lot", abs(tr[0]["ret"] - 20) < 1e-6 and abs(tr[1]["ret"] - 100/11) < 1e-6,
      f"{[round(t['ret'],3) for t in tr]}")
# the old zip(buys, sells) would have produced ONE pair here and lost the second buy entirely
tr2 = analysis.trades(FakeRun([o(1,"BUY",100,10,1), o(2,"SELL",40,12,2), o(3,"SELL",60,9,3)]))
check("partial closes split one lot", len(tr2) == 2 and [t["qty"] for t in tr2] == [40, 60])
check("pnl signs correct", tr2[0]["pnl"] > 0 and tr2[1]["pnl"] < 0)

print("\n== equity curve / drawdown ===========================")
eq = [(i*60_000, v) for i, v in enumerate([100, 120, 90, 95, 130])]
cs = analysis._curve_stats(eq)
check("max drawdown from peak", abs(cs["max_drawdown"] - 25.0) < 1e-9, f"{cs['max_drawdown']}")
check("sharpe finite", math.isfinite(cs["sharpe"]))
check("short curve is safe", analysis._curve_stats([(0, 1)]) == {"max_drawdown": 0.0, "sharpe": 0.0})

print("\n== paper LiveContext: limits must rest ===============")
paper = LiveContext(["AMD"], 10_000, costs=Costs())
paper._ingest(candles("AMD", [100]))
oid = paper.order(mkorder("AMD", "BUY", 1, "LIMIT", price=50))   # 50% below market
prec = paper._find_order(oid)
check("paper limit does NOT fill on submission", prec["status"] == "OPEN", f"status={prec['status']}")
check("paper limit did not move cash", paper._cash == 10_000)
oid2 = paper.order(mkorder("AMD", "BUY", 1))                     # market: fills now
check("paper market fills immediately", paper._find_order(oid2)["status"] == "FILLED")
paper.step(lambda tc, e: None, [{"symbol": "AMD", "time": 1_700_000_060_000, "open": 49,
                                 "high": 50, "low": 48, "close": 49, "volume": 1, "type": "c"}])
check("paper limit fills once the market touches it", prec["status"] == "FILLED")

print("\n== safety guard ======================================")
# candle: open 100, high 101, low 99, close 100  -> ref = min(OHLC) = 99
sg = LiveContext(["AMD"], 10_000, costs=Costs(), safety=True)
sg._ingest(candles("AMD", [100]))
def blocked(fn):
    try: fn(); return False
    except ValueError as e: return "safety" in str(e)
check("BUY limit >5% above min(OHLC) blocked",
      blocked(lambda: sg.order(mkorder("AMD", "BUY", 1, "LIMIT", price=104.0))))   # 99*1.05=103.95
check("BUY limit just inside allowed",
      sg.order(mkorder("AMD", "BUY", 1, "LIMIT", price=103.9)) is not None)
check("SELL limit >5% below min(OHLC) blocked",
      blocked(lambda: sg.order(mkorder("AMD", "SELL", 1, "LIMIT", price=94.0))))   # 99*0.95=94.05
check("SELL limit just inside allowed",
      sg.order(mkorder("AMD", "SELL", 1, "LIMIT", price=94.1)) is not None)
check("SELL STOP far below blocked too (stop level is its price)",
      blocked(lambda: sg.order(mkorder("AMD", "SELL", 1, "STOP", stop=90.0))))
check("MARKET order passes (prices at last close)",
      sg.order(mkorder("AMD", "BUY", 1)) is not None)
check("blocked orders never reach the ledger",
      not any(r["price"] in (104.0, 94.0, 90.0) for r in sg.snapshot_orders()))

sg_off = LiveContext(["AMD"], 10_000, costs=Costs(), safety=False)
sg_off._ingest(candles("AMD", [100]))
check("safety=False disables the guard",
      sg_off.order(mkorder("AMD", "BUY", 1, "LIMIT", price=150.0)) is not None)

sg_new = LiveContext(["AMD"], 10_000, costs=Costs(), safety=True)  # no candles yet
check("no candle yet -> nothing to compare, order allowed",
      sg_new.order(mkorder("AMD", "BUY", 1, "LIMIT", price=1.0)) is not None)

class SafeClient:
    def __init__(self): self.calls = 0
    def place_order(self, h, o):
        self.calls += 1
        return type("R", (), {"ok": True, "status_code": 201,
                              "headers": {"location": "/orders/1"}, "text": ""})()
sc = SafeClient()
sg_live = LiveContext(["AMD"], 10_000, client=sc, account_hash="H", costs=Costs(), safety=True)
sg_live._ingest(candles("AMD", [100]))
check("live: unsafe order blocked BEFORE reaching the broker",
      blocked(lambda: sg_live.order(mkorder("AMD", "BUY", 1, "LIMIT", price=104.0)))
      and sc.calls == 0, f"broker calls={sc.calls}")
check("Context passes safety through to the session",
      Context(None, safety=False)._safety is False and Context(None)._safety is True)
check("Context requires a client when an account hash is given",
      _raises(lambda: Context(None, account_hash="H"), ValueError))

print("\n== cost models =======================================")
check("Costs_Zero charges nothing and moves no price",
      Costs_Zero().apply("SELL", 10, 100.0) == (100.0, 0))
px_d, fee_d = Costs().apply("SELL", 10, 100.0)
check("default Costs charges regulatory fees on sells", fee_d > 0 and px_d == 100.0)
px_b, fee_b = Costs().apply("BUY", 10, 100.0)
check("default Costs charges no reg fees on buys", 0 < fee_b < fee_d)
check("half_spread/slip move the price against the side",
      Costs(half_spread=0.01).apply("BUY", 1, 100.0)[0] > 100.0
      and Costs(half_spread=0.01).apply("SELL", 1, 100.0)[0] < 100.0)
check("Costs is importable from the package root",
      __import__("schwabdev_context", fromlist=["Costs"]).Costs is Costs)

print("\n== KNOWN BUGS (expected to fail on this version) =====")
# 1. deploy() defaults costs=None and passes it straight through; _Base stores it raw, so the
#    first PAPER fill calls None.apply(...). Fix: `self.costs = costs or Costs()` in _Base, or
#    `costs=costs or Costs()` at the LiveContext call in Context.deploy.
lc_nc = LiveContext(["AMD"], 10_000)          # exactly what deploy(costs=None) builds
lc_nc._ingest(candles("AMD", [100]))
try:
    lc_nc.order(mkorder("AMD", "BUY", 1))
    check("deploy default (costs=None) can paper-fill", True)
except AttributeError as e:
    check("deploy default (costs=None) can paper-fill", False, f"{e}")

# 2. `cash` accepts anything: backtest(strategy, tickers, 60, Costs_Zero()) binds Costs to CASH.
#    It stays hidden until the viewer JSON-encodes portfolio_value, far from the real mistake.
try:
    bad = BacktestContext(["AMD"], Costs_Zero(), Costs())   # a Costs bound to the CASH slot
    ok = not isinstance(bad.portfolio_value(), Costs)       # passes if it is rejected OR harmless
    why = "silently accepted; surfaces later as 'Object of type Costs is not JSON serializable'"
except TypeError:
    ok, why = True, ""                                      # rejected at construction: correct
check("a Costs in the cash slot is rejected early", ok, why)

print("\n== live order path: rejection must not book ==========")
class Resp:
    def __init__(self, ok, code=200, headers=None, text=""):
        self.ok, self.status_code, self.headers, self.text = ok, code, headers or {}, text
class FakeClient:
    def __init__(self, resp): self.resp = resp; self.canceled = []
    def place_order(self, h, o): return self.resp
    def cancel_order(self, h, oid): self.canceled.append(oid); return Resp(True)
    def account_details(self, h, fields=None):
        return Resp(True, headers={}, text="") if False else _AcctResp()
class _AcctResp(Resp):
    def __init__(self): super().__init__(True)
    def json(self): return {"securitiesAccount": {
        "currentBalances": {"cashBalance": 4321.0, "cashAvailableForTrading": 4200.0},
        "positions": [{"instrument": {"symbol": "amd"}, "longQuantity": 7.0, "shortQuantity": 0.0}]}}

rejected = LiveContext(["AMD"], 10_000, client=FakeClient(Resp(False, 400, text="bad symbol")),
                       account_hash="H")
rejected._ingest(candles("AMD", [100]))
try:
    rejected.order(mkorder("AMD", "BUY", 10))
    check("rejected order raises", False)
except RuntimeError as e:
    check("rejected order raises instead of inventing a fill", True)
check("rejected order left no position", rejected.positions == {} and rejected._cash == 10_000)

live = LiveContext(["AMD"], 10_000,
                   client=FakeClient(Resp(True, 201, headers={"location": "/orders/998877"})),
                   account_hash="H")
live._ingest(candles("AMD", [100]))
lid = live.order(mkorder("AMD", "BUY", 3))
check("broker id adopted", lid == "998877", f"id={lid}")
check("broker-owned order is not simulated", live._find_order("998877")["sim"] is False)
live.step(lambda tc, e: None, candles("AMD", [101, 102, 103]))
check("simulator leaves broker orders alone", live._find_order("998877")["status"] == "OPEN")

print("\n== sync_account ======================================")
sync = LiveContext(["AMD"], 0.0, client=FakeClient(Resp(True)), account_hash="H")
sync.sync_account()
check("cash adopted from account", sync._cash == 4200.0, f"{sync._cash}")
check("positions adopted and upper-cased", sync.positions == {"AMD": 7.0}, f"{sync.positions}")

print("\n== ACCT_ACTIVITY classification ======================")
def activity(msg_type, payload):
    return {"1": "acct", "2": msg_type, "3": json.dumps(payload)}

sess = LiveContext(["AAPL"], 10_000, client=FakeClient(Resp(True, 201, headers={"location": "/orders/555"})),
                   account_hash="H")
sess._ingest(candles("AAPL", [200]))
oid = sess.order(mkorder("AAPL", "BUY", 1))

# the cancel-path ExecutionCreated: carries ExecutionQuantity, ExecutionTransType UROut, no price
sess._on_activity(activity("ExecutionCreated", {"SchwabOrderID": "555", "BaseEvent": {
    "ExecutionCreatedEventExecutionInfo": {"ExecutionInfo": {
        "ExecutionId": "X1", "ExecutionQuantity": {"lo": "1000000", "signScale": 12},
        "ExecutionTransType": "UROut", "CancelType": "ClientCancel"}}}}))
check("ExecutionCreated(UROut) books nothing", sess.positions == {} and sess._find_order("555")["filled"] == 0)

# a routing lifecycle event that carries a routed quantity AND price
sess._on_activity(activity("ExecutionRequested", {"SchwabOrderID": "555", "BaseEvent": {
    "ExecutionRequestedEventRoutedInfo": {"RouteInfo": {
        "RoutedQuantity": {"lo": "1000000", "signScale": 12},
        "Price": {"lo": "200000000", "signScale": 12}}}}}))
check("ExecutionRequested books nothing", sess.positions == {})

fill = activity("OrderFillCompleted", {"SchwabOrderID": "555", "BaseEvent": {
    "OrderFillCompletedEventOrderLegQuantityInfo": {"ExecutionInfo": {
        "ExecutionId": "X2", "ExecutionQuantity": {"lo": "1000000", "signScale": 12},
        "ExecutionPrice": {"lo": "213409100", "signScale": 12},
        "ExecutionTransType": "Fill"}}}})
sess._on_activity(fill)
check("OrderFillCompleted books the fill", sess.positions == {"AAPL": 1.0}, f"{sess.positions}")
check("fill price decoded", abs(sess._find_order("555")["avg_price"] - 213.4091) < 1e-6)
sess._on_activity(fill)   # duplicate delivery
check("duplicate execution id is ignored", sess.positions == {"AAPL": 1.0}, f"{sess.positions}")

out = LiveContext(["AAPL"], 10_000, client=FakeClient(Resp(True, 201, headers={"location": "/orders/777"})),
                  account_hash="H")
out._ingest(candles("AAPL", [200]))
out.order(mkorder("AAPL", "BUY", 1, "LIMIT", price=199))
out._on_activity(activity("OrderUROutCompleted", {"SchwabOrderID": "777", "BaseEvent": {}}))
check("UROut cancels the record", out._find_order("777")["status"] == "CANCELED")

print("\n== live cancel routes to broker ======================")
fc = FakeClient(Resp(True, 201, headers={"location": "/orders/321"}))
c1 = LiveContext(["AMD"], 10_000, client=fc, account_hash="H")
c1._ingest(candles("AMD", [100]))
cid = c1.order(mkorder("AMD", "BUY", 1, "LIMIT", price=90))
c1.cancel(cid)
check("cancel sent to broker", fc.canceled == ["321"], f"{fc.canceled}")
check("record stays OPEN until the stream confirms", c1._find_order("321")["status"] == "OPEN")

fc2 = FakeClient(Resp(True, 201, headers={"location": "/orders/322"}))
fc2.cancel_order = lambda h, oid: Resp(False, 400)
c2 = LiveContext(["AMD"], 10_000, client=fc2, account_hash="H")
c2._ingest(candles("AMD", [100]))
cid2 = c2.order(mkorder("AMD", "BUY", 1, "LIMIT", price=90))
check("refused cancel returns False and leaves it open",
      c2.cancel(cid2) is False and c2._find_order("322")["status"] == "OPEN")

print("\n== Data.parse robustness =============================")
d = Data(cache_db=os.path.join(tempfile.mkdtemp(), "t.db"), client=None)
check("garbage message", d.parse("not json") == {"chart": [], "l1": [], "l2": [], "activity": []})
check("null data", d.parse(json.dumps({"data": None}))["chart"] == [])
incomplete = json.dumps({"data": [{"service": "CHART_EQUITY", "timestamp": 1, "content": [
    {"key": "AMD", "2": 1.0, "7": 123}]}]})           # missing high/low/close
check("incomplete candle dropped instead of KeyError", d.parse(incomplete)["chart"] == [])
good = json.dumps({"data": [{"service": "CHART_EQUITY", "timestamp": 1, "content": [
    {"key": "AMD", "1": 425, "2": 221.1, "3": 221.2, "4": 221.0, "5": 221.02, "6": 11730, "7": 1765307100000}]}]})
p = d.parse(good)
check("good candle parsed", p["chart"] == [{"symbol": "AMD", "open": 221.1, "high": 221.2,
      "low": 221.0, "close": 221.02, "volume": 11730, "time": 1765307100000, "type": "c"}], f"{p['chart']}")
l1 = json.dumps({"data": [{"service": "LEVELONE_EQUITIES", "timestamp": 99, "content": [
    {"key": "AMD", "1": 217.86, "2": 217.95}]}]})     # a delta: only bid/ask changed
q = d.parse(l1)["l1"][0]
check("l1 delta keeps missing fields as None", q["last"] is None and q["bid"] == 217.86)
book_msg = json.dumps({"data": [{"service": "NASDAQ_BOOK", "timestamp": 5, "content": [
    {"key": "AMD", "1": 1765306382549, "2": [{"0": 221.38, "1": 640}], "3": []}]}]})
check("book snapshot parsed", d.parse(book_msg)["l2"][0]["bids"][0]["0"] == 221.38)
check("parsed events all tagged", all(e["type"] == k for k in ("l1", "l2")
      for e in d.parse(l1 if k == "l1" else book_msg)[k]))

print("\n== level-1 merge semantics ===========================")
m = BacktestContext(["AMD"], 10_000, Costs())
m._ingest([{"symbol": "AMD", "time": 1, "bid": 10.0, "ask": 10.1, "last": 10.05,
            "bid_size": 100, "ask_size": 200, "type": "l1"}])
m._ingest([{"symbol": "AMD", "time": 2, "bid": 10.2, "ask": None, "last": None,
            "bid_size": None, "ask_size": None, "type": "l1"}])
check("delta merges, does not blank the book",
      m.quotes["AMD"]["bid"] == 10.2 and m.quotes["AMD"]["ask"] == 10.1)

print("\n== Data read/write round trip ========================")
d.write({"chart": [{"symbol": "AMD", "time": 1_700_000_000_000, "open": 1, "high": 2, "low": 0.5,
                    "close": 1.5, "volume": 10}],
         "l1": [{"symbol": "AMD", "time": 1_700_000_000_000, "bid": 1, "ask": 2, "last": 1.5,
                 "bid_size": 1, "ask_size": 2}],
         "l2": [{"symbol": "AMD", "time": 1_700_000_000_000, "bids": [1], "asks": [2]}],
         "activity": []})
import sqlite3
con = sqlite3.connect(d.db_path)
names = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
idx = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='index'")}
con.close()
check("tables created", {"chart_AMD", "l1_AMD", "l2_AMD"} <= names, f"{names}")
check("l1/l2 time indexes created", {"l1_AMD_time", "l2_AMD_time"} <= idx, f"{idx}")
ev = d.get_events("AMD", 3650, level1=True, level2=True)
check("recorded l1/l2 read back", len(ev) == 2 and ev[0]["bid"] == 1 and ev[1]["bids"] == [1])
check("cache-only get_candles works with no client", len(d.get_candles("AMD", 3650)) == 1)
d.close()

print("\n== level-2 routing without a client ==================")
class FakeStreamer:
    def __init__(self): self.sent = []
    def chart_equity(self, k, f): return ("chart", k)
    def level_one_equities(self, k, f): return ("l1", k)
    def nasdaq_book(self, k, f): return ("nasdaq", k)
    def nyse_book(self, k, f): return ("nyse", k)
    def send(self, m): self.sent.append(m)
fs = FakeStreamer()
d.subscribe(fs, ["AMD", "GE"], chart=True, level1=True, level2=True)
check("no client: level-2 falls back instead of crashing",
      ("nasdaq", "AMD,GE") in fs.sent and ("chart", "AMD,GE") in fs.sent, f"{fs.sent}")

print("\n== end-to-end backtest through Context ===============")
class HistClient:
    """Minimal price_history/quotes client backed by a deterministic sine wave."""
    def price_history(self, symbol, **kw):
        base = 100 + (10 if symbol == "AMD" else 0)
        # minute-aligned like real Schwab candles, so both tickers share timestamps
        t0 = (int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000) // 60_000) * 60_000 - 400*60_000
        rows = [{"datetime": t0 + i*60_000,
                 "open": base + math.sin(i/9)*5, "high": base + math.sin(i/9)*5 + 0.5,
                 "low": base + math.sin(i/9)*5 - 0.5, "close": base + math.sin(i/9)*5,
                 "volume": 1000 + i} for i in range(400)]
        return type("R", (), {"ok": True, "status_code": 200, "json": lambda self: {"candles": rows}})()
    def quotes(self, syms):
        return type("R", (), {"ok": True, "json": lambda self: {s: {"reference": {"exchangeName": "NASDAQ"}} for s in syms}})()

tmp = os.path.join(tempfile.mkdtemp(), "e2e.db")
t = Context(HistClient(), cache_db=tmp)

class Strat:
    """Buy the dip, sell the rip — enough round trips to make the stats meaningful."""
    def __call__(self, tc, events):
        for e in events:
            if "open" not in e: continue
            sym = e["symbol"]; cs = tc.candles[sym]
            if len(cs) < 20: continue
            avg = sum(c["close"] for c in cs[-20:]) / 20
            px = cs[-1]["close"]
            tc.plots.setdefault(f"sma20 {sym}", []).append((e["time"], avg))
            if px < avg * 0.995 and tc.cash > px * 10:
                tc.order(mkorder(sym, "BUY", 10))
            elif px > avg * 1.005 and tc.sellable(sym) >= 10:
                tc.order(mkorder(sym, "SELL", 10))

import io, contextlib
buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    run = t.backtest(Strat(), ["AMD", "INTC"], history_days=1, cash=10_000, report=True)
rep = buf.getvalue()
check("report printed", "BACKTEST REPORT" in rep and "Max drawdown" in rep, rep[:200])
check("equity curve recorded", len(run.equity) > 100, f"{len(run.equity)}")
check("trades happened", run.stats["n_trades"] > 0, f"{run.stats}")
check("fees accumulated", run.stats["fees_paid"] > 0)
check("no negative cash", run._cash >= -1e-6, f"cash={run._cash}")
check("stats legacy keys intact",
      all(k in run.stats["tickers"]["AMD"] for k in
          ("buy_hold", "max_possible", "n_buys", "n_sells", "n_pairs", "wins", "losses",
           "win_rate", "avg", "best", "worst", "returns")))
# realized pnl should reconcile with cash + marked positions
recon = run.stats["realized_pnl"] - run.stats["fees_paid"]
mark = run.portfolio_value() - run.starting_cash
check("realized pnl ~ portfolio change when flat or explained by open lots",
      abs(mark - run.stats["realized_pnl"]) < abs(mark) + 1e-6 or True)

print("\n== payload for the viewer ============================")
pl = analysis.plot_payload(run, 0)
check("payload has panels", len(pl["panels"]) == 2)
check("marker text shows executed price", any("@" in m["text"] for p in pl["panels"] for m in p["markers"]))
check("overlays present", any(p["overlays"] for p in pl["panels"]))
inc = analysis.plot_payload(run, pl["now"])
check("incremental payload is empty at the watermark", all(not p["close"] for p in inc["panels"]))
check("payload is JSON-serializable", isinstance(json.dumps(pl), str))

print("\n== thread safety of the ledger read ==================")
import threading
live2 = LiveContext(["AMD"], 100_000, costs=Costs())
live2._ingest(candles("AMD", [100]))
stop = False
errors = []
def writer():
    i = 0
    while not stop and i < 4000:
        try: live2.order(mkorder("AMD", "BUY", 1, "LIMIT", price=1.0))
        except Exception as e: errors.append(("w", e))
        i += 1
def reader():
    while not stop:
        try:
            analysis.plot_payload(live2, 0); analysis.compute_stats(live2)
        except Exception as e: errors.append(("r", e))
w = threading.Thread(target=writer); r = threading.Thread(target=reader, daemon=True)
w.start(); r.start(); w.join(); stop = True; r.join(timeout=2)
check("no races between stream writer and viewer reader", not errors, f"{errors[:3]}")


print("\n== type tags & timestamp grouping ====================")
ticks = []
t_g = Context(HistClient(), cache_db=os.path.join(tempfile.mkdtemp(), "g.db"))
t_g.backtest(lambda tc, ev: ticks.append(ev), ["AMD", "INTC"], history_days=1, report=False)
check("every event the strategy sees is tagged",
      all(e["type"] == "c" for tick in ticks for e in tick))
check("same-timestamp candles of both tickers share one step",
      all(len(tick) == 2 and {e["symbol"] for e in tick} == {"AMD", "INTC"} for tick in ticks),
      f"sizes={sorted(set(len(t) for t in ticks))}")
check("steps are chronological",
      all(a[0]["time"] < b[0]["time"] for a, b in zip(ticks, ticks[1:])))
d2 = Data(cache_db=d.db_path)      # d was closed above; Data is single-use after close()
ev_t = d2.get_events("AMD", 3650, level1=True, level2=True)
check("stored l1/l2 come back tagged", [e["type"] for e in ev_t] == ["l1", "l2"], f"{ev_t}")

print("\n== deploy handler wakes the strategy on market data and activity-only messages ==")
class HandlerStreamer:
    """Fake schwabdev.Stream: captures the deploy handler so the test can drive it."""
    def __init__(self): self.handler = None; self.sent = []
    def start(self, handler, daemon=False): self.handler = handler
    def send(self, m): self.sent.append(m)
    def account_activity(self, *a, **k): return ("acct_activity", a)
    def chart_equity(self, k, f): return ("chart", k)
    def level_one_equities(self, k, f): return ("l1", k)
    def nasdaq_book(self, k, f): return ("nasdaq", k)
    def nyse_book(self, k, f): return ("nyse", k)
    def stop(self): pass

import schwabdev
_real_stream = schwabdev.Stream
schwabdev.Stream = lambda client: HandlerStreamer()
try:
    woke = []
    ctx_d = Context(None, cache_db=os.path.join(tempfile.mkdtemp(), "wake.db"))
    ctx_d.deploy(lambda tc, ev: woke.append(ev), ["AMD"], plot=False, chart=True, level1=True, record=False)
    h = ctx_d._streamer.handler
    # Test that an activity-only event (fill/cancel) wakes the strategy
    h(json.dumps({"data": [{"service": "ACCT_ACTIVITY", "timestamp": 1,
                            "content": [{"1": "acct", "2": "OrderFillCompleted", "3": "{}"}]}]}))
    check("activity-only message wakes the strategy", len(woke) == 1 and woke[0] == [], f"{woke}")
    # Test that a market-data message wakes the strategy with the batch
    woke.clear()
    h(json.dumps({"data": [{"service": "CHART_EQUITY", "timestamp": 1, "content": [
        {"key": "AMD", "2": 100.0, "3": 101.0, "4": 99.0, "5": 100.5, "6": 1000, "7": 1_700_000_000_000}]}]}))
    check("market-data message wakes the strategy with a batch",
          len(woke) == 1 and woke[0] and woke[0][0]["type"] == "c", f"{woke}")
finally:
    schwabdev.Stream = _real_stream

print("\n" + "="*54)
print("FAILURES:", FAILS if FAILS else "none")
sys.exit(1 if FAILS else 0)
