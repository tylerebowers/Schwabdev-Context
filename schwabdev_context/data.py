import contextlib
import datetime
import json
import os
import sqlite3
import time as _time

_CHART_DDL = ("time INTEGER PRIMARY KEY, open REAL, high REAL, low REAL, close REAL, volume REAL")
_L1_DDL = "time INTEGER, bid REAL, ask REAL, last REAL, bid_size REAL, ask_size REAL"
_L2_DDL = "time INTEGER, bids TEXT, asks TEXT"


class Data:
    def __init__(self, cache_db="~/.schwabdev/candles.db", client=None):
        self.db_path = os.path.expanduser(cache_db)
        os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
        self._client = client
        self._con = sqlite3.connect(self.db_path, check_same_thread=False)
        self._tables = set() 

    @staticmethod
    def table(prefix, ticker):
        """Per-ticker table name, e.g. ("chart", "BRK/B") -> "chart_BRK_B"."""
        return f"{prefix}_" + "".join(ch if ch.isalnum() else "_" for ch in ticker)

    def close(self):
        """Close the streaming write connection, if one was opened."""
        if self._con is not None:
            with contextlib.suppress(Exception):
                self._con.commit()
                self._con.close()
            self._con = None
            self._tables.clear()

    # reading (backtest) ------------------------------------------------------

    def get_candles(self, ticker, history_days=90, start=None, end=None, extended_hours=False):
        """Chronological minute candles for `ticker` over the requested dates, as
        [{"symbol", "time" (ms), "open", "high", "low", "close", "volume", "type": "c"}].
        The date range is `start`..`end` if both are given. `end` defaults to now. `history_days`
        may be used to automatically calculate start.
        Missing ranges are fetched via the client (when one is set) and cached; everything is
        read back from SQLite. `extended_hours` requests pre/after-market candles from Schwab
        for any range that has to be fetched."""
        ms = lambda d: int(d.timestamp() * 1000)
        to_dt = lambda m: datetime.datetime.fromtimestamp(m / 1000, datetime.timezone.utc)

        table = self.table("chart", ticker)  # same table the chart-equity stream records to
        self._con.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({_CHART_DDL})')

        now = datetime.datetime.now(datetime.timezone.utc)
        if start is not None:
            end = end or now  # start wins; end defaults to now (history_days ignored)
        elif end is not None:
            start = end - datetime.timedelta(days=history_days)  # end given: history_days defines start
        else:
            end = now  # neither: history_days to now
            start = end - datetime.timedelta(days=history_days)
        need_start, need_end = start, end
        earliest, latest = self._con.execute(f'SELECT MIN(time), MAX(time) FROM "{table}"').fetchone()

        # determine which ranges are missing from the cache
        ranges = []
        if latest is None:
            ranges.append((need_start, need_end)) # empty cache: full range
        else:
            if to_dt(latest) < need_end: # recent gap (latest -> end)
                ranges.append((max(to_dt(latest), need_start), need_end))
            if earliest > ms(need_start): # backfill older history
                ranges.append((need_start, min(to_dt(earliest), need_end)))

        # fetch each missing range in <=10-day chunks and cache it (INSERT OR IGNORE dedupes) with no client we run cache-only and skip all API calls.
        for start_r, end_r in (ranges if self._client else []):
            cur = end_r
            while cur > start_r:
                chunk_start = max(start_r, cur - datetime.timedelta(days=10))
                data = self._fetch_candles(ticker, chunk_start, cur, extended_hours)
                if not data:
                    break
                self._con.executemany(
                    f'INSERT OR IGNORE INTO "{table}" (time, open, high, low, close, volume) VALUES (?,?,?,?,?,?)',
                    [(c["datetime"], c["open"], c["high"], c["low"], c["close"], c["volume"])
                        for c in data])
                self._con.commit()
                cur = chunk_start
                _time.sleep(0.5)  # rate-limit: 2 requests/sec

        rows = self._con.execute(f'SELECT time, open, high, low, close, volume FROM "{table}" '
                            "WHERE time >= ? AND time <= ? ORDER BY time",
                            (ms(need_start), ms(need_end))).fetchall()
        return [{"symbol": ticker, "time": t, "open": o, "high": h, "low": l, "close": c,
                 "volume": v, "type": "c"} for t, o, h, l, c, v in rows]


    def _fetch_candles(self, ticker, start, end, extended_hours=False):
        """One price-history request, returning the raw candle list (empty on any failure).
        `extended_hours` requests pre/after-market candles from Schwab."""
        try:
            r = self._client.price_history(ticker, periodType="day", frequencyType="minute",
                                           frequency=1, startDate=start, endDate=end,
                                           needExtendedHoursData=extended_hours)
        except Exception as exc:
            print(f"[data] {ticker} price_history failed: {exc}")
            return []
        if not r.ok:
            print(f"[data] {ticker} price_history {r.status_code} for "
                  f"{start:%Y-%m-%d}..{end:%Y-%m-%d}")
            return []
        try:
            return r.json().get("candles", []) or []
        except ValueError:
            return []

    def get_events(self, ticker, history_days=90, start=None, end=None, level1=False, level2=False):
        """Chronological RECORDED level-1 quotes / level-2 book snapshots for `ticker` over the
        requested dates, in the same event shapes `parse()` produces. Read-only: Schwab offers
        no l1/l2 history, so these can only come from a prior `record()` or live `deploy()`.
        The date range is `start`..`end` if both are given. `end` defaults to now. `history_days`
        may be used to automatically calculate start.
        Missing tables simply yield no events.
        """
        now = datetime.datetime.now(datetime.timezone.utc)
        if start is not None:
            end = end or now  # start wins; end defaults to now (history_days ignored)
        elif end is not None:
            start = end - datetime.timedelta(days=history_days)  # end given: history_days defines start
        else:
            end = now  # neither: history_days to now
            start = end - datetime.timedelta(days=history_days)
        since = int(start.timestamp() * 1000)
        until = int(end.timestamp() * 1000)
        events = []
        exists = lambda t: self._con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (t,)).fetchone()

        if level1 and exists(t := self.table("l1", ticker)):
            for time, bid, ask, last, bsz, asz in self._con.execute(
                    f'SELECT time, bid, ask, last, bid_size, ask_size FROM "{t}" '
                    "WHERE time >= ? AND time <= ? ORDER BY time", (since, until)):
                events.append({"symbol": ticker, "time": time, "bid": bid, "ask": ask,
                                "last": last, "bid_size": bsz, "ask_size": asz, "type": "l1"})
        if level2 and exists(t := self.table("l2", ticker)):
            for time, bids, asks in self._con.execute(
                    f'SELECT time, bids, asks FROM "{t}" WHERE time >= ? AND time <= ? ORDER BY time',
                    (since, until)):
                events.append({"symbol": ticker, "time": time, "type": "l2",
                                "bids": json.loads(bids or "[]"), "asks": json.loads(asks or "[]")})
        events.sort(key=lambda e: e["time"])
        return events

    # streaming (shared by record() and Trader.deploy()) ----------------------

    def parse(self, msg):
        """Decode one raw stream message into normalized events, keyed by kind. Every market
        event also carries a "type" tag ("c" candle / "l1" quote / "l2" book) so consumers never
        have to sniff its shape:
            chart    : {"symbol","time","open","high","low","close","volume","type":"c"}
            l1       : {"symbol","time","bid","ask","last","bid_size","ask_size","type":"l1"}
                       (level-one streams DELTAS: unchanged fields arrive as None)
            l2       : {"symbol","time","bids","asks","type":"l2"}  (full book snapshots)
            activity : the raw ACCT_ACTIVITY content item, untouched
        This is the one place stream field numbers are interpreted; everything downstream
        (recording via `write`, the live session, backtest replay) speaks these dicts.

        Every field is read with `.get` and incomplete items are dropped. This runs on the stream
        thread inside the message handler, where a KeyError on one malformed item would take the
        whole batch — and, depending on the transport, the reader loop — down with it."""
        events = {"chart": [], "l1": [], "l2": [], "activity": []}
        try:
            data = json.loads(msg)
        except Exception:
            return events
        if not isinstance(data, dict):
            return events
        for blk in data.get("data", []) or []:
            svc, ts = blk.get("service"), blk.get("timestamp", 0)
            for it in blk.get("content", []) or []:
                sym = str(it.get("key", "")).split(" ")[0].upper()
                if svc == "CHART_EQUITY":
                    candle = {"symbol": sym, "open": it.get("2"), "high": it.get("3"),
                              "low": it.get("4"), "close": it.get("5"),
                              "volume": it.get("6", 0.0), "time": it.get("7"), "type": "c"}
                    if sym and candle["time"] is not None and None not in (
                            candle["open"], candle["high"], candle["low"], candle["close"]):
                        events["chart"].append(candle)
                elif svc == "LEVELONE_EQUITIES":
                    if sym:
                        events["l1"].append({"symbol": sym, "time": ts, "bid": it.get("1"),
                                             "ask": it.get("2"), "last": it.get("3"),
                                             "bid_size": it.get("4"), "ask_size": it.get("5"),
                                             "type": "l1"})
                elif svc in ("NASDAQ_BOOK", "NYSE_BOOK"):
                    if sym:
                        events["l2"].append({"symbol": sym, "time": it.get("1", ts), "type": "l2",
                                             "bids": it.get("2") or [], "asks": it.get("3") or []})
                elif svc == "ACCT_ACTIVITY":
                    events["activity"].append(it)
        return events

    def write(self, events):
        """Persist parsed stream events into the per-ticker tables. Called from the stream thread,
        so the connection is opened lazily there (check_same_thread=False) and reused. Candles use
        INSERT OR REPLACE — the stream re-sends the forming minute bar, and the final send wins,
        landing in the same chart_{ticker} table `get_candles` reads. The l1/l2 tables are
        append-only and grow by thousands of rows a day, so each gets a time index: without one
        every `get_events` range scan reads (and sorts) the entire table."""
        if not (events["chart"] or events["l1"] or events["l2"]):
            return
        if self._con is None:
            self._con = sqlite3.connect(self.db_path, check_same_thread=False)
        con = self._con

        def ensure(table, ddl, index=False):
            if table not in self._tables:
                con.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({ddl})')
                if index:
                    con.execute(f'CREATE INDEX IF NOT EXISTS "{table}_time" ON "{table}" (time)')
                self._tables.add(table)

        try:
            for e in events["chart"]:
                t = self.table("chart", e["symbol"])
                ensure(t, _CHART_DDL)
                con.execute(f'INSERT OR REPLACE INTO "{t}" VALUES (?,?,?,?,?,?)',
                            (e["time"], e["open"], e["high"], e["low"], e["close"], e["volume"]))
            for e in events["l1"]:
                t = self.table("l1", e["symbol"])
                ensure(t, _L1_DDL, index=True)
                con.execute(f'INSERT INTO "{t}" VALUES (?,?,?,?,?,?)',
                            (e["time"], e["bid"], e["ask"], e["last"], e["bid_size"], e["ask_size"]))
            for e in events["l2"]:
                t = self.table("l2", e["symbol"])
                ensure(t, _L2_DDL, index=True)
                con.execute(f'INSERT INTO "{t}" VALUES (?,?,?)',
                            (e["time"], json.dumps(e["bids"]), json.dumps(e["asks"])))
            con.commit()
        except sqlite3.Error as exc:
            print(f"[data] write failed: {exc}")
            with contextlib.suppress(Exception):
                con.rollback()

    def subscribe(self, streamer, tickers, chart=True, level1=False, level2=False):
        """Send the market-data subscriptions for `tickers` on an already-started streamer. Book
        (level-2) symbols are routed to their listing exchange's stream (NASDAQ_BOOK / NYSE_BOOK)
        via a one-shot client.quotes lookup; without a client (or if the lookup fails) they all go
        to NASDAQ_BOOK, since guessing beats raising inside a live session."""
        tickers = list(tickers)
        keys = ",".join(tickers)
        if chart:
            streamer.send(streamer.chart_equity(keys, "0,1,2,3,4,5,6,7"))
        if level1:
            streamer.send(streamer.level_one_equities(keys, "0,1,2,3,4,5"))
        if level2:
            nasdaq, nyse = self._split_by_exchange(tickers)
            if nasdaq:
                streamer.send(streamer.nasdaq_book(",".join(nasdaq), "0,1,2,3"))
            if nyse:
                streamer.send(streamer.nyse_book(",".join(nyse), "0,1,2,3"))

    def _split_by_exchange(self, tickers):
        """(nasdaq, nyse) split of `tickers` by listing exchange, for book subscriptions."""
        if not self._client:
            print("[data] no client: routing all level-2 subscriptions to NASDAQ_BOOK")
            return list(tickers), []
        try:
            r = self._client.quotes(list(tickers))
            q = r.json() if r.ok else {}
        except Exception as exc:
            print(f"[data] exchange lookup failed ({exc}); routing level-2 to NASDAQ_BOOK")
            return list(tickers), []
        nasdaq, nyse = [], []
        for t in tickers:
            ex = str(q.get(t, {}).get("reference", {}).get("exchangeName", "")).upper()
            (nasdaq if "NASDAQ" in ex or not ex else nyse).append(t)
        return nasdaq, nyse

    # writing (live recording) ------------------------------------------------

    def record(self, tickers, level1=True, level2=True, chart=True, verbose=True,
               **start_auto_kwargs):
        """Stream live market data straight into the DB and return the streamer (call `.stop()` to
        end). Records exactly the flagged data types:
            chart_{ticker} : minute candles from CHART_EQUITY (the table get_candles reads, so
                             recorded sessions backfill the backtest cache too)
            l1_{ticker}    : level-one quotes, fields 0-5      (level1=True)
            l2_{ticker}    : full order-book snapshots         (level2=True; NASDAQ/NYSE per symbol)

        The stream is started with `start_auto`, so it opens/closes with market hours and
        resubscribes across sessions — leave it running and it records every trading day. Any
        `start_auto` kwargs (start_time, stop_time, on_days, daemon, ...) pass through; use
        daemon=False if recording is the process's only job. `verbose=False` silences the
        per-message counts, which at level-2 rates are thousands of lines an hour."""
        def handle(msg):
            parsed = self.parse(msg)
            if verbose:
                print(f"{datetime.datetime.now():%H:%M:%S} chart:{len(parsed['chart']):<3} "
                      f"l1:{len(parsed['l1']):<3} l2:{len(parsed['l2']):<3} "
                      f"activity:{len(parsed['activity']):<3}")
            self.write(parsed)

        import schwabdev
        tickers = [t.strip().upper() for t in tickers]
        streamer = schwabdev.Stream(self._client)
        streamer.start_auto(handle, daemon=False, **start_auto_kwargs)
        _time.sleep(1.0)  # let the socket finish connecting before subscriptions are sent
        self.subscribe(streamer, tickers, chart=chart, level1=level1, level2=level2)
        return streamer
