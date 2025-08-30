r"""
End-to-end loader with JSON sanitization:
- Ensure MySQL database + table exist (created automatically)
- Read ALL symbols from CSV
- Skip symbols already present in DB (no repeat fetch/insert)
- Fetch Yahoo Finance summary + financials for missing symbols
- Sanitize payload so it is valid JSON for MySQL (no NaN/Infinity, etc.)
- Insert into MySQL (JSON column), COMMIT PER SYMBOL
- Emit symbols_loaded.csv of successful inserts only
(NO JSON file output)
"""

import json
import math
import time
from pathlib import Path
from datetime import datetime, timezone, UTC, date  # <-- UTC added

import numpy as np
import pandas as pd
import yfinance as yf
import mysql.connector as mysql
from tqdm import tqdm

# ---------- Config ----------
CSV_PATH = r"D:\C Documents\ETL\stocks_full.csv"   # <- your CSV path
SLEEP_SECONDS_BETWEEN_CALLS = 1.0                  # only applied to fetched symbols

# MySQL connection (your credentials)
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "Dolby1127@@"
MYSQL_DATABASE = "marketdata"
MYSQL_TABLE = "yahoo_financials"
# ----------------------------


def utc_naive_now() -> datetime:
    """
    Return a UTC timestamp that is timezone-aware internally,
    then drop tzinfo so MySQL DATETIME accepts it.
    """
    return datetime.now(UTC).replace(tzinfo=None)


def find_symbol_column(df: pd.DataFrame) -> str:
    for c in ["symbol", "ticker", "Symbol", "Ticker", "SYMBOL", "TICKER"]:
        if c in df.columns:
            return c
    return df.columns[0]


def _str_key(x):
    if isinstance(x, pd.Timestamp):
        return x.isoformat(sep=" ")
    return str(x)


def df_to_jsonable(df: pd.DataFrame) -> dict:
    """Safely convert a DataFrame to a JSON-serializable nested dict."""
    if df is None or df.empty:
        return {}
    df2 = df.copy()
    try:
        df2.columns = [_str_key(c) for c in df2.columns]
    except Exception:
        df2.columns = [str(c) for c in df2.columns]
    try:
        df2.index = [_str_key(i) for i in df2.index]
    except Exception:
        df2.index = [str(i) for i in df2.index]
    df2 = df2.where(pd.notnull(df2), None)
    return df2.to_dict()


def clean_json(obj):
    """Sanitize for MySQL JSON: NaN→None, numpy scalars→Python, datetime→ISO, etc."""
    if obj is None:
        return None

    try:
        if pd.isna(obj):
            return None
    except Exception:
        pass

    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        val = float(obj)
        if math.isnan(val) or math.isinf(val):
            return None
        return val
    if isinstance(obj, (np.bool_,)):
        return bool(obj)

    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj

    if isinstance(obj, (int, bool, str)):
        return obj

    if isinstance(obj, (bytes, bytearray)):
        return obj.decode("utf-8", errors="replace")

    if isinstance(obj, (datetime, date, pd.Timestamp)):
        try:
            if isinstance(obj, datetime) and obj.tzinfo is None:
                obj = obj.replace(tzinfo=timezone.utc)
        except Exception:
            pass
        try:
            return obj.isoformat()
        except Exception:
            return str(obj)

    if isinstance(obj, dict):
        return {str(k): clean_json(v) for k, v in obj.items()}

    if isinstance(obj, (list, tuple, set)):
        return [clean_json(v) for v in obj]

    return str(obj)


def fetch_for_symbol(symbol: str) -> dict:
    t = yf.Ticker(symbol)
    try:
        try:
            info = t.get_info() or {}
        except AttributeError:
            info = t.info or {}
    except Exception as e:
        info = {"_error": f"{type(e).__name__}: {e}"}

    payload = {
        "info": info,
        "cashflow": {
            "yearly": df_to_jsonable(getattr(t, "cashflow", None)),
            "quarterly": df_to_jsonable(getattr(t, "quarterly_cashflow", None)),
        },
        "balancesheet": {
            "yearly": df_to_jsonable(getattr(t, "balance_sheet", None)),
            "quarterly": df_to_jsonable(getattr(t, "quarterly_balance_sheet", None)),
        },
        "incomestatement": {
            "yearly": df_to_jsonable(getattr(t, "financials", None)),
            "quarterly": df_to_jsonable(getattr(t, "quarterly_financials", None)),
        },
    }
    return clean_json(payload)


# -------------------- MySQL helpers --------------------
def connect_server():
    return mysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        autocommit=True,  # server-level ops are fine with autocommit
        charset="utf8mb4",
    )


def ensure_database(conn):
    ddl = (
        f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DATABASE}` "
        "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
    )
    with conn.cursor() as cur:
        cur.execute(ddl)


def connect_database():
    # Explicit transaction control for per-symbol commit
    return mysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        autocommit=False,  # we'll commit manually after each insert
        charset="utf8mb4",
    )


def ensure_table(conn):
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{MYSQL_TABLE}` (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        symbol VARCHAR(32) NOT NULL,
        payload JSON NOT NULL,
        loaded_at DATETIME NOT NULL,
        UNIQUE KEY uq_symbol_loaded_at (symbol, loaded_at),
        INDEX idx_symbol (symbol)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


def insert_symbol_payload(conn, symbol: str, payload: dict) -> None:
    """Insert and COMMIT one symbol. Raises on failure."""
    sql = f"""INSERT INTO `{MYSQL_TABLE}` (symbol, payload, loaded_at)
              VALUES (%s, %s, %s)"""
    try:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    symbol,
                    json.dumps(payload, ensure_ascii=False, allow_nan=False),
                    utc_naive_now(),  # naive UTC avoids tz issues for DATETIME
                ),
            )
        conn.commit()  # commit per symbol
    except mysql.Error as e:
        conn.rollback()
        raise RuntimeError(
            f"MySQL insert failed for {symbol}: {getattr(e, 'errno', '')} "
            f"{getattr(e, 'sqlstate', '')} {getattr(e, 'msg', str(e))}"
        ) from e


def get_existing_symbols(conn) -> set:
    sql = f"SELECT DISTINCT symbol FROM `{MYSQL_TABLE}`"
    with conn.cursor() as cur:
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            return {r[0] for r in rows} if rows else set()
        except mysql.Error:
            return set()
# ------------------------------------------------------


def main():
    print("Connecting to MySQL server ...")
    srv = connect_server()
    print("✅ Connected to MySQL server")

    ensure_database(srv)
    srv.close()
    print(f"✅ Database ensured: {MYSQL_DATABASE}")

    conn = connect_database()
    ensure_table(conn)
    print(f"✅ Table ensured: {MYSQL_TABLE}")

    # Read ALL symbols
    csv_path = Path(CSV_PATH)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found at {csv_path}")

    df = pd.read_csv(csv_path)
    sym_col = find_symbol_column(df)
    symbols = (
        df[sym_col]
        .dropna()
        .astype(str)
        .str.strip()
        .replace({"": None})
        .dropna()
        .drop_duplicates()
        .tolist()
    )
    if not symbols:
        raise ValueError("No symbols found in the CSV.")
    print(f"Found {len(symbols)} symbol(s) in CSV.")

    # Skip existing
    already = get_existing_symbols(conn)
    if already:
        print(f"{len(already)} symbol(s) already in DB will be skipped.")
    to_fetch = [s for s in symbols if s not in already]
    print(f"{len(to_fetch)} symbol(s) to fetch.")

    loaded_ok = []
    failed = 0

    # Fetch → Insert → Commit per symbol
    for sym in tqdm(to_fetch, desc="Fetching symbols", unit="sym"):
        try:
            payload = fetch_for_symbol(sym)
            # Insert whatever we fetched; commit happens inside insert_symbol_payload
            insert_symbol_payload(conn, sym, payload)
            loaded_ok.append(sym)  # record only if committed
            time.sleep(SLEEP_SECONDS_BETWEEN_CALLS)
        except Exception as e:
            failed += 1
            tqdm.write(f"❌ {sym} failed: {type(e).__name__}: {e}")

    conn.close()

    # Save successfully loaded ONLY
    sym_out = Path(CSV_PATH).with_name("symbols_loaded.csv")
    pd.DataFrame({"symbol": loaded_ok}).to_csv(sym_out, index=False)
    print(f"Wrote {len(loaded_ok)} newly loaded symbols to {sym_out}")
    if failed:
        print(f"⚠️ {failed} symbol(s) failed to insert (see logs).")
    print("✅ Done (skipped existing, committed per symbol, no JSON file created).")


if __name__ == "__main__":
    main()
