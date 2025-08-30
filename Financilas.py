#!/usr/bin/env python3
import os, json, math, sys, traceback
from datetime import datetime
import mysql.connector

# ---- DB config
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_NAME = os.getenv("DB_NAME", "marketdata")
DB_USER = os.getenv("DB_USER", "root")
DB_PASS = os.getenv("DB_PASS", "Dolby1127@@")

# Tunables
COMMIT_EVERY_SYMBOLS = int(os.getenv("COMMIT_EVERY_SYMBOLS", "20"))  # commit after N symbols

SCHEMA_CREATE = """
CREATE TABLE IF NOT EXISTS financials (
  stock              VARCHAR(32) NOT NULL,
  yf_name            VARCHAR(255),
  statement_type     VARCHAR(4) NOT NULL,
  metric             VARCHAR(191) NOT NULL,
  stockcurrency      VARCHAR(16),
  financialcurrency  VARCHAR(16),
  calendar_year      INT,
  period             INT,
  value              DOUBLE,
  date               DATE,
  PRIMARY KEY (stock, statement_type, metric, date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

def connect():
    return mysql.connector.connect(
        host=DB_HOST, port=DB_PORT, database=DB_NAME,
        user=DB_USER, password=DB_PASS
    )

def ensure_financials_table(cur):
    # Try creating with the correct schema
    try:
        cur.execute(SCHEMA_CREATE)
        return
    except mysql.connector.Error as e:
        # If schema already exists but old/invalid, try to fix
        if e.errno != 1170:  # 1170 = BLOB/TEXT in key
            raise

    # Repair path: drop PK if exists, change metric to VARCHAR, re-add PK
    # (safe no-ops if already correct)
    try:
        cur.execute("ALTER TABLE financials DROP PRIMARY KEY")
    except mysql.connector.Error:
        pass
    try:
        cur.execute("ALTER TABLE financials MODIFY COLUMN metric VARCHAR(191) NOT NULL")
    except mysql.connector.Error:
        pass
    # Make sure 'stock' column exists and is bounded for PK
    try:
        cur.execute("ALTER TABLE financials MODIFY COLUMN stock VARCHAR(32) NOT NULL")
    except mysql.connector.Error:
        pass
    cur.execute("""
        ALTER TABLE financials
        ADD PRIMARY KEY (stock, statement_type, metric, date)
    """)

def quarter_from_date(d: datetime) -> int:
    return (d.month - 1)//3 + 1

def normalize_financials(symbol, info_json):
    out = []
    info = (info_json or {}).get("info", {}) or {}
    stockcurrency = info.get("currency")
    financialcurrency = info.get("financialCurrency") or info.get("financialcurrency")
    yf_name = info.get("longName") or info.get("shortName") or info.get("displayName")

    folder_map = {"cashflow": "CF", "incomestatement": "IS", "balancesheet": "BS"}

    for folder, stype in folder_map.items():
        block = (info_json or {}).get(folder) or {}
        for freq in ("yearly", "quarterly"):
            freq_block = block.get(freq) or {}
            for dt_str, metrics in freq_block.items():
                # keys look like "2024-12-31 00:00:00"
                try:
                    d = datetime.strptime(dt_str[:10], "%Y-%m-%d")
                except Exception:
                    try:
                        d = datetime.fromisoformat(dt_str.split()[0])
                    except Exception:
                        continue
                cal_year = d.year
                period = 4 if freq == "yearly" else quarter_from_date(d)

                for metric, val in (metrics or {}).items():
                    if not metric:
                        continue
                    if isinstance(val, (dict, list)):
                        continue
                    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
                        val = None
                    out.append((
                        symbol, yf_name, stype, metric, stockcurrency,
                        financialcurrency, cal_year, period, val, d.date()
                    ))
    return out

def describe_columns(cur, table):
    cur.execute(f"DESCRIBE `{table}`")
    return [r[0] for r in cur.fetchall()]

def find_symbol_and_json_columns(cur):
    cols = describe_columns(cur, "yahoo_financials")
    # symbol column
    sym_col = None
    for c in ("stock", "symbol", "ticker", "SYMBOL", "TICKER"):
        if c in cols:
            sym_col = c
            break
    if not sym_col:
        raise RuntimeError("Could not find a symbol column in yahoo_financials (looked for stock/symbol/ticker).")

    # json column
    json_col = None
    for c in ("json", "payload", "data", "JSON", "PAYLOAD", "DATA"):
        if c in cols:
            json_col = c
            break
    if not json_col:
        raise RuntimeError("Could not find a JSON column in yahoo_financials (looked for json/payload/data).")

    return sym_col, json_col

def get_all_symbols(cur, sym_col):
    cur.execute(f"SELECT DISTINCT `{sym_col}` FROM yahoo_financials ORDER BY `{sym_col}` ASC")
    return [r[0] for r in cur.fetchall()]

def load_one_row(cur, sym_col, json_col, symbol):
    # Fetch *raw* column; don't use JSON_EXTRACT so it works whether it's JSON or TEXT
    cur.execute(
        f"SELECT `{sym_col}`, `{json_col}` FROM yahoo_financials WHERE `{sym_col}` = %s LIMIT 1",
        (symbol,)
    )
    return cur.fetchone()

def parse_json_value(j):
    """Robustly convert a MySQL JSON/TEXT/BLOB value into a Python dict."""
    obj = j
    if isinstance(j, (bytes, bytearray)):
        j = j.decode("utf-8", errors="replace")
    if isinstance(j, str):
        try:
            obj = json.loads(j)
        except Exception:
            try:
                obj = json.loads(j.strip('"').encode('utf-8').decode('unicode_escape'))
            except Exception:
                obj = {}
    if not isinstance(obj, dict):
        try:
            obj = json.loads(obj)
        except Exception:
            obj = {}
    return obj

def upsert_financials(cur, rows):
    if not rows:
        return 0
    insert_sql = """
      INSERT INTO financials
        (stock, yf_name, statement_type, metric, stockcurrency,
         financialcurrency, calendar_year, period, value, date)
      VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
      ON DUPLICATE KEY UPDATE
        yf_name=VALUES(yf_name),
        stockcurrency=VALUES(stockcurrency),
        financialcurrency=VALUES(financialcurrency),
        calendar_year=VALUES(calendar_year),
        period=VALUES(period),
        value=VALUES(value)
    """
    cur.executemany(insert_sql, rows)
    return len(rows)

def verify(cur, symbol, limit=10):
    cur.execute("""
      SELECT stock, yf_name, statement_type, metric, stockcurrency,
             financialcurrency, calendar_year, period, value, date
      FROM financials
      WHERE stock = %s
      ORDER BY date DESC, statement_type, metric
      LIMIT %s
    """, (symbol, limit))
    return cur.fetchall()

def main():
    conn = connect()
    try:
        cur = conn.cursor()
        # Ensure/repair financials schema
        ensure_financials_table(cur)
        conn.commit()

        # Detect columns on yahoo_financials
        sym_col, json_col = find_symbol_and_json_columns(cur)

        # --- NEW: loop over all DISTINCT symbols
        symbols = get_all_symbols(cur, sym_col)
        if not symbols:
            print("No symbols found in yahoo_financials.")
            return

        print(f"Processing {len(symbols):,} symbol(s) from yahoo_financials...")
        total_rows = 0
        errors = 0

        for i, symbol in enumerate(symbols, 1):
            try:
                row = load_one_row(cur, sym_col, json_col, symbol)
                if not row:
                    print(f"[WARN] No JSON row found for {symbol}.")
                    continue

                stock, j = row
                obj = parse_json_value(j)
                rows = normalize_financials(stock, obj)
                upserted = upsert_financials(cur, rows)
                total_rows += upserted

                if i % COMMIT_EVERY_SYMBOLS == 0:
                    conn.commit()
                    print(f"…processed {i:,}/{len(symbols):,} symbols "
                          f"(rows upserted so far: {total_rows:,})")

            except Exception as e:
                errors += 1
                print(f"[WARN] Failed on symbol {symbol}: {e}", file=sys.stderr)
                traceback.print_exc(limit=2)

        # final commit
        conn.commit()

        print("\n=== Done ===")
        print(f"Symbols processed: {len(symbols):,}")
        print(f"Symbols with errors: {errors:,}")
        print(f"Total financial rows upserted: {total_rows:,}")

        # Optional: small sample printout for the last symbol
        last_symbol = symbols[-1]
        sample = verify(cur, last_symbol, limit=10)
        print(f"\nSample rows for {last_symbol}:")
        for r in sample:
            print(r)

    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()

if __name__ == "__main__":
    main()

































#for 1 
'''
#!/usr/bin/env python3
import os, json, math
from datetime import datetime
import mysql.connector

# ---- DB config
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_NAME = os.getenv("DB_NAME", "marketdata")
DB_USER = os.getenv("DB_USER", "root")
DB_PASS = os.getenv("DB_PASS", "Dolby1127@@")

SCHEMA_CREATE = """
CREATE TABLE IF NOT EXISTS financials (
  stock              VARCHAR(32) NOT NULL,
  yf_name            VARCHAR(255),
  statement_type     VARCHAR(4) NOT NULL,
  metric             VARCHAR(191) NOT NULL,
  stockcurrency      VARCHAR(16),
  financialcurrency  VARCHAR(16),
  calendar_year      INT,
  period             INT,
  value              DOUBLE,
  date               DATE,
  PRIMARY KEY (stock, statement_type, metric, date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

def connect():
    return mysql.connector.connect(
        host=DB_HOST, port=DB_PORT, database=DB_NAME,
        user=DB_USER, password=DB_PASS
    )

def ensure_financials_table(cur):
    # Try creating with the correct schema
    try:
        cur.execute(SCHEMA_CREATE)
        return
    except mysql.connector.Error as e:
        # If schema already exists but old/invalid, try to fix
        if e.errno != 1170:  # 1170 = BLOB/TEXT in key
            raise

    # Repair path: drop PK if exists, change metric to VARCHAR, re-add PK
    # (safe no-ops if already correct)
    try:
        cur.execute("ALTER TABLE financials DROP PRIMARY KEY")
    except mysql.connector.Error:
        pass
    try:
        cur.execute("ALTER TABLE financials MODIFY COLUMN metric VARCHAR(191) NOT NULL")
    except mysql.connector.Error:
        pass
    # Make sure 'stock' column exists and is bounded for PK
    try:
        cur.execute("ALTER TABLE financials MODIFY COLUMN stock VARCHAR(32) NOT NULL")
    except mysql.connector.Error:
        pass
    cur.execute("""
        ALTER TABLE financials
        ADD PRIMARY KEY (stock, statement_type, metric, date)
    """)

def quarter_from_date(d: datetime) -> int:
    return (d.month - 1)//3 + 1

def normalize_financials(symbol, info_json):
    out = []
    info = (info_json or {}).get("info", {}) or {}
    stockcurrency = info.get("currency")
    financialcurrency = info.get("financialCurrency") or info.get("financialcurrency")
    yf_name = info.get("longName") or info.get("shortName") or info.get("displayName")

    folder_map = {"cashflow": "CF", "incomestatement": "IS", "balancesheet": "BS"}

    for folder, stype in folder_map.items():
        block = (info_json or {}).get(folder) or {}
        for freq in ("yearly", "quarterly"):
            freq_block = block.get(freq) or {}
            for dt_str, metrics in freq_block.items():
                # keys look like "2024-12-31 00:00:00"
                try:
                    d = datetime.strptime(dt_str[:10], "%Y-%m-%d")
                except Exception:
                    d = datetime.fromisoformat(dt_str.split()[0])
                cal_year = d.year
                period = 4 if freq == "yearly" else quarter_from_date(d)

                for metric, val in (metrics or {}).items():
                    if not metric:
                        continue
                    if isinstance(val, (dict, list)):
                        continue
                    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
                        val = None
                    out.append((
                        symbol, yf_name, stype, metric, stockcurrency,
                        financialcurrency, cal_year, period, val, d.date()
                    ))
    return out

def describe_columns(cur, table):
    cur.execute(f"DESCRIBE `{table}`")
    return [r[0] for r in cur.fetchall()]

def find_symbol_and_json_columns(cur):
    cols = describe_columns(cur, "yahoo_financials")
    # symbol column
    sym_col = None
    for c in ("stock", "symbol", "ticker", "SYMBOL", "TICKER"):
        if c in cols:
            sym_col = c
            break
    if not sym_col:
        raise RuntimeError("Could not find a symbol column in yahoo_financials (looked for stock/symbol/ticker).")

    # json column
    json_col = None
    for c in ("json", "payload", "data", "JSON", "PAYLOAD", "DATA"):
        if c in cols:
            json_col = c
            break
    if not json_col:
        raise RuntimeError("Could not find a JSON column in yahoo_financials (looked for json/payload/data).")

    return sym_col, json_col

def get_first_symbol(cur, sym_col):
    cur.execute(f"SELECT `{sym_col}` FROM yahoo_financials ORDER BY `{sym_col}` ASC LIMIT 1")
    row = cur.fetchone()
    return row[0] if row else None

def load_one_row(cur, sym_col, json_col, symbol):
    # Fetch *raw* column; don't use JSON_EXTRACT so it works whether it's JSON or TEXT
    cur.execute(
        f"SELECT `{sym_col}`, `{json_col}` FROM yahoo_financials WHERE `{sym_col}` = %s LIMIT 1",
        (symbol,)
    )
    return cur.fetchone()

def upsert_financials(cur, rows):
    if not rows:
        return 0
    insert_sql = """
      INSERT INTO financials
        (stock, yf_name, statement_type, metric, stockcurrency,
         financialcurrency, calendar_year, period, value, date)
      VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
      ON DUPLICATE KEY UPDATE
        yf_name=VALUES(yf_name),
        stockcurrency=VALUES(stockcurrency),
        financialcurrency=VALUES(financialcurrency),
        calendar_year=VALUES(calendar_year),
        period=VALUES(period),
        value=VALUES(value)
    """
    cur.executemany(insert_sql, rows)
    return len(rows)

def verify(cur, symbol, limit=20):
    cur.execute("""
      SELECT stock, yf_name, statement_type, metric, stockcurrency,
             financialcurrency, calendar_year, period, value, date
      FROM financials
      WHERE stock = %s
      ORDER BY date DESC, statement_type, metric
      LIMIT %s
    """, (symbol, limit))
    return cur.fetchall()

def main():
    conn = connect()
    try:
        cur = conn.cursor()
        # Ensure/repair financials schema
        ensure_financials_table(cur)
        conn.commit()

        # Detect columns on yahoo_financials
        sym_col, json_col = find_symbol_and_json_columns(cur)

        # First symbol only
        symbol = get_first_symbol(cur, sym_col)
        if not symbol:
            print("No symbols found in yahoo_financials.")
            return
        print(f"Processing first symbol from yahoo_financials: {symbol}")

        row = load_one_row(cur, sym_col, json_col, symbol)
        if not row:
            print(f"No JSON row found in yahoo_financials for {symbol}.")
            return

        stock, j = row
        obj = j
        # MySQL may return JSON column as dict, str, bytes:
        if isinstance(j, (bytes, bytearray)):
            j = j.decode("utf-8", errors="replace")
        if isinstance(j, str):
            try:
                obj = json.loads(j)
            except Exception:
                try:
                    obj = json.loads(j.strip('"').encode('utf-8').decode('unicode_escape'))
                except Exception:
                    obj = {}
        if not isinstance(obj, dict):
            try:
                obj = json.loads(obj)
            except Exception:
                obj = {}

        rows = normalize_financials(stock, obj)
        inserted = upsert_financials(cur, rows)
        conn.commit()
        print(f"Upserted {inserted} financial row(s) for {symbol}.")

        sample = verify(cur, symbol, limit=20)
        print(f"\nSample rows for {symbol}:")
        for r in sample:
            print(r)

    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()

if __name__ == "__main__":
    main()
'''