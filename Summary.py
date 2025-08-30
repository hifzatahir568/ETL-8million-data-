#!/usr/bin/env python3
import os, json, sys, math, traceback, re
from datetime import datetime
import mysql.connector

# ---- DB config (updated: default DB_NAME is yahoo_financials)
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_NAME = os.getenv("DB_NAME", "marketdata")
DB_USER = os.getenv("DB_USER", "root")
DB_PASS = os.getenv("DB_PASS", "Dolby1127@@")

# Tunables
COMMIT_EVERY_SYMBOLS = int(os.getenv("COMMIT_EVERY_SYMBOLS", "20"))
SOURCE_TABLE = os.getenv("SOURCE_TABLE", "yahoo_financials")   # your source table

SCHEMA_CREATE_SUMMARY = """
CREATE TABLE IF NOT EXISTS summary (
  stock          VARCHAR(32)  NOT NULL,
  yf_name        VARCHAR(255),
  long_summary   MEDIUMTEXT,
  sector         VARCHAR(128),
  industry       VARCHAR(128),
  website        VARCHAR(255),
  employees      INT,
  city           VARCHAR(128),
  state          VARCHAR(128),
  country        VARCHAR(128),
  currency       VARCHAR(16),
  founded_year   INT,
  former_name    VARCHAR(255),
  updated_at     DATETIME,
  PRIMARY KEY (stock)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

def connect():
    return mysql.connector.connect(
        host=DB_HOST, port=DB_PORT, database=DB_NAME,
        user=DB_USER, password=DB_PASS
    )

def ensure_summary_table(cur):
    cur.execute(SCHEMA_CREATE_SUMMARY)

def describe_columns(cur, table):
    cur.execute(f"DESCRIBE `{table}`")
    return [r[0] for r in cur.fetchall()]

def find_symbol_and_json_columns(cur, table):
    cols = describe_columns(cur, table)
    # symbol column (optional; if missing we fall back to info.symbol)
    sym_col = None
    for c in ("stock", "symbol", "ticker", "SYMBOL", "TICKER"):
        if c in cols:
            sym_col = c
            break
    # json/payload column
    json_col = None
    for c in ("json", "payload", "data", "info", "INFO", "JSON", "PAYLOAD", "DATA", "yf_info", "YF_INFO"):
        if c in cols:
            json_col = c
            break
    if not json_col:
        raise RuntimeError(f"Could not find a JSON column in {table} (looked for json/payload/data/info).")
    return sym_col, json_col

def get_all_symbols(cur, table, sym_col):
    if sym_col:
        cur.execute(f"SELECT DISTINCT `{sym_col}` FROM `{table}` ORDER BY `{sym_col}` ASC")
        return [r[0] for r in cur.fetchall()]
    # If no symbol column, iterate by offset (OK for small tables)
    cur.execute(f"SELECT COUNT(*) FROM `{table}`")
    n = cur.fetchone()[0]
    return list(range(n))  # dummy indices for progress

def load_one_row(cur, table, sym_col, json_col, symbol_or_index):
    if sym_col:
        cur.execute(
            f"SELECT `{sym_col}`, `{json_col}` FROM `{table}` WHERE `{sym_col}` = %s LIMIT 1",
            (symbol_or_index,)
        )
        return cur.fetchone()
    # Fallback by offset
    cur.execute(f"SELECT `{json_col}` FROM `{table}` LIMIT %s,1", (symbol_or_index,))
    r = cur.fetchone()
    if not r:
        return None
    return (None, r[0])

def parse_json_value(j):
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

_WS = re.compile(r"\s+")
def clean_text(s):
    if not s:
        return None
    s = _WS.sub(" ", s.strip())
    return s[:200000] if len(s) > 200000 else s

# best-effort enrichers from paragraph
_RE_FOUNDED = re.compile(r"\bfounded in (\d{4})\b", re.IGNORECASE)
_RE_FORMER  = re.compile(r"\bformerly known as ([^.,;]+)", re.IGNORECASE)
_RE_HQ      = re.compile(r"\bheadquartered in ([^.]+?)(?:\.|$)", re.IGNORECASE)

def extract_from_summary(summary):
    if not summary:
        return None, None, None, None, None
    founded = None
    m = _RE_FOUNDED.search(summary)
    if m:
        try:
            founded = int(m.group(1))
        except Exception:
            founded = None
    former = None
    m = _RE_FORMER.search(summary)
    if m:
        former = clean_text(m.group(1))
    city = state = country = None
    m = _RE_HQ.search(summary)
    if m:
        loc = m.group(1).strip()
        parts = [p.strip() for p in loc.split(",")]
        if len(parts) == 1:
            city = parts[0]
        elif len(parts) == 2:
            city, state = parts
        elif len(parts) >= 3:
            city, state, country = parts[0], parts[1], ", ".join(parts[2:])
    return founded, former, city, state, country

def normalize_summary(symbol_hint, root_obj):
    obj = root_obj or {}
    info = obj.get("info") or obj

    # symbol: prefer table symbol; fallback to info.symbol
    stock = symbol_hint or info.get("symbol") or info.get("ticker")

    yf_name = (
        info.get("longName") or info.get("shortName") or
        info.get("displayName") or info.get("name")
    )
    long_summary = (
        info.get("longBusinessSummary") or
        obj.get("summary") or
        (obj.get("profile") or {}).get("longBusinessSummary")
    )

    sector   = info.get("sector") or info.get("sectorDisp")
    industry = info.get("industry") or info.get("industryDisp")
    website  = info.get("website") or info.get("irWebsite")
    employees = info.get("fullTimeEmployees")
    city     = info.get("city")
    state    = info.get("state") or info.get("province")
    country  = info.get("country")
    currency = info.get("currency") or info.get("financialCurrency")

    # clean
    yf_name = clean_text(yf_name)
    long_summary = clean_text(long_summary)
    sector = clean_text(sector)
    industry = clean_text(industry)
    website = clean_text(website)
    city = clean_text(city)
    state = clean_text(state)
    country = clean_text(country)
    currency = clean_text(currency)

    # employees → int
    if isinstance(employees, str):
        try:
            employees = int(re.sub(r"[^\d]", "", employees))
        except Exception:
            employees = None
    elif isinstance(employees, (int, float)):
        if isinstance(employees, float) and (math.isnan(employees) or math.isinf(employees)):
            employees = None
        else:
            employees = int(employees)
    else:
        employees = None

    # enrich from paragraph (non-destructive)
    f_year, former, s_city, s_state, s_country = extract_from_summary(long_summary)
    city = city or s_city
    state = state or s_state
    country = country or s_country

    return {
        "stock": stock,
        "yf_name": yf_name,
        "long_summary": long_summary,
        "sector": sector,
        "industry": industry,
        "website": website,
        "employees": employees,
        "city": city,
        "state": state,
        "country": country,
        "currency": currency,
        "founded_year": f_year,
        "former_name": former,
        "updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
    }

def upsert_summary(cur, rows):
    if not rows:
        return 0
    insert_sql = """
      INSERT INTO summary
        (stock, yf_name, long_summary, sector, industry, website,
         employees, city, state, country, currency, founded_year, former_name, updated_at)
      VALUES
        (%(stock)s, %(yf_name)s, %(long_summary)s, %(sector)s, %(industry)s, %(website)s,
         %(employees)s, %(city)s, %(state)s, %(country)s, %(currency)s, %(founded_year)s, %(former_name)s, %(updated_at)s)
      ON DUPLICATE KEY UPDATE
        yf_name=VALUES(yf_name),
        long_summary=VALUES(long_summary),
        sector=VALUES(sector),
        industry=VALUES(industry),
        website=VALUES(website),
        employees=VALUES(employees),
        city=VALUES(city),
        state=VALUES(state),
        country=VALUES(country),
        currency=VALUES(currency),
        founded_year=VALUES(founded_year),
        former_name=VALUES(former_name),
        updated_at=VALUES(updated_at)
    """
    cur.executemany(insert_sql, rows)
    return len(rows)

def verify(cur, symbol):
    cur.execute("""
      SELECT stock, yf_name, LEFT(long_summary, 200) AS summary_snippet,
             sector, industry, website, employees, city, state, country,
             currency, founded_year, former_name, updated_at
      FROM summary
      WHERE stock = %s
    """, (symbol,))
    return cur.fetchall()

def main():
    conn = connect()
    try:
        cur = conn.cursor()
        ensure_summary_table(cur)
        conn.commit()

        sym_col, json_col = find_symbol_and_json_columns(cur, SOURCE_TABLE)
        symbols = get_all_symbols(cur, SOURCE_TABLE, sym_col)
        if not symbols:
            print(f"No rows found in {SOURCE_TABLE}.")
            return

        total_rows = 0
        errors = 0
        n = len(symbols)
        print(f"Processing {n:,} row(s) from {SOURCE_TABLE}...")

        for i, s in enumerate(symbols, 1):
            try:
                row = load_one_row(cur, SOURCE_TABLE, sym_col, json_col, s)
                if not row:
                    print(f"[WARN] No row found for index/symbol {s}")
                    continue

                symbol_hint, j = row
                obj = parse_json_value(j)
                rec = normalize_summary(symbol_hint, obj)
                if not rec.get("stock"):
                    print(f"[WARN] Skipping row with missing stock (index/symbol={s})")
                    continue

                upsert_summary(cur, [rec])
                total_rows += 1

                if i % COMMIT_EVERY_SYMBOLS == 0:
                    conn.commit()
                    print(f"…processed {i:,}/{n:,} (rows upserted so far: {total_rows:,})")

            except Exception as e:
                errors += 1
                print(f"[WARN] Failed on index/symbol {s}: {e}", file=sys.stderr)
                traceback.print_exc(limit=2)

        conn.commit()
        print("\n=== Done ===")
        print(f"Rows processed: {n:,}")
        print(f"Rows with errors: {errors:,}")
        print(f"Total summary rows upserted: {total_rows:,}")

        # Optional: sample verify for last symbol if we have a symbol string
        if sym_col and symbols:
            last_symbol = symbols[-1]
            sample = verify(cur, last_symbol)
            print(f"\nSample row for {last_symbol}:")
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
