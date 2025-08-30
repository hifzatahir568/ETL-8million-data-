# main.py — scrape Symbol, Company, Industry, Market Cap (handles All/500 rows & lazy rendering)

import csv
import time
from typing import Dict, List, Optional

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

URL = "https://stockanalysis.com/stocks/"
HEADLESS = True
WAIT_SEC = 25
PAUSE_AFTER_CLICK = 0.8          # slightly slower to improve reliability on heavy pages
MAX_PAGES = 13                   # set to 13 if you want ~6k at 500/page; increase if needed
TARGET_RECORDS = None            # or set an int to stop after N total rows

def build_driver(headless: bool = True) -> webdriver.Chrome:
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-software-rasterizer")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1600,1400")
    opts.add_argument("--log-level=3")
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=opts)

def dismiss_popups(driver) -> None:
    try:
        ActionChains(driver).send_keys(Keys.ESCAPE).perform()
        time.sleep(0.15)
        xpaths = [
            "//button[contains(., 'Accept')]",
            "//button[contains(., 'I agree')]",
            "//button[contains(., 'Got it')]",
            "//div[@role='dialog']//button[contains(., 'Accept')]",
            "//button[contains(@aria-label,'Close')]",
            "//div[@role='dialog']//button[contains(@aria-label,'Close')]",
            "//div[contains(@class,'fixed') and contains(@class,'left-0') and contains(@class,'top-0')]//button",
        ]
        for xp in xpaths:
            for b in driver.find_elements(By.XPATH, xp):
                if b.is_displayed() and b.is_enabled():
                    try:
                        b.click()
                        time.sleep(0.15)
                    except Exception:
                        pass
        WebDriverWait(driver, 2).until(
            EC.invisibility_of_element_located(
                (By.XPATH, "//div[contains(@class,'fixed') and contains(@class,'left-0') and contains(@class,'top-0')]")
            )
        )
    except Exception:
        pass

def wait_for_table(driver) -> None:
    WebDriverWait(driver, WAIT_SEC).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "table#main-table thead th"))
    )
    WebDriverWait(driver, WAIT_SEC).until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR, "table#main-table tbody tr"))
    )

def ensure_rows_per_page(driver) -> None:
    """Prefer the largest page size every time we land on a page."""
    try:
        rows_toggle = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located(
                (By.XPATH, "//button[contains(., 'Rows') or contains(., 'Rows:') or contains(., 'Rows ')]")
            )
        )
        driver.execute_script("arguments[0].scrollIntoView({block:'center'})", rows_toggle)
        WebDriverWait(driver, 5).until(EC.element_to_be_clickable(rows_toggle)).click()
        time.sleep(0.25)
        for label in ("All Rows", "All", "500 Rows", "500", "250 Rows", "250", "200 Rows", "200", "100 Rows", "100"):
            try:
                opt = driver.find_element(By.XPATH, f"//div//button[normalize-space()='{label}']")
                if opt.is_displayed():
                    opt.click()
                    time.sleep(0.35)
                    return
            except Exception:
                continue
    except Exception:
        pass

def first_symbol_in_table(table) -> Optional[str]:
    try:
        first_row = table.find_element(By.CSS_SELECTOR, "tbody tr:first-child")
        tds = first_row.find_elements(By.CSS_SELECTOR, "td")
        if not tds:
            return None
        return tds[0].text.strip() or None
    except Exception:
        return None

def wait_for_page_advance(driver, prev_first_symbol, timeout=WAIT_SEC) -> bool:
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: first_symbol_in_table(d.find_element(By.CSS_SELECTOR, "table#main-table")) != prev_first_symbol
        )
        return True
    except Exception:
        return False

def load_all_rows_rendered(driver, max_loops: int = 50, pause: float = 0.25) -> int:
    """
    Scroll the table's scrollable container (or window) to force lazy rows to render.
    Returns the final number of <tr> found.
    """
    table = driver.find_element(By.CSS_SELECTOR, "table#main-table")

    # Try to find the scrollable ancestor (Tailwind often uses overflow-auto/scroll classes)
    container = None
    try:
        container = table.find_element(
            By.XPATH,
            "ancestor::div[contains(@class,'overflow') or contains(@class,'overflow-auto') or contains(@class,'overflow-scroll')][1]"
        )
    except Exception:
        container = None

    def row_count():
        return len(table.find_elements(By.CSS_SELECTOR, "tbody tr"))

    last = -1
    loops = 0

    # Initial small pause to let the page-size change settle
    time.sleep(0.2)

    while loops < max_loops:
        current = row_count()
        if current == last:
            # no new rows since last scroll
            break
        last = current

        if container:
            driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight;", container)
        else:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

        time.sleep(pause)
        loops += 1

    # One final settle & recount
    time.sleep(0.15)
    return row_count()

def locate_target_columns(table) -> Dict[str, int]:
    ths = table.find_elements(By.CSS_SELECTOR, "thead th")
    col_index: Dict[str, int] = {}
    for i, th in enumerate(ths):
        th_id = (th.get_attribute("id") or "").strip()
        if th_id == "s":
            col_index["symbol"] = i
        elif th_id == "n":
            col_index["company"] = i
        elif th_id == "industry":
            col_index["industry"] = i
        elif th_id == "marketCap":
            col_index["market_cap"] = i
    expected = {"symbol", "company", "industry", "market_cap"}
    if expected - set(col_index.keys()):
        raise RuntimeError("Could not find all target columns.")
    return col_index

def read_rows_for_targets(table, col_index: Dict[str, int]) -> List[Dict[str, Optional[str]]]:
    rows_out: List[Dict[str, Optional[str]]] = []
    body_rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
    for tr in body_rows:
        tds = tr.find_elements(By.CSS_SELECTOR, "td")
        if not tds:
            continue

        def grab(idx: int) -> Optional[str]:
            if idx >= len(tds):
                return None
            td = tds[idx]
            try:
                a = td.find_element(By.TAG_NAME, "a")
                txt = a.text.strip() or td.text.strip()
            except Exception:
                txt = td.text.strip()
            return txt or None

        row = {
            "symbol": grab(col_index["symbol"]),
            "company": grab(col_index["company"]),
            "industry": grab(col_index["industry"]),
            "market_cap": grab(col_index["market_cap"]),
        }
        if any(row.values()):
            rows_out.append(row)
    return rows_out

def find_next_button(driver):
    try:
        return WebDriverWait(driver, 3).until(
            EC.presence_of_element_located((
                By.XPATH,
                "//button[contains(@class,'controls-btn')][.//span[normalize-space()='Next']]"
            ))
        )
    except Exception:
        return None

def safe_click(driver, el) -> bool:
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'})", el)
        WebDriverWait(driver, 5).until(EC.element_to_be_clickable(el)).click()
        return True
    except Exception:
        dismiss_popups(driver)
        try:
            driver.execute_script("arguments[0].click();", el)
            return True
        except Exception:
            return False

def scrape_all() -> pd.DataFrame:
    driver = build_driver(HEADLESS)
    all_rows: List[Dict[str, Optional[str]]] = []
    try:
        driver.get(URL)
        dismiss_popups(driver)
        wait_for_table(driver)

        # Page 1: pick largest page size and force-render all rows
        ensure_rows_per_page(driver)
        rendered = load_all_rows_rendered(driver)
        print(f"[page 1] rows rendered: {rendered}", flush=True)
        dismiss_popups(driver)

        page = 1
        while page <= MAX_PAGES:
            table = driver.find_element(By.CSS_SELECTOR, "table#main-table")

            # Ensure rows are fully rendered before reading
            rendered = load_all_rows_rendered(driver)
            print(f"[page {page}] rows rendered: {rendered}", flush=True)

            col_index = locate_target_columns(table)
            page_rows = read_rows_for_targets(table, col_index)
            print(f"[page {page}] rows extracted: {len(page_rows)}", flush=True)
            all_rows.extend(page_rows)

            # Stop if we hit TARGET_RECORDS (if set)
            if TARGET_RECORDS and len(all_rows) >= TARGET_RECORDS:
                print(f"[info] Collected {len(all_rows)} rows; stopping.")
                break

            next_btn = find_next_button(driver)
            if not next_btn:
                print("[done] last page (no Next button).")
                break

            # Track first row so we can verify the page actually changes
            prev_first = first_symbol_in_table(table)

            # Try to click Next
            tries = 0
            clicked = False
            while tries < 3 and not clicked:
                clicked = safe_click(driver, next_btn)
                if not clicked:
                    tries += 1
                    time.sleep(0.3)

            if not clicked:
                print("[stop] Could not click Next.")
                break

            time.sleep(PAUSE_AFTER_CLICK)

            # Verify page advance (first row must change)
            if not wait_for_page_advance(driver, prev_first):
                print("[warn] page didn’t advance; retrying click once…")
                if safe_click(driver, next_btn):
                    time.sleep(PAUSE_AFTER_CLICK)
                    if not wait_for_page_advance(driver, prev_first):
                        print("[stop] table did not advance; stopping to avoid duplicates.")
                        break
                else:
                    print("[stop] Could not re-click Next.")
                    break

            # New page is up: re-apply largest rows/page (some sites reset it), then force-render
            ensure_rows_per_page(driver)
            load_all_rows_rendered(driver)

            page += 1

        df = pd.DataFrame(all_rows, columns=["symbol", "company", "industry", "market_cap"])
        # Deduplicate by symbol (prevents double-counts if a page was accidentally read twice)
        df = df.dropna(subset=["symbol"]).drop_duplicates(subset=["symbol"]).reset_index(drop=True)
        return df
    finally:
        driver.quit()

def save_csv(df: pd.DataFrame, path: str = "stocks_full.csv") -> None:
    df.to_csv(path, index=False, quoting=csv.QUOTE_MINIMAL)

if __name__ == "__main__":
    df = scrape_all()
    print(df.head(10))
    print(f"\nTotal rows: {len(df):,}")
    save_csv(df)
    print("\nSaved -> stocks_full.csv")
