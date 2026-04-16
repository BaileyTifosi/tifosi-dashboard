#!/usr/bin/env python3
"""
generate_dashboard.py  —  Tifosi Marketing Dashboard Generator

Stores data at daily granularity. Supports any date range selection.

Usage:
    py -3.12 generate_dashboard.py              # refresh last 14 days + regenerate HTML
    py -3.12 generate_dashboard.py --full       # fetch full history from scratch (~40 min first run)
    py -3.12 generate_dashboard.py --cache-only # regenerate HTML from cached data only
    py -3.12 generate_dashboard.py --months 24  # extend history (use with --full)
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import tempfile
import time
import datetime as dt
from calendar import monthrange
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo


# ============================================================
# CONFIG  —  credentials read from environment variables.
#            Locally: create a .env file next to this script (never commit it).
#            In CI:   set GitHub Secrets and map them in the workflow env: block.
# ============================================================

# Load .env file for local development (ignored in CI where secrets come from env)
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
except ImportError:
    pass

def _e(key: str) -> str:
    """Return env var value, or empty string with a warning if missing."""
    val = os.environ.get(key, "")
    if not val:
        print(f"[config] {key} not set — that data source will show N/A")
    return val

# --- Shopify ---
SHOPIFY_SHOP_DOMAIN        = "tifosioptics.myshopify.com"
SHOPIFY_ADMIN_ACCESS_TOKEN = _e("SHOPIFY_ADMIN_ACCESS_TOKEN")
SHOPIFY_API_VERSION        = "2024-10"

# --- Meta / Facebook Ads ---
META_APP_ID        = _e("META_APP_ID")
META_APP_SECRET    = _e("META_APP_SECRET")
META_ACCESS_TOKEN  = _e("META_ACCESS_TOKEN")
META_AD_ACCOUNT_ID = _e("META_AD_ACCOUNT_ID")

# --- Google Ads ---
GOOGLEADS_DEVELOPER_TOKEN     = _e("GOOGLEADS_DEVELOPER_TOKEN")
GOOGLEADS_OAUTH_CLIENT_ID     = _e("GOOGLEADS_OAUTH_CLIENT_ID")
GOOGLEADS_OAUTH_CLIENT_SECRET = _e("GOOGLEADS_OAUTH_CLIENT_SECRET")
GOOGLEADS_REFRESH_TOKEN       = _e("GOOGLEADS_REFRESH_TOKEN")
GOOGLEADS_LOGIN_CUSTOMER_ID   = _e("GOOGLEADS_LOGIN_CUSTOMER_ID")
GOOGLEADS_CUSTOMER_ID         = _e("GOOGLEADS_CUSTOMER_ID")

# --- GA4 ---
GA4_PROPERTY_ID = _e("GA4_PROPERTY_ID")
_ga4_json_str   = os.environ.get("GA4_SERVICE_ACCOUNT_JSON", "")
GA4_SERVICE_ACCOUNT_INFO: dict = {}  # used directly via from_service_account_info()
if _ga4_json_str:
    # GitHub Secrets sometimes expands \n sequences to real newlines inside the
    # private key, making the JSON invalid. Try raw first, then re-escape newlines.
    _ga4_info = None
    for _attempt in [_ga4_json_str, _ga4_json_str.replace('\r\n', '\\n').replace('\n', '\\n')]:
        try:
            _ga4_info = json.loads(_attempt)
            break
        except json.JSONDecodeError:
            continue
    if _ga4_info:
        if "private_key" in _ga4_info:
            import re as _re
            pk = _ga4_info["private_key"]
            # Fix corrupted PEM headers (space between words replaced by newline)
            pk = _re.sub(r'-----BEGIN\s+PRIVATE\s+KEY-----', '-----BEGIN PRIVATE KEY-----', pk)
            pk = _re.sub(r'-----END\s+PRIVATE\s+KEY-----', '-----END PRIVATE KEY-----', pk)
            # Ensure line separators are real newlines (not literal \n)
            pk = pk.replace('\\n', '\n')
            _ga4_info["private_key"] = pk
        GA4_SERVICE_ACCOUNT_INFO = _ga4_info
    else:
        print("[config] GA4_SERVICE_ACCOUNT_JSON could not be parsed — GA4 will show N/A")
else:
    print("[config] GA4_SERVICE_ACCOUNT_JSON not set — GA4 will show N/A")

# --- Microsoft Ads ---
MSADS_DEVELOPER_TOKEN    = _e("MSADS_DEVELOPER_TOKEN")
MSADS_CLIENT_ID          = _e("MSADS_CLIENT_ID")
MSADS_CLIENT_SECRET      = _e("MSADS_CLIENT_SECRET")
MSADS_TENANT_ID          = _e("MSADS_TENANT_ID")
MSADS_ACCOUNT_ID         = _e("MSADS_ACCOUNT_ID")
MSADS_REDIRECT_URI       = "http://localhost:8000/callback"
MSADS_REPORT_TIMEZONE    = "PacificTimeUSCanadaTijuana"
MSADS_REFRESH_TOKEN_VALUE = os.environ.get("MSADS_REFRESH_TOKEN", "")

# --- Klaviyo ---
KLAVIYO_PRIVATE_API_KEY   = _e("KLAVIYO_PRIVATE_API_KEY")
KLAVIYO_API_REVISION      = "2026-01-15"
KLAVIYO_CONVERSION_METRIC = "RHuXhk"

# --- Reddit Ads (API currently returning 404 — pending Reddit support) ---
REDDIT_CLIENT_ID     = os.environ.get("REDDIT_CLIENT_ID", "")
REDDIT_CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET", "")
REDDIT_AD_ACCOUNT_ID = os.environ.get("REDDIT_AD_ACCOUNT_ID", "")
REDDIT_BUSINESS_ID   = os.environ.get("REDDIT_BUSINESS_ID", "")
REDDIT_REDIRECT_URI  = "https://www.tifosioptics.com"
REDDIT_REFRESH_TOKEN_VALUE = os.environ.get("REDDIT_REFRESH_TOKEN", "")

# --- Amazon Ads API ---
AMAZON_ADS_CLIENT_ID     = _e("AMAZON_ADS_CLIENT_ID")
AMAZON_ADS_CLIENT_SECRET = _e("AMAZON_ADS_CLIENT_SECRET")
AMAZON_ADS_REFRESH_TOKEN = _e("AMAZON_ADS_REFRESH_TOKEN")
AMAZON_ADS_PROFILE_ID    = _e("AMAZON_ADS_PROFILE_ID")

# --- Amazon Seller Central (SP-API) ---
AMAZON_SP_REFRESH_TOKEN    = _e("AMAZON_SP_REFRESH_TOKEN")
AMAZON_SP_LWA_APP_ID       = _e("AMAZON_SP_LWA_APP_ID")
AMAZON_SP_LWA_CLIENT_SECRET= _e("AMAZON_SP_LWA_CLIENT_SECRET")
AMAZON_SP_AWS_ACCESS_KEY   = _e("AMAZON_SP_AWS_ACCESS_KEY")
AMAZON_SP_AWS_SECRET_KEY   = _e("AMAZON_SP_AWS_SECRET_KEY")
AMAZON_SP_CREDENTIALS: dict = {
    "refresh_token":    AMAZON_SP_REFRESH_TOKEN,
    "lwa_app_id":       AMAZON_SP_LWA_APP_ID,
    "lwa_client_secret":AMAZON_SP_LWA_CLIENT_SECRET,
    "aws_access_key":   AMAZON_SP_AWS_ACCESS_KEY,
    "aws_secret_key":   AMAZON_SP_AWS_SECRET_KEY,
}
AMAZON_SP_MARKETPLACE_ID = "ATVPDKIKX0DER"  # US
AMAZON_SP_TIMEZONE       = "US/Pacific"

# ============================================================
# OUTPUT PATHS / SETTINGS
# ============================================================
_HERE       = os.path.dirname(os.path.abspath(__file__))
OUTPUT_HTML = os.path.join(_HERE, "index.html")
CACHE_FILE  = os.path.join(_HERE, ".cache", "dashboard_cache.json")
os.makedirs(os.path.join(_HERE, ".cache"), exist_ok=True)
SHOPIFY_TIMEZONE = "America/New_York"   # Must match your Shopify store timezone
HISTORY_MONTHS = 24   # default history depth
REFRESH_DAYS   = 14   # days re-fetched on daily run (attribution window)


# ============================================================
# DATE HELPERS
# ============================================================

def yesterday() -> dt.date:
    return dt.date.today() - dt.timedelta(days=1)

def history_start(n_months: int) -> dt.date:
    """First day of the month n_months ago."""
    today = dt.date.today()
    d = dt.date(today.year, today.month, 1)
    for _ in range(n_months):
        d = (d - dt.timedelta(days=1)).replace(day=1)
    return d

def date_range_list(start: dt.date, end: dt.date) -> List[str]:
    return [(start + dt.timedelta(days=i)).isoformat() for i in range((end - start).days + 1)]

def iter_months(start: dt.date, end: dt.date):
    """Yield (month_start, month_end) clamped to [start, end]."""
    cur = dt.date(start.year, start.month, 1)
    while cur <= end:
        _, last = monthrange(cur.year, cur.month)
        yield cur, min(dt.date(cur.year, cur.month, last), end)
        cur = dt.date(cur.year, cur.month, last) + dt.timedelta(days=1)


# ============================================================
# CACHE
# ============================================================

_AMZ_HISTORY_FILE = os.path.join(_HERE, "amazon_ads_history.json")

def load_cache() -> Optional[Dict]:
    if not os.path.exists(CACHE_FILE):
        return None
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            cache = json.load(f)
    except Exception as e:
        print(f"[cache] Could not load: {e}")
        return None
    # Merge amazon_ads_history.json into cache for any missing/zeroed historical fields.
    # Covers: amz_*, ga4_users/sessions, bing_*, reddit_*, and monthly_ga4.
    # This ensures backfilled data survives actions/cache overwrites on every run.
    if os.path.exists(_AMZ_HISTORY_FILE):
        try:
            with open(_AMZ_HISTORY_FILE, "r", encoding="utf-8") as f:
                history = json.load(f)
            # Merge daily fields.
            # For dates older than 30 days: history file is authoritative — always
            # overwrites the cache. This ensures manual corrections survive
            # actions/cache restores which may bring back stale/wrong values.
            # For recent dates: only fill gaps (let live API data take precedence).
            daily = cache.setdefault("daily", {})
            daily_merged = 0
            cutoff = (dt.date.today() - dt.timedelta(days=15)).isoformat()
            for date, fields in history.get("daily", {}).items():
                if date not in daily:
                    daily[date] = {}
                for k, v in fields.items():
                    if v is None:
                        continue
                    if date < cutoff:
                        # Historical data: history file wins
                        if daily[date].get(k) != v:
                            daily[date][k] = v
                            daily_merged += 1
                    else:
                        # Recent data: only fill gaps
                        if not daily[date].get(k):
                            daily[date][k] = v
                            daily_merged += 1
            # Merge monthly_ga4
            ga4_merged = 0
            existing_ga4 = cache.setdefault("monthly_ga4", {})
            for ym, v in history.get("monthly_ga4", {}).items():
                if ym not in existing_ga4:
                    existing_ga4[ym] = v
                    ga4_merged += 1
            if daily_merged or ga4_merged:
                print(f"[cache] History merge: {daily_merged} daily fields, {ga4_merged} monthly_ga4 months")
        except Exception as e:
            print(f"[cache] Could not merge history: {e}")
    return cache

def save_cache(cache: Dict) -> None:
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, default=str)
    print(f"[cache] Saved to {CACHE_FILE}")


# ============================================================
# SHOPIFY
# ============================================================

_SHOPIFY_PRODUCTS_QUERY = """
query($cursor: String, $query: String) {
  orders(first: 250, after: $cursor, query: $query) {
    edges {
      node {
        createdAt
        lineItems(first: 50) {
          edges {
            node {
              title
              quantity
              discountedTotalSet { shopMoney { amount } }
            }
          }
        }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

_SHOPIFY_ORDERS_QUERY = """
query($cursor: String, $query: String) {
  orders(first: 250, after: $cursor, query: $query) {
    edges {
      node {
        createdAt
        subtotalPriceSet { shopMoney { amount } }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

_SHOPIFY_GQL_URL          = f"https://{SHOPIFY_SHOP_DOMAIN}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"
_SHOPIFY_GQL_URL_UNSTABLE = f"https://{SHOPIFY_SHOP_DOMAIN}/admin/api/unstable/graphql.json"
_SHOPIFY_HEADERS = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": SHOPIFY_ADMIN_ACCESS_TOKEN,
}

def _shopify_gql(query: str, variables: Dict) -> Dict:
    import requests
    for attempt in range(3):
        r = requests.post(_SHOPIFY_GQL_URL, headers=_SHOPIFY_HEADERS,
                          json={"query": query, "variables": variables}, timeout=60)
        if r.status_code == 429:
            time.sleep(float(r.headers.get("Retry-After", "3")))
            continue
        r.raise_for_status()
        d = r.json()
        errors = d.get("errors") or []
        if any((e.get("extensions") or {}).get("code") == "THROTTLED" for e in errors):
            time.sleep(3)
            continue
        if errors:
            raise RuntimeError(f"[Shopify GQL] {errors}")
        return d.get("data", {})
    raise RuntimeError("[Shopify GQL] Exceeded retry limit")

def _utc_to_store_date(created_at: str) -> str:
    """Convert Shopify UTC createdAt (e.g. '2026-02-28T23:45:00Z') to store-local date string."""
    from zoneinfo import ZoneInfo
    ts = dt.datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    return ts.astimezone(ZoneInfo(SHOPIFY_TIMEZONE)).date().isoformat()

_SHOPIFY_QL_QUERY = """
query ShopifyQL($q: String!) {
  shopifyqlQuery(query: $q) {
    tableData {
      columns { name dataType }
      rows
    }
    parseErrors
  }
}
"""

def _fetch_shopify_analytics(start: dt.date, end: dt.date) -> Dict[str, Dict]:
    """Query ShopifyQL via unstable API — exact same data as Shopify dashboard."""
    import requests as _req
    shopify_q = (
        f"FROM sales SHOW day, gross_sales, discounts, orders, average_order_value "
        f"SINCE {start.isoformat()} UNTIL {end.isoformat()} "
        f"GROUP BY day ORDER BY day ASC"
    )
    r = _req.post(_SHOPIFY_GQL_URL_UNSTABLE, headers=_SHOPIFY_HEADERS,
                  json={"query": _SHOPIFY_QL_QUERY, "variables": {"q": shopify_q}}, timeout=60)
    r.raise_for_status()
    sq = r.json().get("data", {}).get("shopifyqlQuery", {})
    if sq.get("parseErrors"):
        raise RuntimeError(f"[Shopify ShopifyQL] {sq['parseErrors']}")
    table   = sq.get("tableData", {})
    daily: Dict[str, Dict] = {}
    for row in table.get("rows", []):
        if isinstance(row, str):
            row = json.loads(row)
        ds        = str(row.get("day", ""))[:10]
        if not ds: continue
        gross     = float(row.get("gross_sales", 0) or 0)
        discounts = float(row.get("discounts",   0) or 0)
        orders    = int(float(row.get("orders",  0) or 0))
        aov_raw   = row.get("average_order_value")
        aov       = float(aov_raw) if aov_raw else None
        daily[ds] = {"net_sales": round(gross - abs(discounts), 2), "orders": orders, "aov": aov}
    return daily

def _fetch_shopify_month(ms: dt.date, me: dt.date) -> Dict[str, Dict]:
    return _fetch_shopify_analytics(ms, me)

def fetch_shopify(start: dt.date, end: dt.date, batch_by_month: bool = False) -> Dict[str, Dict]:
    if not batch_by_month:
        days = (end - start).days + 1
        print(f"[Shopify] Fetching {days} days ({start} to {end})...")
        result = _fetch_shopify_month(start, end)
        print(f"  Done: {sum(v['orders'] for v in result.values()):,} orders across {len(result)} days.")
        return result
    months = list(iter_months(start, end))
    out: Dict[str, Dict] = {}
    print(f"[Shopify] Fetching {len(months)} months ({start} to {end}) month-by-month...")
    for i, (ms, me) in enumerate(months):
        try:
            month_data = _fetch_shopify_month(ms, me)
            orders = sum(v["orders"] for v in month_data.values())
            sales  = sum(v["net_sales"] for v in month_data.values())
            print(f"  [{i+1}/{len(months)}] {ms.strftime('%Y-%m')}: ${sales:,.0f}, {orders:,} orders, {len(month_data)} days")
            out.update(month_data)
        except Exception as e:
            print(f"  [Shopify] Error {ms.strftime('%Y-%m')}: {e}")
    return out


def _fetch_shopify_products_range(start: dt.date, end: dt.date) -> Dict[str, Dict]:
    gql_filter = (
        f"created_at:>={start.isoformat()}T00:00:00 "
        f"created_at:<={end.isoformat()}T23:59:59 "
        f"(status:open OR status:closed)"
    )
    daily: Dict[str, Dict] = {}
    cursor = None
    while True:
        data   = _shopify_gql(_SHOPIFY_PRODUCTS_QUERY, {"cursor": cursor, "query": gql_filter})
        result = data["orders"]
        for edge in result["edges"]:
            node = edge["node"]
            ds   = _utc_to_store_date(node["createdAt"])
            if ds not in daily:
                daily[ds] = {}
            for li_edge in node["lineItems"]["edges"]:
                li      = li_edge["node"]
                title   = li["title"]
                qty     = li["quantity"]
                revenue = float(li["discountedTotalSet"]["shopMoney"]["amount"])
                if title not in daily[ds]:
                    daily[ds][title] = {"orders": 0, "revenue": 0.0}
                daily[ds][title]["orders"]  += qty
                daily[ds][title]["revenue"] += revenue
        pi = result["pageInfo"]
        if not pi["hasNextPage"]:
            break
        cursor = pi["endCursor"]
    for ds in daily:
        for title in daily[ds]:
            daily[ds][title]["revenue"] = round(daily[ds][title]["revenue"], 2)
    return daily


def fetch_shopify_products(start: dt.date, end: dt.date, batch_by_month: bool = False) -> Dict[str, Dict]:
    """Fetch line-item product data. Returns {YYYY-MM-DD: {product_title: {orders, revenue}}}"""
    if not batch_by_month:
        print(f"[Shopify Products] Fetching line items ({start} to {end})...")
        result = _fetch_shopify_products_range(start, end)
        unique = len({t for day in result.values() for t in day})
        print(f"  Done: {unique} unique products across {len(result)} days.")
        return result
    # Month-by-month for large historical ranges
    months = list(iter_months(start, end))
    out: Dict[str, Dict] = {}
    print(f"[Shopify Products] Fetching line items for {len(months)} months ({start} to {end})...")
    print("  NOTE: This is slow on first run (~1-2 min/month).")
    for i, (ms, me) in enumerate(months):
        try:
            month_data = _fetch_shopify_products_range(ms, me)
            unique = len({t for day in month_data.values() for t in day})
            revenue = sum(p["revenue"] for day in month_data.values() for p in day.values())
            print(f"  [{i+1}/{len(months)}] {ms.strftime('%Y-%m')}: ${revenue:,.0f}, {unique} products, {len(month_data)} days")
            out.update(month_data)
        except Exception as e:
            print(f"  [Shopify Products] Error {ms.strftime('%Y-%m')}: {e}")
    return out


# ============================================================
# META ADS
# ============================================================

def fetch_meta(start: dt.date, end: dt.date) -> Dict[str, Dict]:
    """Returns {YYYY-MM-DD: {spend, clicks, impressions, reach, purchase_value}}"""
    if not META_ACCESS_TOKEN or not META_AD_ACCOUNT_ID:
        print("[Meta] Credentials not set — skipping.")
        return {}
    print(f"[Meta] Fetching daily ads insights ({start} to {end})...")
    try:
        from facebook_business.api import FacebookAdsApi
        from facebook_business.adobjects.adaccount import AdAccount
    except ImportError:
        raise RuntimeError("Install: pip install facebook-business")

    FacebookAdsApi.init(META_APP_ID, META_APP_SECRET, META_ACCESS_TOKEN)
    account = AdAccount(META_AD_ACCOUNT_ID)

    params = {
        "time_range": {"since": start.isoformat(), "until": end.isoformat()},
        "time_increment": "1",
        "level": "account",
    }
    fields = ["spend", "clicks", "impressions", "reach", "purchase_roas", "date_start"]
    rows   = list(account.get_insights(fields=fields, params=params))
    out: Dict[str, Dict] = {}

    for row in rows:
        ds = (row.get("date_start") or "")[:10]
        if not ds:
            continue
        spend       = float(row.get("spend",       0) or 0)
        clicks      = int(float(row.get("clicks",  0) or 0))
        impressions = int(float(row.get("impressions", 0) or 0))
        reach       = int(float(row.get("reach",   0) or 0))
        roas = 0.0
        for item in (row.get("purchase_roas") or []):
            if not isinstance(item, dict):
                continue
            if item.get("action_type","") in ("omni_purchase","offsite_conversion.fb_pixel_purchase","purchase"):
                try:
                    roas = float(item.get("value", 0) or 0)
                    break
                except Exception:
                    pass
        out[ds] = {
            "spend":          round(spend, 2),
            "clicks":         clicks,
            "impressions":    impressions,
            "reach":          reach,
            "purchase_value": round(roas * spend, 2),
        }

    print(f"  Got {len(out)} days of data.")
    return out


def fetch_meta_monthly_reach(start: dt.date, end: dt.date) -> Dict[str, int]:
    """Returns {YYYY-MM: reach} — deduplicated unique reach per calendar month.
    Daily reach numbers can't be summed (same person counted every day).
    This fetches the period-level reach for each full calendar month."""
    print(f"[Meta] Fetching monthly reach ({start} to {end})...")
    try:
        from facebook_business.api import FacebookAdsApi
        from facebook_business.adobjects.adaccount import AdAccount
    except ImportError:
        return {}

    FacebookAdsApi.init(META_APP_ID, META_APP_SECRET, META_ACCESS_TOKEN)
    account = AdAccount(META_AD_ACCOUNT_ID)
    out: Dict[str, int] = {}

    for ms, me in iter_months(start, end):
        try:
            params = {
                "time_range": {"since": ms.isoformat(), "until": me.isoformat()},
                "level": "account",
            }
            rows = list(account.get_insights(fields=["reach"], params=params))
            if rows:
                out[ms.strftime("%Y-%m")] = int(float(rows[0].get("reach", 0) or 0))
        except Exception as e:
            print(f"  [Meta] Monthly reach error {ms.strftime('%Y-%m')}: {e}")

    print(f"  Got monthly reach for {len(out)} months.")
    return out


# ============================================================
# GOOGLE ADS
# ============================================================

def fetch_google(start: dt.date, end: dt.date) -> Dict[str, Dict]:
    """Returns {YYYY-MM-DD: {spend, clicks, impressions, conversions_value}}"""
    if not GOOGLEADS_REFRESH_TOKEN or not GOOGLEADS_DEVELOPER_TOKEN:
        print("[Google Ads] Credentials not set — skipping.")
        return {}
    print(f"[Google Ads] Fetching daily campaign data ({start} to {end})...")
    try:
        from google.ads.googleads.client import GoogleAdsClient
    except ImportError:
        print("[Google Ads] SDK not installed — skipping.")
        return {}

    try:
        client = GoogleAdsClient.load_from_dict({
            "developer_token":   GOOGLEADS_DEVELOPER_TOKEN,
            "client_id":         GOOGLEADS_OAUTH_CLIENT_ID,
            "client_secret":     GOOGLEADS_OAUTH_CLIENT_SECRET,
            "refresh_token":     GOOGLEADS_REFRESH_TOKEN,
            "login_customer_id": GOOGLEADS_LOGIN_CUSTOMER_ID,
            "use_proto_plus":    True,
        })
        service = client.get_service("GoogleAdsService")
        query = f"""
            SELECT segments.date, metrics.cost_micros, metrics.clicks,
                   metrics.impressions, metrics.conversions_value
            FROM customer
            WHERE segments.date BETWEEN '{start.isoformat()}' AND '{end.isoformat()}'
        """
        daily: Dict[str, Dict] = {}
        for row in service.search(customer_id=GOOGLEADS_CUSTOMER_ID, query=query):
            ds = row.segments.date
            if ds not in daily:
                daily[ds] = {"cost_micros": 0, "clicks": 0, "impressions": 0, "conv_value": 0.0}
            daily[ds]["cost_micros"]  += int(row.metrics.cost_micros or 0)
            daily[ds]["clicks"]       += int(row.metrics.clicks or 0)
            daily[ds]["impressions"]  += int(row.metrics.impressions or 0)
            daily[ds]["conv_value"]   += float(row.metrics.conversions_value or 0)

        out: Dict[str, Dict] = {}
        for ds, v in daily.items():
            spend = v["cost_micros"] / 1_000_000.0
            out[ds] = {
                "spend":             round(spend, 2),
                "clicks":            v["clicks"],
                "impressions":       v["impressions"],
                "conversions_value": round(v["conv_value"], 2),
            }
        print(f"  Got {len(out)} days of data.")
        return out
    except Exception as e:
        print(f"[Google Ads] Error — skipping: {e}")
        return {}


# ============================================================
# GA4
# ============================================================

def fetch_ga4(start: dt.date, end: dt.date) -> Dict[str, Dict]:
    """Returns {YYYY-MM-DD: {users, sessions}}"""
    if not GA4_SERVICE_ACCOUNT_INFO or not GA4_PROPERTY_ID:
        print("[GA4] Credentials not set — skipping.")
        return {}
    print(f"[GA4] Fetching daily users + sessions ({start} to {end})...")
    try:
        from google.analytics.data_v1beta import BetaAnalyticsDataClient
        from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
        from google.oauth2 import service_account
    except ImportError:
        print("[GA4] SDK not installed — skipping.")
        return {}

    try:
        creds = service_account.Credentials.from_service_account_info(
            GA4_SERVICE_ACCOUNT_INFO,
            scopes=["https://www.googleapis.com/auth/analytics.readonly"],
        )
        ga4_client = BetaAnalyticsDataClient(credentials=creds)
        req = RunReportRequest(
            property=f"properties/{GA4_PROPERTY_ID}",
            dimensions=[Dimension(name="date")],
            metrics=[Metric(name="totalUsers"), Metric(name="sessions")],
            date_ranges=[DateRange(start_date=start.isoformat(), end_date=end.isoformat())],
        )
        resp = ga4_client.run_report(req)
        out: Dict[str, Dict] = {}
        for row in resp.rows:
            compact = row.dimension_values[0].value   # "YYYYMMDD"
            ds = f"{compact[:4]}-{compact[4:6]}-{compact[6:8]}"
            out[ds] = {
                "users":    int(row.metric_values[0].value or 0),
                "sessions": int(row.metric_values[1].value or 0),
            }
        print(f"  Got {len(out)} days of data.")
        return out
    except Exception as e:
        print(f"[GA4] Error — skipping: {e}")
        return {}


def fetch_ga4_monthly(start: dt.date, end: dt.date) -> Dict[str, Dict]:
    """Returns {YYYY-MM: {users, sessions}} — period-level totals per calendar month.
    Fetching month-by-month avoids GA4 sampling that occurs on long date ranges,
    and gives exact numbers matching what Whatagraph pulls."""
    if not GA4_SERVICE_ACCOUNT_INFO or not GA4_PROPERTY_ID:
        print("[GA4] Credentials not set — skipping monthly.")
        return {}
    print(f"[GA4] Fetching monthly period totals ({start} to {end})...")
    try:
        from google.analytics.data_v1beta import BetaAnalyticsDataClient
        from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
        from google.oauth2 import service_account
    except ImportError:
        return {}

    try:
        creds = service_account.Credentials.from_service_account_info(
            GA4_SERVICE_ACCOUNT_INFO,
            scopes=["https://www.googleapis.com/auth/analytics.readonly"],
        )
    except Exception as e:
        print(f"[GA4] Could not load service account — skipping monthly: {e}")
        return {}

    ga4_client = BetaAnalyticsDataClient(credentials=creds)
    out: Dict[str, Dict] = {}

    for ms, me in iter_months(start, end):
        try:
            req = RunReportRequest(
                property=f"properties/{GA4_PROPERTY_ID}",
                dimensions=[],
                metrics=[Metric(name="totalUsers"), Metric(name="sessions")],
                date_ranges=[DateRange(start_date=ms.isoformat(), end_date=me.isoformat())],
            )
            resp = ga4_client.run_report(req)
            if resp.rows:
                row = resp.rows[0]
                out[ms.strftime("%Y-%m")] = {
                    "users":    int(row.metric_values[0].value or 0),
                    "sessions": int(row.metric_values[1].value or 0),
                }
        except Exception as e:
            print(f"  [GA4] Monthly error {ms.strftime('%Y-%m')}: {e}")

    print(f"  Got monthly totals for {len(out)} months.")
    return out


# ============================================================
# MICROSOFT ADS
# ============================================================

def fetch_msads(start: dt.date, end: dt.date) -> Dict[str, Dict]:
    """Returns {YYYY-MM-DD: {spend, clicks, conversions_value}} — Daily aggregation.
    Uses direct SOAP/HTTP calls to bypass platform-specific SDK issues."""
    print(f"[Microsoft Ads] Fetching daily spend + clicks ({start} to {end})...")

    refresh_token = MSADS_REFRESH_TOKEN_VALUE
    if not refresh_token or not MSADS_CLIENT_ID or not MSADS_DEVELOPER_TOKEN:
        print("  Microsoft Ads credentials not set. Skipping.")
        return {}

    import requests as _req
    import io, zipfile

    out: Dict[str, Dict] = {}
    try:
        # Step 1: Get OAuth access token
        tok_r = _req.post(
            f"https://login.microsoftonline.com/{MSADS_TENANT_ID}/oauth2/v2.0/token",
            data={
                "grant_type":    "refresh_token",
                "client_id":     MSADS_CLIENT_ID,
                "client_secret": MSADS_CLIENT_SECRET,
                "refresh_token": refresh_token,
                "scope":         "https://ads.microsoft.com/msads.manage offline_access",
            },
            timeout=30,
        )
        tok_r.raise_for_status()
        access_token = tok_r.json()["access_token"]

        account_id = int(float(MSADS_ACCOUNT_ID))

        soap_headers = {
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction":   "SubmitGenerateReport",
        }
        soap_body = f"""<?xml version="1.0" encoding="utf-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                  xmlns:v13="https://bingads.microsoft.com/Reporting/v13"
                  xmlns:arr="http://schemas.microsoft.com/2003/10/Serialization/Arrays"
                  xmlns:i="http://www.w3.org/2001/XMLSchema-instance">
  <soapenv:Header>
    <v13:AuthenticationToken>{access_token}</v13:AuthenticationToken>
    <v13:DeveloperToken>{MSADS_DEVELOPER_TOKEN}</v13:DeveloperToken>
    <v13:AccountId>{account_id}</v13:AccountId>
  </soapenv:Header>
  <soapenv:Body>
    <v13:SubmitGenerateReportRequest>
      <v13:ReportRequest i:type="v13:CampaignPerformanceReportRequest">
        <v13:ExcludeColumnHeaders>false</v13:ExcludeColumnHeaders>
        <v13:ExcludeReportFooter>true</v13:ExcludeReportFooter>
        <v13:ExcludeReportHeader>true</v13:ExcludeReportHeader>
        <v13:Format>Csv</v13:Format>
        <v13:ReportName>Dashboard Daily</v13:ReportName>
        <v13:ReturnOnlyCompleteData>false</v13:ReturnOnlyCompleteData>
        <v13:Aggregation>Daily</v13:Aggregation>
        <v13:Columns>
          <v13:CampaignPerformanceReportColumn>TimePeriod</v13:CampaignPerformanceReportColumn>
          <v13:CampaignPerformanceReportColumn>Spend</v13:CampaignPerformanceReportColumn>
          <v13:CampaignPerformanceReportColumn>Clicks</v13:CampaignPerformanceReportColumn>
          <v13:CampaignPerformanceReportColumn>Revenue</v13:CampaignPerformanceReportColumn>
        </v13:Columns>
        <v13:Scope>
          <v13:AccountIds>
            <arr:long>{account_id}</arr:long>
          </v13:AccountIds>
        </v13:Scope>
        <v13:Time>
          <v13:CustomDateRangeEnd>
            <v13:Day>{end.day}</v13:Day>
            <v13:Month>{end.month}</v13:Month>
            <v13:Year>{end.year}</v13:Year>
          </v13:CustomDateRangeEnd>
          <v13:CustomDateRangeStart>
            <v13:Day>{start.day}</v13:Day>
            <v13:Month>{start.month}</v13:Month>
            <v13:Year>{start.year}</v13:Year>
          </v13:CustomDateRangeStart>
          <v13:ReportTimeZone>{MSADS_REPORT_TIMEZONE}</v13:ReportTimeZone>
        </v13:Time>
      </v13:ReportRequest>
    </v13:SubmitGenerateReportRequest>
  </soapenv:Body>
</soapenv:Envelope>"""

        # Step 2: Submit report
        endpoint = "https://reporting.api.bingads.microsoft.com/Api/Advertiser/Reporting/v13/ReportingService.svc"
        r = _req.post(endpoint, headers=soap_headers, data=soap_body.encode("utf-8"), timeout=60)
        r.raise_for_status()

        import xml.etree.ElementTree as ET
        ns = {
            "s":   "http://schemas.xmlsoap.org/soap/envelope/",
            "v13": "https://bingads.microsoft.com/Reporting/v13",
        }
        root = ET.fromstring(r.text)
        req_id_el = root.find(".//v13:ReportRequestId", ns)
        if req_id_el is None:
            print(f"  [Microsoft Ads] No request ID in response: {r.text[:300]}")
            return {}
        report_request_id = req_id_el.text.strip()

        # Step 3: Poll for completion
        poll_soap = f"""<?xml version="1.0" encoding="utf-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                  xmlns:v13="https://bingads.microsoft.com/Reporting/v13">
  <soapenv:Header>
    <v13:AuthenticationToken>{access_token}</v13:AuthenticationToken>
    <v13:DeveloperToken>{MSADS_DEVELOPER_TOKEN}</v13:DeveloperToken>
    <v13:AccountId>{account_id}</v13:AccountId>
  </soapenv:Header>
  <soapenv:Body>
    <v13:PollGenerateReportRequest>
      <v13:ReportRequestId>{report_request_id}</v13:ReportRequestId>
    </v13:PollGenerateReportRequest>
  </soapenv:Body>
</soapenv:Envelope>"""

        download_url = None
        for _ in range(60):
            time.sleep(5)
            poll_r = _req.post(
                endpoint,
                headers={**soap_headers, "SOAPAction": "PollGenerateReport"},
                data=poll_soap.encode("utf-8"),
                timeout=60,
            )
            poll_r.raise_for_status()
            poll_root = ET.fromstring(poll_r.text)
            status_el  = poll_root.find(".//v13:ReportRequestStatus/v13:Status", ns)
            url_el     = poll_root.find(".//v13:ReportRequestStatus/v13:ReportDownloadUrl", ns)
            status = status_el.text.strip() if status_el is not None else ""
            if status == "Success" and url_el is not None:
                download_url = url_el.text.strip()
                break
            if status in ("Error", "Failed"):
                print(f"  [Microsoft Ads] Report generation failed: {status}")
                return {}

        if not download_url:
            print("  [Microsoft Ads] Timed out waiting for report.")
            return {}

        # Step 4: Download and parse
        dl = _req.get(download_url, timeout=120)
        dl.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(dl.content)) as zf:
            csv_name = zf.namelist()[0]
            with zf.open(csv_name) as csv_file:
                reader = csv.DictReader(io.TextIOWrapper(csv_file, encoding="utf-8-sig"))
                daily: Dict[str, Dict] = {}
                for row in reader:
                    tp = (row.get("TimePeriod") or "").strip()
                    if not tp or len(tp) < 10:
                        continue
                    ds = tp[:10]
                    if ds not in daily:
                        daily[ds] = {"spend": 0.0, "clicks": 0, "conversions_value": 0.0}
                    try:
                        daily[ds]["spend"]             += float(row.get("Spend",   0) or 0)
                        daily[ds]["clicks"]            += int(float(row.get("Clicks", 0) or 0))
                        daily[ds]["conversions_value"] += float(row.get("Revenue", 0) or 0)
                    except Exception:
                        pass
                for ds, v in daily.items():
                    out[ds] = {
                        "spend":             round(v["spend"], 2),
                        "clicks":            v["clicks"],
                        "conversions_value": round(v["conversions_value"], 2),
                    }
    except Exception as e:
        print(f"  [Microsoft Ads] Error: {e}")

    print(f"  Got {len(out)} days of data.")
    return out


# ============================================================
# REDDIT ADS (optional)
# ============================================================

def fetch_reddit(start: dt.date, end: dt.date) -> Dict[str, Dict]:
    if not REDDIT_CLIENT_ID or not REDDIT_CLIENT_SECRET or not REDDIT_AD_ACCOUNT_ID:
        print("[Reddit] Not configured — showing N/A.")
        return {}
    refresh_token = REDDIT_REFRESH_TOKEN_VALUE
    if not refresh_token:
        print("[Reddit] REDDIT_REFRESH_TOKEN not set. Skipping.")
        return {}
    print(f"[Reddit] Fetching daily ads data ({start} to {end})...")
    import requests
    tok_r = requests.post(
        "https://www.reddit.com/api/v1/access_token",
        auth=(REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET),
        headers={"User-Agent": "TifosiDashboard/1.0"},
        data={"grant_type": "refresh_token", "refresh_token": refresh_token},
        timeout=15,
    )
    tok_r.raise_for_status()
    access_token = tok_r.json()["access_token"]
    headers = {
        "Authorization": f"Bearer {access_token}",
        "User-Agent": "TifosiDashboard/1.0",
        "Content-Type": "application/json",
    }

    def _report(starts_at: str, ends_at: str) -> Optional[dict]:
        r = requests.post(
            f"https://ads-api.reddit.com/api/v3/ad_accounts/{REDDIT_AD_ACCOUNT_ID}/reports",
            headers=headers,
            json={"data": {"starts_at": starts_at, "ends_at": ends_at,
                           "fields": ["SPEND", "CLICKS",
                                      "CONVERSION_PURCHASE_TOTAL_VALUE"]}},
            timeout=30,
        )
        r.raise_for_status()
        metrics = (r.json().get("data") or {}).get("metrics") or []
        if not metrics:
            return None
        if len(metrics) > 1:
            # Aggregate in case API returns per-campaign rows
            return {k: sum(int(m.get(k, 0) or 0) for m in metrics) for k in metrics[0]}
        return metrics[0]

    # Reddit API ends_at is INCLUSIVE — querying "day X to day X+1" returns
    # data for both X and X+1, causing double-counting.  Using ends_at = starts_at
    # (same-day query) returns exactly one day of data at face value (/1e6).
    out: Dict[str, Dict] = {}
    current = start
    while current <= end:
        try:
            ts = current.isoformat() + "T00:00:00Z"
            m = _report(ts, ts)
            if m and int(m.get("spend", 0) or 0) > 0:
                out[current.isoformat()] = {
                    "spend":          round(int(m.get("spend", 0) or 0) / 1_000_000, 2),
                    "clicks":         int(m.get("clicks", 0) or 0),
                    "purchase_value": round(int(m.get("conversion_purchase_total_value", 0) or 0) / 100, 2),
                }
        except Exception as e:
            print(f"  [Reddit] Error {current}: {e}")
        current += dt.timedelta(days=1)
        time.sleep(0.3)

    print(f"  Got {len(out)} days of Reddit data.")
    return out


# ============================================================
# KLAVIYO
# ============================================================

def fetch_klaviyo_monthly(start: dt.date, end: dt.date) -> Dict[str, Dict]:
    """Returns {YYYY-MM: {emails_sent, revenue}} — period totals per calendar month."""
    if not KLAVIYO_PRIVATE_API_KEY:
        print("[Klaviyo] Credentials not set — skipping monthly.")
        return {}
    print(f"[Klaviyo] Fetching monthly email stats ({start} to {end})...")
    import requests as _req
    from datetime import timezone

    session = _req.Session()
    session.headers.update({
        "Authorization": f"Klaviyo-API-Key {KLAVIYO_PRIVATE_API_KEY}",
        "accept": "application/vnd.api+json",
        "revision": KLAVIYO_API_REVISION,
    })

    def _klaviyo_post(url: str, payload: Dict) -> List:
        results = []
        next_url = url
        while next_url:
            for attempt in range(8):
                r = session.post(next_url, json=payload, timeout=60)
                if r.status_code == 429:
                    wait = float(r.headers.get("Retry-After", 30))
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                break
            data  = r.json()
            obj   = data.get("data") or {}
            attrs = obj.get("attributes") or {}
            results.extend(attrs.get("results") or [])
            links = data.get("links") or obj.get("links") or {}
            next_url = links.get("next")
        return results

    def _sum_stat(rows: List, stat: str, channel: str = None) -> float:
        total = 0.0
        for row in rows:
            if channel:
                ch = (row.get("groupings") or {}).get("send_channel", "").lower()
                if ch != channel:
                    continue
            v = (row.get("statistics") or {}).get(stat)
            try:
                total += float(v or 0)
            except Exception:
                pass
        return total

    out: Dict[str, Dict] = {}
    stats = ["delivered", "conversion_value"]

    for ms, me in iter_months(start, end):
        ym = ms.strftime("%Y-%m")
        # Build UTC timeframe for this calendar month (Eastern time)
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("America/New_York")
        ms_dt = dt.datetime(ms.year, ms.month, ms.day, tzinfo=tz)
        me_next = dt.datetime(me.year, me.month, me.day, tzinfo=tz) + dt.timedelta(days=1)
        tf = {
            "start": ms_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end":   me_next.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        payload_c = {"data": {"type": "campaign-values-report", "attributes": {"statistics": stats, "timeframe": tf, "conversion_metric_id": KLAVIYO_CONVERSION_METRIC}}}
        payload_f = {"data": {"type": "flow-values-report",     "attributes": {"statistics": stats, "timeframe": tf, "conversion_metric_id": KLAVIYO_CONVERSION_METRIC}}}
        try:
            camp_rows = _klaviyo_post("https://a.klaviyo.com/api/campaign-values-reports", payload_c)
            flow_rows = _klaviyo_post("https://a.klaviyo.com/api/flow-values-reports",     payload_f)
            emails_sent = int(_sum_stat(camp_rows, "delivered", "email") + _sum_stat(flow_rows, "delivered", "email"))
            revenue     = round(_sum_stat(camp_rows, "conversion_value") + _sum_stat(flow_rows, "conversion_value"), 2)
            out[ym] = {"emails_sent": emails_sent, "revenue": revenue}
            print(f"  {ym}: {emails_sent:,} emails, ${revenue:,.2f} revenue")
        except Exception as e:
            print(f"  [Klaviyo] Error {ym}: {e}")
        time.sleep(2)

    print(f"  Got Klaviyo data for {len(out)} months.")
    return out


def fetch_klaviyo_daily(start: dt.date, end: dt.date) -> Dict[str, Dict]:
    """Returns {YYYY-MM-DD: {emails_sent}} using metric-aggregates API."""
    if not KLAVIYO_PRIVATE_API_KEY:
        print("[Klaviyo] Credentials not set — skipping daily.")
        return {}
    print(f"[Klaviyo] Fetching daily email counts ({start} to {end})...")
    import requests as _req

    session = _req.Session()
    session.headers.update({
        "Authorization": f"Klaviyo-API-Key {KLAVIYO_PRIVATE_API_KEY}",
        "accept": "application/vnd.api+json",
        "content-type": "application/vnd.api+json",
        "revision": KLAVIYO_API_REVISION,
    })

    end_next = end + dt.timedelta(days=1)
    payload = {
        "data": {
            "type": "metric-aggregate",
            "attributes": {
                "metric_id": "SSbNrk",  # Received Email
                "interval": "day",
                "measurements": ["count"],
                "filter": [
                    f"greater-or-equal(datetime,{start.isoformat()})",
                    f"less-than(datetime,{end_next.isoformat()})",
                ],
                "timezone": "America/New_York",
            }
        }
    }

    for attempt in range(8):
        r = session.post("https://a.klaviyo.com/api/metric-aggregates/", json=payload, timeout=60)
        if r.status_code == 429:
            time.sleep(float(r.headers.get("Retry-After", 30)))
            continue
        r.raise_for_status()
        break

    attrs  = r.json()["data"]["attributes"]
    dates  = attrs.get("dates", [])
    counts = attrs.get("data", [{}])[0].get("measurements", {}).get("count", [])

    out: Dict[str, Dict] = {}
    for date_str, count in zip(dates, counts):
        date_obj = dt.datetime.fromisoformat(date_str).date()
        if start <= date_obj <= end:
            out[date_obj.isoformat()] = {"emails_sent": int(count)}

    print(f"  Got {len(out)} days of Klaviyo email data.")
    return out


# ============================================================
# AMAZON ADS
# ============================================================

_AMZ_REPORT_CONFIGS = [
    ("SP", "SPONSORED_PRODUCTS", "spCampaigns",
     ["date", "impressions", "cost", "purchases7d",     "sales7d"],
     "purchases7d",     "sales7d"),
    ("SB", "SPONSORED_BRANDS",   "sbCampaigns",
     ["date", "impressions", "cost", "purchases",       "sales"],
     "purchases",       "sales"),
    ("SD", "SPONSORED_DISPLAY",  "sdCampaigns",
     ["date", "impressions", "cost", "purchasesClicks", "salesClicks"],
     "purchasesClicks", "salesClicks"),
]


def _amz_access_token() -> str:
    import requests as _req
    tok_r = _req.post(
        "https://api.amazon.com/auth/o2/token",
        data={
            "grant_type":    "refresh_token",
            "refresh_token": AMAZON_ADS_REFRESH_TOKEN,
            "client_id":     AMAZON_ADS_CLIENT_ID,
            "client_secret": AMAZON_ADS_CLIENT_SECRET,
        },
        timeout=30,
    )
    tok_r.raise_for_status()
    return tok_r.json()["access_token"]


def create_amazon_ads_reports(start: dt.date, end: dt.date) -> List[Dict]:
    """Create Amazon Ads async reports for SP/SB/SD and return a list of pending report specs.
    Does NOT wait for completion — call download_amazon_ads_reports() on the next run.
    Returns list of {reportId, label, purch_col, sales_col, start, end}."""
    if not AMAZON_ADS_CLIENT_ID or not AMAZON_ADS_REFRESH_TOKEN:
        return []
    try:
        import requests as _req, re as _re
        access_token = _amz_access_token()
        api_base = "https://advertising-api.amazon.com"
        base_headers = {
            "Authorization":                   f"Bearer {access_token}",
            "Amazon-Advertising-API-ClientId": AMAZON_ADS_CLIENT_ID,
            "Amazon-Advertising-API-Scope":    AMAZON_ADS_PROFILE_ID,
        }
        create_headers = {**base_headers,
                          "Content-Type": "application/vnd.createasyncreportrequest.v3+json"}
        pending = []
        for label, ad_product, report_type_id, columns, purch_col, sales_col in _AMZ_REPORT_CONFIGS:
            body = {
                "name": f"Dashboard {label} {start} {end}",
                "startDate": start.isoformat(),
                "endDate":   end.isoformat(),
                "configuration": {
                    "adProduct":    ad_product,
                    "groupBy":      ["campaign"],
                    "columns":      columns,
                    "reportTypeId": report_type_id,
                    "timeUnit":     "DAILY",
                    "format":       "GZIP_JSON",
                },
            }
            r = _req.post(f"{api_base}/reporting/reports", headers=create_headers,
                          json=body, timeout=30)
            if r.status_code == 425:
                m = _re.search(
                    r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}'
                    r'-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}', r.text)
                if m:
                    pending.append({"reportId": m.group(0), "label": label,
                                    "purch_col": purch_col, "sales_col": sales_col,
                                    "start": start.isoformat(), "end": end.isoformat()})
                    print(f"  [Amazon Ads] {label} reusing existing report {m.group(0)}")
                continue
            if r.status_code == 429:
                time.sleep(30)
                r = _req.post(f"{api_base}/reporting/reports", headers=create_headers,
                              json=body, timeout=30)
            if not r.ok:
                print(f"  [Amazon Ads] {label} create failed {r.status_code}: {r.text[:200]}")
                continue
            report_id = r.json().get("reportId")
            if report_id:
                pending.append({"reportId": report_id, "label": label,
                                "purch_col": purch_col, "sales_col": sales_col,
                                "start": start.isoformat(), "end": end.isoformat()})
                print(f"  [Amazon Ads] {label} report created: {report_id}")
        return pending
    except Exception as e:
        print(f"[Amazon Ads] create_reports error: {e}")
        return []


def download_amazon_ads_reports(pending: List[Dict]) -> Dict[str, Dict]:
    """Download previously created Amazon Ads reports that are now COMPLETED.
    pending: list of specs from create_amazon_ads_reports().
    Returns {YYYY-MM-DD: {amz_ad_spend, amz_ad_sales, amz_impressions, amz_purchases}}
    and a list of still-pending specs (not yet completed)."""
    if not pending or not AMAZON_ADS_CLIENT_ID or not AMAZON_ADS_REFRESH_TOKEN:
        return {}, pending
    try:
        import requests as _req, gzip as _gzip, io as _io
        access_token = _amz_access_token()
        api_base = "https://advertising-api.amazon.com"
        base_headers = {
            "Authorization":                   f"Bearer {access_token}",
            "Amazon-Advertising-API-ClientId": AMAZON_ADS_CLIENT_ID,
            "Amazon-Advertising-API-Scope":    AMAZON_ADS_PROFILE_ID,
        }
        daily: Dict[str, Dict] = {}
        still_pending = []
        for spec in pending:
            report_id = spec["reportId"]
            sr = _req.get(f"{api_base}/reporting/reports/{report_id}",
                          headers=base_headers, timeout=30)
            sr.raise_for_status()
            data = sr.json()
            status = data.get("status", "").upper()
            if status == "COMPLETED":
                url        = data["url"]
                purch_col  = spec.get("purch_col")
                sales_col  = spec.get("sales_col")
                start_iso  = spec.get("start", "")
                end_iso    = spec.get("end", "")
                dl = _req.get(url, timeout=120)
                dl.raise_for_status()
                try:
                    with _gzip.GzipFile(fileobj=_io.BytesIO(dl.content)) as gz:
                        content = gz.read().decode("utf-8")
                except Exception:
                    content = dl.content.decode("utf-8")
                try:
                    rows = json.loads(content)
                except json.JSONDecodeError:
                    rows = [json.loads(line) for line in content.splitlines() if line.strip()]
                if not isinstance(rows, list):
                    continue
                for row in rows:
                    ds = str(row.get("date", ""))[:10]
                    if not ds or (start_iso and ds < start_iso) or (end_iso and ds > end_iso):
                        continue
                    if ds not in daily:
                        daily[ds] = {"amz_ad_spend": 0.0, "amz_ad_sales": 0.0,
                                     "amz_impressions": 0, "amz_purchases": 0}
                    daily[ds]["amz_ad_spend"]    += float(row.get("cost", 0) or 0)
                    daily[ds]["amz_ad_sales"]    += float(row.get(sales_col, 0) or 0) if sales_col else 0
                    daily[ds]["amz_impressions"] += int(row.get("impressions", 0) or 0)
                    daily[ds]["amz_purchases"]   += int(row.get(purch_col, 0) or 0) if purch_col else 0
                print(f"  [Amazon Ads] {spec['label']} downloaded ({report_id})")
            elif status in ("FAILURE", "FAILED", "CANCELLED"):
                print(f"  [Amazon Ads] {spec['label']} report {status} — dropping")
            else:
                still_pending.append(spec)
                print(f"  [Amazon Ads] {spec['label']} still {status} — will retry next run")
        for d in daily.values():
            d["amz_ad_spend"] = round(d["amz_ad_spend"], 2)
            d["amz_ad_sales"] = round(d["amz_ad_sales"], 2)
        print(f"[Amazon Ads] Downloaded {len(daily)} days from {len(pending)-len(still_pending)} reports.")
        return daily, still_pending
    except Exception as e:
        print(f"[Amazon Ads] download_reports error: {e}")
        return {}, pending


def fetch_amazon_ads(start: dt.date, end: dt.date) -> Dict[str, Dict]:
    """Fetch Amazon Ads by creating reports and waiting up to 10 min (history/backfill use).
    For daily refresh, use create_amazon_ads_reports + download_amazon_ads_reports instead."""
    if not AMAZON_ADS_CLIENT_ID or not AMAZON_ADS_REFRESH_TOKEN:
        print("[Amazon Ads] Credentials not set — skipping.")
        return {}
    if (end - start).days > 90:
        print(f"[Amazon Ads] Date range {(end-start).days} days — chunking into 90-day windows...")
        merged: Dict[str, Dict] = {}
        chunk_start = start
        while chunk_start <= end:
            chunk_end = min(chunk_start + dt.timedelta(days=89), end)
            chunk_data = fetch_amazon_ads(chunk_start, chunk_end)
            merged.update(chunk_data)
            chunk_start = chunk_end + dt.timedelta(days=1)
            time.sleep(2)
        print(f"[Amazon Ads] Got {len(merged)} days total across all chunks.")
        return merged
    try:
        pending_specs = create_amazon_ads_reports(start, end)
        if not pending_specs:
            return {}
        # Poll until all complete (max 10 min)
        import requests as _req
        access_token = _amz_access_token()
        api_base = "https://advertising-api.amazon.com"
        base_headers = {
            "Authorization":                   f"Bearer {access_token}",
            "Amazon-Advertising-API-ClientId": AMAZON_ADS_CLIENT_ID,
            "Amazon-Advertising-API-Scope":    AMAZON_ADS_PROFILE_ID,
        }
        deadline = time.time() + 600
        remaining = list(pending_specs)
        ready = []
        while remaining and time.time() < deadline:
            time.sleep(10)
            still = []
            for spec in remaining:
                sr = _req.get(
                    f"{api_base}/reporting/reports/{spec['reportId']}",
                    headers=base_headers, timeout=30)
                sr.raise_for_status()
                status = sr.json().get("status", "").upper()
                if status == "COMPLETED":
                    ready.append(spec)
                elif status in ("FAILURE", "FAILED", "CANCELLED"):
                    print(f"  [Amazon Ads] {spec['label']} {status}")
                else:
                    still.append(spec)
            remaining = still
        if remaining:
            print(f"[Amazon Ads] {len(remaining)} reports still pending after timeout — no data.")
            return {}
        daily, _ = download_amazon_ads_reports(ready)
        print(f"[Amazon Ads] Got {len(daily)} days of data.")
        return daily
    except Exception as e:
        print(f"[Amazon Ads] Error — skipping: {e}")
        return {}


# ============================================================
# AMAZON SELLER CENTRAL (SP-API)
# ============================================================

def fetch_amazon_sc_monthly(start: dt.date, end: dt.date) -> Dict[str, Dict]:
    """Returns {YYYY-MM: {amz_sc_sales, amz_sc_orders, amz_sc_units}} — FBA/DTC only."""
    if not AMAZON_SP_REFRESH_TOKEN or not AMAZON_SP_LWA_APP_ID:
        print("[Amazon SC] Credentials not set — skipping.")
        return {}
    try:
        from sp_api.api import Sales
        from sp_api.base import Marketplaces, SellingApiException
        from zoneinfo import ZoneInfo
        from decimal import Decimal
    except ImportError as e:
        print(f"[Amazon SC] Missing library: {e}. Skipping.")
        return {}

    class _Val:
        def __init__(self, v: str): self.value = v
        def __str__(self): return self.value

    sales_client = Sales(credentials=AMAZON_SP_CREDENTIALS, marketplace=Marketplaces.US)
    tz = ZoneInfo(AMAZON_SP_TIMEZONE)
    out: Dict[str, Dict] = {}

    for ms, me in iter_months(start, end):
        ym = ms.strftime("%Y-%m")
        next_start = me + dt.timedelta(days=1)
        start_local = dt.datetime(ms.year, ms.month, 1, 0, 0, 0, tzinfo=tz)
        end_local   = dt.datetime(next_start.year, next_start.month, next_start.day, 0, 0, 0, tzinfo=tz)
        interval_str = f"{start_local.isoformat(timespec='seconds')}--{end_local.isoformat(timespec='seconds')}"
        print(f"[Amazon SC] Fetching {ym}...")

        payload = None
        for interval in [[interval_str], interval_str]:
            for granularity in [_Val("Total"), "Total"]:
                for fulfillment in [_Val("AFN"), "AFN"]:
                    for mk in ["marketplaceIds", "marketplace_ids"]:
                        for fk in ["fulfillmentNetwork", "fulfillment_network"]:
                            try:
                                res = sales_client.get_order_metrics(**{
                                    mk: [AMAZON_SP_MARKETPLACE_ID],
                                    "interval": interval,
                                    "granularity": granularity,
                                    fk: fulfillment,
                                })
                                payload = res.payload or {}
                                break
                            except Exception:
                                continue
                        if payload is not None: break
                    if payload is not None: break
                if payload is not None: break
            if payload is not None: break

        if payload is None:
            print(f"  {ym}: Could not fetch data.")
            continue

        rows = payload if isinstance(payload, list) else (payload.get("payload") or [])
        if not rows:
            print(f"  {ym}: No rows returned.")
            continue

        row = rows[0] if isinstance(rows, list) else rows
        total_sales = row.get("totalSales") or {}
        amount  = float(Decimal(str(total_sales.get("amount", "0") or "0")))
        orders  = int(row.get("orderCount", 0) or 0)
        units   = int(row.get("unitCount",  0) or 0)
        out[ym] = {"amz_sc_sales": amount, "amz_sc_orders": orders, "amz_sc_units": units}
        print(f"  {ym}: sales=${amount:,.2f}, orders={orders:,}, units={units:,}")

    print(f"[Amazon SC] Got data for {len(out)} months.")
    return out


def fetch_amazon_sc_daily_gross_sales(start: dt.date, end: dt.date) -> Dict[str, float]:
    """Returns {YYYY-MM-DD: gross_sales_amount} — all channels (FBA + FBM), daily granularity.
    Queries month-by-month to avoid SP-API truncation on long date ranges."""
    if not AMAZON_SP_REFRESH_TOKEN or not AMAZON_SP_LWA_APP_ID:
        print("[Amazon SC] Credentials not set — skipping daily gross sales.")
        return {}
    try:
        from sp_api.api import Sales
        from sp_api.base import Marketplaces
        from zoneinfo import ZoneInfo
        from decimal import Decimal
    except ImportError as e:
        print(f"[Amazon SC] Missing library: {e}. Skipping.")
        return {}

    class _Val:
        def __init__(self, v): self.value = v
        def __str__(self): return self.value

    sales_client = Sales(credentials=AMAZON_SP_CREDENTIALS, marketplace=Marketplaces.US)
    tz = ZoneInfo(AMAZON_SP_TIMEZONE)
    end_iso = end.isoformat()
    out: Dict[str, float] = {}

    for ms, me in iter_months(start, end):
        next_day    = me + dt.timedelta(days=1)
        start_local = dt.datetime(ms.year, ms.month, ms.day, 0, 0, 0, tzinfo=tz)
        end_local   = dt.datetime(next_day.year, next_day.month, next_day.day, 0, 0, 0, tzinfo=tz)
        ym = ms.strftime("%Y-%m")
        print(f"[Amazon SC] Fetching daily gross sales {ym}...")
        try:
            res = sales_client.get_order_metrics(
                interval=(start_local, end_local),
                granularity=_Val("Day"),
                marketplaceIds=[AMAZON_SP_MARKETPLACE_ID],
            )
            rows = res.payload or []
        except Exception as e:
            print(f"[Amazon SC] Error fetching {ym}: {e}")
            continue

        for row in rows:
            if not isinstance(row, dict):
                continue
            interval_val = row.get("interval", "")
            date_str = interval_val.split("--")[0][:10] if isinstance(interval_val, str) else ""
            if len(date_str) < 10 or date_str > end_iso:
                continue
            total_sales = row.get("totalSales") or {}
            amount = float(Decimal(str(total_sales.get("amount", "0") or "0")))
            out[date_str] = amount

    print(f"[Amazon SC] Got daily gross sales for {len(out)} days total.")
    return out


# ============================================================
# MERGE
# ============================================================

def merge_daily(
    start: dt.date, end: dt.date,
    shopify: Dict, meta: Dict, google: Dict, ga4: Dict, msads: Dict, reddit: Dict,
    amazon_ads: Optional[Dict] = None,
) -> Dict[str, Dict]:
    """
    Returns {YYYY-MM-DD: raw_metrics} for every day in [start, end].
    Derived metrics (roas, cac, aov, cvr) are NOT stored here — the JS computes
    them from aggregated sums so any date range works correctly.
    """
    reddit_configured = bool(reddit)
    out: Dict[str, Dict] = {}

    for ds in date_range_list(start, end):
        s = shopify.get(ds, {})
        m = meta.get(ds, {})
        g = google.get(ds, {})
        a = ga4.get(ds, {})
        b = msads.get(ds, {})
        r = reddit.get(ds, {})
        z = (amazon_ads or {}).get(ds)

        row: Dict = {
            "net_sales": s.get("net_sales", 0.0),
            "orders":    s.get("orders",    0),
            "aov":       s.get("aov"),
            "meta_spend":               m.get("spend",             0.0),
            "meta_clicks":              m.get("clicks",            0),
            "meta_impressions":         m.get("impressions",       0),
            "meta_reach":               m.get("reach",             0),
            "meta_purchase_value":      m.get("purchase_value",    0.0),
            "google_spend":             g.get("spend",             0.0),
            "google_clicks":            g.get("clicks",            0),
            "google_impressions":       g.get("impressions",       0),
            "google_conversions_value": g.get("conversions_value", 0.0),
            "reddit_spend":          r.get("spend",          0.0) if reddit_configured else None,
            "reddit_clicks":         r.get("clicks",         0)   if reddit_configured else None,
            "reddit_purchase_value": r.get("purchase_value", 0.0) if reddit_configured else None,
            "bing_spend":             b.get("spend",             0.0),
            "bing_clicks":            b.get("clicks",            0),
            "bing_conversions_value": b.get("conversions_value", 0.0),
            "ga4_users":    a.get("users",    0),
            "ga4_sessions": a.get("sessions", 0),
        }
        if z is not None:
            row["amz_ad_spend"]    = z["amz_ad_spend"]
            row["amz_ad_sales"]    = z["amz_ad_sales"]
            row["amz_impressions"] = z["amz_impressions"]
            row["amz_purchases"]   = z["amz_purchases"]
        out[ds] = row

    return out


# ============================================================
# HTML TEMPLATE
# ============================================================

HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Tifosi Optics | Marketing Dashboard</title>
<script>
(function(){
  var HASH = "e2572f2e38f1c7172b86a2ce41feb7b6000fb794284a53ec57945cd09d9c95e4";
  async function sha256hex(s){
    var buf = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(s));
    return Array.from(new Uint8Array(buf)).map(function(b){return b.toString(16).padStart(2,'0');}).join('');
  }
  var stored = sessionStorage.getItem('td_auth');
  if(stored !== HASH){
    document.addEventListener('DOMContentLoaded', function(){
      document.body.style.display='none';
      var overlay = document.createElement('div');
      overlay.style.cssText='position:fixed;inset:0;background:#1B3A6B;display:flex;align-items:center;justify-content:center;z-index:99999;';
      overlay.innerHTML='<div style="background:#fff;border-radius:12px;padding:40px 48px;text-align:center;box-shadow:0 8px 32px rgba(0,0,0,.3);min-width:320px"><div style="font-size:18px;font-weight:700;color:#1B3A6B;margin-bottom:8px">Marketing Dashboard</div><div style="font-size:13px;color:#666;margin-bottom:24px">Enter your password to continue</div><input id="td_pw" type="password" placeholder="Password" style="width:100%;padding:10px 14px;border:1.5px solid #ddd;border-radius:7px;font-size:15px;outline:none;margin-bottom:12px"><div id="td_err" style="color:#c0392b;font-size:13px;min-height:18px;margin-bottom:8px"></div><button id="td_btn" style="width:100%;padding:11px;background:#1B3A6B;color:#fff;border:none;border-radius:7px;font-size:15px;font-weight:600;cursor:pointer">Enter</button></div>';
      document.body.parentNode.insertBefore(overlay, document.body);
      document.getElementById('td_pw').addEventListener('keydown', function(e){if(e.key==='Enter')td_check();});
      document.getElementById('td_btn').addEventListener('click', td_check);
      function td_check(){
        var pw = document.getElementById('td_pw').value;
        sha256hex(pw).then(function(hash){
          if(hash === HASH){ sessionStorage.setItem('td_auth', HASH); overlay.remove(); document.body.style.display=''; }
          else{ document.getElementById('td_err').textContent='Incorrect password'; document.getElementById('td_pw').value=''; document.getElementById('td_pw').focus(); }
        });
      }
      setTimeout(function(){document.getElementById('td_pw').focus();}, 100);
    });
  }
})();
</script>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #EEF0F5; min-height: 100vh; }

  .header {
    background: #1B3A6B; color: #fff;
    padding: 14px 24px; display: flex; align-items: center; justify-content: space-between;
    flex-wrap: wrap; gap: 12px; position: sticky; top: 0; z-index: 100; box-shadow: 0 2px 8px rgba(0,0,0,.3);
  }
  .header-title { font-size: 17px; font-weight: 700; letter-spacing: .3px; }
  .header-sub   { font-size: 11px; opacity: .7; margin-top: 2px; }

  .refresh-btn {
    display: flex; align-items: center; gap: 6px;
    background: rgba(255,255,255,.12); border: 1px solid rgba(255,255,255,.3);
    color: #fff; padding: 7px 13px; border-radius: 6px; font-size: 12px;
    cursor: pointer; white-space: nowrap; transition: background .15s; font-family: inherit;
  }
  .refresh-btn:hover { background: rgba(255,255,255,.22); }
  .refresh-btn:active { transform: scale(.97); }

  .date-btn {
    display: flex; align-items: center; gap: 8px;
    background: rgba(255,255,255,.15); border: 1px solid rgba(255,255,255,.3);
    color: #fff; padding: 7px 14px; border-radius: 6px; font-size: 13px;
    cursor: pointer; white-space: nowrap; transition: background .15s;
  }
  .date-btn:hover { background: rgba(255,255,255,.25); }
  .date-btn-sep { opacity: .45; margin: 0 4px; }
  .date-btn-cmp { opacity: .75; font-size: 11px; }

  .modal-backdrop {
    display: none; position: fixed; inset: 0; background: rgba(0,0,0,.45);
    z-index: 200; align-items: center; justify-content: center;
  }
  .modal-backdrop.open { display: flex; }
  .picker-modal {
    background: #fff; border-radius: 12px; box-shadow: 0 8px 40px rgba(0,0,0,.28);
    width: 840px; max-width: 98vw; overflow: hidden;
  }
  .picker-head { background: #1B3A6B; color: #fff; padding: 13px 20px; font-size: 14px; font-weight: 700; }
  .picker-body { display: flex; }

  /* ── Presets sidebar ── */
  .presets-panel {
    width: 155px; flex-shrink: 0; border-right: 1px solid #E5E7EB;
    padding: 14px 10px; display: flex; flex-direction: column; gap: 2px;
  }
  .preset-group-label { font-size: 9px; font-weight: 700; color: #9CA3AF; text-transform: uppercase; letter-spacing: .6px; padding: 8px 6px 4px; }
  .preset-group-label:first-child { padding-top: 0; }
  .preset-btn {
    background: none; border: none; text-align: left; padding: 6px 8px;
    font-size: 12px; color: #374151; cursor: pointer; border-radius: 5px;
    font-family: inherit; transition: background .1s;
  }
  .preset-btn:hover { background: #EEF0F5; color: #1B3A6B; font-weight: 600; }
  .preset-btn.active { background: #DBEAFE; color: #1B3A6B; font-weight: 700; }
  .presets-divider { height: 1px; background: #E5E7EB; margin: 4px 6px; }

  .cal-area { flex: 1; min-width: 0; }
  .calendars { display: flex; padding: 16px 20px 8px; gap: 0; }
  .cal { flex: 1; min-width: 190px; }
  .cal + .cal { border-left: 1px solid #E5E7EB; padding-left: 16px; margin-left: 4px; }
  .cal-nav { display: flex; align-items: center; justify-content: space-between; margin-bottom: 8px; }
  .cal-nav button {
    background: none; border: 1px solid #E5E7EB; border-radius: 5px;
    width: 24px; height: 24px; cursor: pointer; color: #374151; font-size: 15px;
    display: flex; align-items: center; justify-content: center;
  }
  .cal-nav button:hover { background: #F3F4F6; }
  .cal-nav button.invisible { visibility: hidden; }
  .cal-month-label {
    font-size: 13px; font-weight: 700; color: #111827;
    cursor: pointer; padding: 2px 6px; border-radius: 4px; transition: background .1s;
  }
  .cal-month-label:hover { background: #EEF0F5; color: #1B3A6B; }
  .cal-grid { display: grid; grid-template-columns: repeat(7,1fr); gap: 1px; }
  .cal-dow { font-size: 9px; font-weight: 700; color: #9CA3AF; text-align: center; padding: 3px 0; text-transform: uppercase; }
  .cal-day {
    font-size: 11px; text-align: center; padding: 5px 1px; border-radius: 3px;
    cursor: pointer; color: #374151; user-select: none; transition: background .08s;
  }
  .cal-day:hover:not(.disabled):not(.empty):not(.no-data) { background: #DBEAFE; color: #1E40AF; }
  .cal-day.empty  { cursor: default; }
  .cal-day.no-data { cursor: default; color: #D1D5DB; }
  .cal-day.disabled { color: #D1D5DB; cursor: not-allowed; }
  .cal-day.in-range    { background: #DBEAFE; color: #1E40AF; border-radius: 0; }
  .cal-day.range-start { background: #1B3A6B; color: #fff; border-radius: 3px 0 0 3px; font-weight: 700; }
  .cal-day.range-end   { background: #1B3A6B; color: #fff; border-radius: 0 3px 3px 0; font-weight: 700; }
  .cal-day.range-start.range-end { border-radius: 3px; }
  .cal-day.cmp-in-range { background: #FEF3C7; color: #92400E; border-radius: 0; }
  .cal-day.cmp-start  { background: #D97706; color: #fff; border-radius: 3px 0 0 3px; font-weight: 700; }
  .cal-day.cmp-end    { background: #D97706; color: #fff; border-radius: 0 3px 3px 0; font-weight: 700; }
  .cal-day.cmp-start.cmp-end { border-radius: 3px; }

  .picker-hint { font-size: 11px; color: #9CA3AF; padding: 0 20px 8px; min-height: 20px; }

  .period-panel { border-top: 1px solid #E5E7EB; padding: 12px 20px; display: flex; flex-direction: column; gap: 8px; }
  .period-row   { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
  .period-lbl   { font-size: 10px; font-weight: 700; color: #6B7280; text-transform: uppercase; letter-spacing: .5px; min-width: 58px; }
  .period-tag   { background: #1B3A6B; color: #fff; padding: 3px 10px; border-radius: 99px; font-size: 11px; font-weight: 600; white-space: nowrap; cursor: pointer; transition: box-shadow .15s; }
  .period-tag:hover { box-shadow: 0 0 0 2px rgba(27,58,107,.4); }
  .period-tag.active-edit { box-shadow: 0 0 0 3px #fff, 0 0 0 5px #1B3A6B; }
  .cmp-tag      { background: #D97706; }
  .cmp-tag:hover { box-shadow: 0 0 0 2px rgba(217,119,6,.4); }
  .cmp-tag.active-edit { box-shadow: 0 0 0 3px #fff, 0 0 0 5px #D97706; }
  .cmp-toggle   { display: flex; align-items: center; gap: 6px; cursor: pointer; user-select: none; }
  .cmp-toggle input { width: 14px; height: 14px; accent-color: #1B3A6B; cursor: pointer; }
  .cmp-toggle span  { font-size: 12px; font-weight: 600; color: #374151; }
  .last-yr-btn  { background: #F3F4F6; border: 1px solid #D1D5DB; border-radius: 5px; padding: 3px 10px; font-size: 11px; color: #374151; cursor: pointer; font-weight: 600; }
  .last-yr-btn:hover { background: #E5E7EB; }

  .picker-footer {
    display: flex; align-items: center; justify-content: flex-end; gap: 10px;
    padding: 11px 20px; border-top: 1px solid #E5E7EB; background: #F9FAFB;
  }
  .btn-cancel { background: #fff; border: 1px solid #D1D5DB; border-radius: 6px; padding: 7px 18px; font-size: 13px; color: #374151; cursor: pointer; font-weight: 600; }
  .btn-cancel:hover { background: #F3F4F6; }
  .btn-apply  { background: #1B3A6B; border: none; border-radius: 6px; padding: 7px 18px; font-size: 13px; color: #fff; cursor: pointer; font-weight: 600; }
  .btn-apply:hover  { background: #163260; }

  .grid { display: grid; grid-template-columns: repeat(3,1fr); gap: 14px; padding: 20px 24px; max-width: 1400px; margin: 0 auto; }
  @media (max-width: 900px) { .grid { grid-template-columns: repeat(2,1fr); } }
  @media (max-width: 580px) { .grid { grid-template-columns: 1fr; } }

  .card { background: #fff; border-radius: 10px; padding: 16px 18px; box-shadow: 0 1px 4px rgba(0,0,0,.08); display: flex; flex-direction: column; gap: 6px; transition: box-shadow .15s; }
  .card:hover { box-shadow: 0 3px 12px rgba(0,0,0,.14); }
  .card-source  { display: flex; align-items: center; gap: 6px; }
  .source-dot   { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }
  .source-label { font-size: 10px; font-weight: 600; text-transform: uppercase; letter-spacing: .6px; opacity: .55; }
  .card-name    { font-size: 13px; color: #374151; font-weight: 500; margin-top: 2px; }
  .card-value   { font-size: 26px; font-weight: 700; color: #111827; line-height: 1.1; }
  .card-compare { display: flex; align-items: center; gap: 8px; margin-top: 4px; }
  .card-compare-val { font-size: 12px; color: #6B7280; }
  .badge      { font-size: 11px; font-weight: 700; padding: 2px 7px; border-radius: 99px; display: inline-flex; align-items: center; gap: 3px; }
  .badge-up   { background: #DCFCE7; color: #16A34A; }
  .badge-down { background: #FEE2E2; color: #DC2626; }
  .badge-zero { background: #F3F4F6; color: #6B7280; }
  .card-na    { font-size: 20px; color: #9CA3AF; font-weight: 600; }

  .table-card { background: #fff; border-radius: 10px; padding: 18px 20px; box-shadow: 0 1px 4px rgba(0,0,0,.08); max-width: 1400px; margin: 0 24px 24px; }
  .table-card h3 { font-size: 14px; font-weight: 700; color: #1B3A6B; margin-bottom: 14px; }
  .prod-table { width: 100%; border-collapse: collapse; font-size: 13px; }
  .prod-table th { text-align: left; padding: 8px 10px; background: #F9FAFB; color: #6B7280; font-size: 11px; text-transform: uppercase; letter-spacing: .5px; border-bottom: 2px solid #E5E7EB; }
  .prod-table th.right, .prod-table td.right { text-align: right; }
  .prod-table td { padding: 9px 10px; border-bottom: 1px solid #F3F4F6; color: #111827; }
  .prod-table tr:last-child td { border-bottom: none; }
  .prod-table tr:hover td { background: #F9FAFB; }
  .prod-view-toggle { display:flex; gap:6px; }
  .prod-view-btn { padding:5px 14px; border-radius:6px; border:1px solid #D1D5DB; background:#fff; color:#6B7280; font-size:12px; font-weight:600; cursor:pointer; transition:all .15s; }
  .prod-view-btn.active { background:#1B3A6B; color:#fff; border-color:#1B3A6B; }
</style>
</head>
<body>

<div class="header">
  <div>
    <div class="header-title">Tifosi Optics &nbsp;|&nbsp; Marketing Dashboard</div>
    <div class="header-sub">Updated: __GENERATED_AT__ &nbsp;<span id="staleness"></span></div>
  </div>
  <div style="display:flex;align-items:center;gap:8px;">
    <button class="refresh-btn" onclick="triggerRefresh()" title="Re-run the data pipeline and push a fresh dashboard (~5 min)">
      <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
        <polyline points="23 4 23 10 17 10"/><polyline points="1 20 1 14 7 14"/>
        <path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/>
      </svg>
      Refresh Data
    </button>
    <button class="date-btn" id="date-btn" onclick="openPicker()">
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
        <rect x="3" y="4" width="18" height="18" rx="2"/><line x1="16" y1="2" x2="16" y2="6"/>
        <line x1="8" y1="2" x2="8" y2="6"/><line x1="3" y1="10" x2="21" y2="10"/>
      </svg>
      <span id="date-btn-main">Loading...</span>
      <span class="date-btn-sep" id="date-btn-sep">|</span>
      <span class="date-btn-cmp" id="date-btn-cmp"></span>
    </button>
  </div>
</div>

<div class="modal-backdrop" id="modal-backdrop" onclick="backdropClick(event)">
  <div class="picker-modal" onclick="event.stopPropagation()">
    <div class="picker-head">Select Date Range</div>
    <div class="picker-body">
      <div class="presets-panel" id="presets-panel">
        <div class="preset-group-label">Quick Select</div>
      </div>
      <div class="cal-area">
        <div class="calendars">
          <div class="cal" id="cal-left"></div>
          <div class="cal" id="cal-right"></div>
        </div>
        <div class="picker-hint" id="picker-hint">Click a date to start selecting</div>
        <div class="period-panel">
          <div class="period-row">
            <span class="period-lbl">Period</span>
            <span class="period-tag active-edit" id="pp-main-tag" onclick="switchSelecting('main')" title="Click to edit main period">—</span>
          </div>
          <div class="period-row">
            <label class="cmp-toggle">
              <input type="checkbox" id="cmp-chk" onchange="toggleCompare(this.checked)">
              <span>Compare Period</span>
            </label>
            <span class="period-tag cmp-tag" id="pp-cmp-tag" onclick="switchSelecting('cmp')" title="Click to edit compare period" style="display:none">—</span>
            <button class="last-yr-btn" id="last-yr-btn" onclick="setLastYear()" style="display:none">Last year</button>
            <button class="last-yr-btn" id="same-days-btn" onclick="setSameDaysLastYear()" style="display:none">Same days last year</button>
            <button class="last-yr-btn" id="prev-period-btn" onclick="setPrevPeriod()" style="display:none">Previous period</button>
          </div>
        </div>
        <div class="picker-footer">
          <button class="btn-cancel" onclick="cancelPicker()">Cancel</button>
          <button class="btn-apply"  onclick="applyPicker()">Apply</button>
        </div>
      </div>
    </div>
  </div>
</div>

<div class="grid" id="card-grid"></div>
<div style="padding:24px 24px 4px;max-width:1400px;margin:0 auto;border-top:2px solid #E5E7EB;margin-top:8px;">
  <h2 style="font-size:15px;font-weight:700;color:#374151;margin:0;text-transform:uppercase;letter-spacing:.5px;">Amazon</h2>
</div>
<div class="grid" id="amazon-card-grid"></div>

<div class="table-card">
  <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:14px;">
    <h3 id="prod-table-title" style="margin-bottom:0">Top Eyewear Products</h3>
    <div class="prod-view-toggle">
      <button class="prod-view-btn active" onclick="setProdView('summary')">Summary</button>
      <button class="prod-view-btn" onclick="setProdView('daily')">Daily</button>
      <button class="prod-view-btn" onclick="setProdView('monthly')">Monthly</button>
    </div>
  </div>
  <table class="prod-table">
    <thead><tr id="prod-thead-row">
      <th>Product</th><th class="right">Orders</th><th class="right">Orders Change</th>
      <th class="right">Net Sales</th><th class="right">Sales Change</th>
    </tr></thead>
    <tbody id="prod-tbody"></tbody>
  </table>
</div>

<script>
const GENERATED_AT = new Date("__GENERATED_AT_ISO__");
(function() {
  function tick() {
    const hrs = (Date.now() - GENERATED_AT) / 3600000;
    const el  = document.getElementById("staleness");
    if (!el) return;
    let age, color;
    if (hrs < 1)       { age = "just now";                               color = "#6EE7B7"; }
    else if (hrs < 12) { age = Math.floor(hrs)+"h ago";                  color = "#6EE7B7"; }
    else if (hrs < 24) { age = Math.floor(hrs)+"h ago";                  color = "#FCD34D"; }
    else {
      const d = Math.floor(hrs/24);
      age = d+"d "+Math.floor(hrs%24)+"h ago — run update";
      color = "#FCA5A5";
    }
    const etTime = GENERATED_AT.toLocaleTimeString("en-US", {hour:"numeric", minute:"2-digit", timeZone:"America/New_York"});
    el.textContent = "· refreshed at " + etTime + " ET (" + age + ")";
    el.style.cssText = `color:${color};font-weight:600;`;
  }
  tick();
  setInterval(tick, 60000);
})();

async function triggerRefresh() {
  const TOKEN = 'jlwkxebsdwb44FEIUHKT37MGz5VPgzRnkb<D6yegD}h}zfrp6hLt}mz5{Ny;u8IZjRe4q5i}UpEEeHKJEI78ZUZ:35w3N'.split('').map(c=>String.fromCharCode(c.charCodeAt(0)-3)).join('');
  const btn = document.querySelector('.refresh-btn');
  btn.disabled = true;
  btn.style.opacity = '0.6';
  btn.innerHTML = btn.innerHTML.replace('Refresh Data', 'Triggering...');

  let success = false;
  try {
    const resp = await fetch('https://api.github.com/repos/BaileyTifosi/tifosi-dashboard/actions/workflows/refresh.yml/dispatches', {
      method: 'POST',
      headers: {
        'Authorization': 'Bearer ' + TOKEN,
        'Accept': 'application/vnd.github+json',
        'X-GitHub-Api-Version': '2022-11-28',
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ ref: 'main' })
    });
    success = (resp.status === 204);
  } catch(e) { success = false; }

  const toast = document.createElement('div');
  toast.style.cssText = 'position:fixed;bottom:24px;right:24px;background:#1B3A6B;color:#fff;padding:14px 20px;border-radius:8px;box-shadow:0 4px 20px rgba(0,0,0,.35);z-index:9999;font-size:13px;max-width:340px;line-height:1.5;';
  if (success) {
    toast.innerHTML = '<strong style="font-size:14px;">Update triggered!</strong><br><span style="opacity:.85;">The data pipeline is running (~5 min). Hard-refresh this page when done (Ctrl+Shift+R).</span>';
  } else {
    toast.innerHTML = '<strong style="font-size:14px;">Could not trigger update</strong><br><span style="opacity:.85;">Check that the runner is online, then try again.</span>';
    toast.style.background = '#991b1b';
  }
  const close = document.createElement('span');
  close.textContent = '✕';
  close.style.cssText = 'position:absolute;top:8px;right:12px;cursor:pointer;opacity:.6;font-size:16px;';
  close.onclick = () => toast.remove();
  toast.appendChild(close);
  document.body.appendChild(toast);
  setTimeout(() => toast.remove(), 10000);

  setTimeout(() => {
    btn.disabled = false;
    btn.style.opacity = '';
    btn.innerHTML = btn.innerHTML.replace('Triggering...', 'Refresh Data');
  }, 5000);
}

const DATA               = __DATA_JSON__;
const DAYS               = __DAYS_JSON__;   // all dates with data, newest first
const PRODUCT_DATA       = __PRODUCT_JSON__;  // {date: {product_title: {orders, revenue}}}
const MONTHLY_META_REACH  = __MONTHLY_REACH_JSON__;   // {YYYY-MM: unique_reach}
const MONTHLY_GA4         = __MONTHLY_GA4_JSON__;     // {YYYY-MM: {users, sessions}}
const MONTHLY_KLAVIYO     = __MONTHLY_KLAVIYO_JSON__;  // {YYYY-MM: {emails_sent, revenue}}
const MONTHLY_AMAZON_ADS  = __MONTHLY_AMAZON_ADS_JSON__;  // {YYYY-MM: {amz_ad_spend, amz_ad_sales, ...}}
const MONTHLY_AMAZON_SC   = __MONTHLY_AMAZON_SC_JSON__;   // {YYYY-MM: {amz_sc_sales, amz_sc_orders, amz_sc_units}}

const DATA_MIN = DAYS[DAYS.length - 1];
const DATA_MAX = DAYS[0];
const DAYS_SET = new Set(DAYS);
const REDDIT_ON = Object.values(DATA).some(d => d.reddit_spend !== null && d.reddit_spend !== undefined);

const SOURCE_COLORS = { shopify:"#96BF47", meta:"#1877F2", google:"#EA4335", ga4:"#E37400", reddit:"#FF4500", bing:"#00809D", klaviyo:"#6B4FBB", ads:"#6B7280", amazon:"#FF9900" };
const SOURCE_LABELS = { shopify:"Shopify", meta:"Facebook Ads", google:"Google Ads", ga4:"GA4", reddit:"Reddit Ads", bing:"Microsoft Ads", klaviyo:"Klaviyo", ads:"All Platforms", amazon:"Amazon" };
const CARDS = [
  { key:"net_sales",          name:"Net Sales",                 fmt:fmtCur, source:"shopify", hib:true  },
  { key:"orders",             name:"Orders",                    fmt:fmtInt, source:"shopify", hib:true  },
  { key:"aov",                name:"Average Order Value",       fmt:fmtCur, source:"shopify", hib:true  },
  { key:"total_ad_spend",     name:"Total Ad Spend",            fmt:fmtCur, source:"ads",     hib:false },
  { key:"roas",               name:"Return on Spend",           fmt:fmtX,   source:"ads",     hib:true  },
  { key:"cac",                name:"Acquisition Cost",          fmt:fmtCur, source:"ads",     hib:false },
  { key:"meta_roas",          name:"Facebook ROAS",             fmt:fmtX,   source:"meta",    hib:true  },
  { key:"google_roas",        name:"Google Ads ROAS",           fmt:fmtX,   source:"google",  hib:true  },
  { key:"cvr",                name:"Ecommerce Conversion Rate", fmt:fmtPct, source:"shopify", hib:true  },
  { key:"meta_spend",         name:"Facebook Total Spend",      fmt:fmtCur, source:"meta",    hib:false },
  { key:"google_spend",       name:"Google Ad Spend",           fmt:fmtCur, source:"google",  hib:false },
  { key:"google_impressions", name:"Google Impressions",        fmt:fmtInt, source:"google",  hib:true  },
  { key:"reddit_roas",        name:"Reddit Ads ROAS",           fmt:fmtX,   source:"reddit",  hib:true  },
  { key:"reddit_spend",       name:"Reddit Ads Total Spend",    fmt:fmtCur, source:"reddit",  hib:false },
  { key:"bing_spend",         name:"Bing Ads Total Spend",      fmt:fmtCur, source:"bing",    hib:false },
  { key:"bing_roas",          name:"Bing Ads ROAS",             fmt:fmtX,   source:"bing",    hib:true  },
  { key:"meta_impressions",   name:"Facebook Impressions",      fmt:fmtInt, source:"meta",    hib:true  },
  { key:"meta_reach",         name:"Facebook Reach",            fmt:fmtInt, source:"meta",    hib:true  },
  { key:"ga4_users",          name:"Website Users",              fmt:fmtInt, source:"ga4",     hib:true  },
  { key:"ga4_sessions",       name:"Website Sessions",          fmt:fmtInt, source:"ga4",     hib:true  },
  { key:"klaviyo_emails_sent",name:"Emails Sent (Campaigns + Flows)", fmt:fmtInt, source:"klaviyo", hib:true },
  { key:"klaviyo_revenue",    name:"Email Revenue",             fmt:fmtCur, source:"klaviyo", hib:true  },
];

const AMAZON_CARDS = [
  { key:"amz_gross_sales",  name:"Amazon Gross Sales (US)",  fmt:fmtCur, hib:true  },
  { key:"amz_tacos",        name:"Amazon TACOS",             fmt:fmtPct, hib:false },
  { key:"amz_ad_spend",     name:"Amazon Ad Spend",          fmt:fmtCur, hib:false },
  { key:"amz_ad_sales",     name:"Amazon Ad Sales",          fmt:fmtCur, hib:true  },
  { key:"amz_ad_roas",      name:"Amazon ROAS",              fmt:fmtX,   hib:true  },
  { key:"amz_impressions",  name:"Amazon Impressions",       fmt:fmtInt, hib:true  },
  { key:"amz_purchases",    name:"Amazon Purchases",         fmt:fmtInt, hib:true  },
  { key:"amz_sc_sales",     name:"Amazon Sales (FBA/DTC)",   fmt:fmtCur, hib:true  },
  { key:"amz_sc_orders",    name:"Amazon Orders",            fmt:fmtInt, hib:true  },
  { key:"amz_sc_units",     name:"Amazon Units Sold",        fmt:fmtInt, hib:true  },
];

function fmtCur(v) { return v==null?"N/A":"$"+v.toLocaleString("en-US",{minimumFractionDigits:2,maximumFractionDigits:2}); }
function fmtInt(v) { return v==null?"N/A":Math.round(v).toLocaleString("en-US"); }
function fmtX(v)   { return v==null?"N/A":v.toFixed(2)+"x"; }
function fmtPct(v) { return v==null?"N/A":v.toFixed(2)+"%"; }

function badge(cur, cmp, hib=true) {
  if (cur==null||cmp==null||cmp===0) return '<span class="badge badge-zero">—</span>';
  const pct=((cur-cmp)/Math.abs(cmp))*100, abs=Math.abs(pct).toFixed(1), isUp=pct>0;
  const good=hib?isUp:!isUp;
  return `<span class="badge ${good?"badge-up":"badge-down"}">${isUp?"▲":"▼"} ${abs}%</span>`;
}

// ── Date helpers ─────────────────────────────────────────────
function addDays(ds, n) {
  const d=new Date(ds+"T12:00:00"); d.setDate(d.getDate()+n);
  return d.getFullYear()+"-"+String(d.getMonth()+1).padStart(2,"0")+"-"+String(d.getDate()).padStart(2,"0");
}
function addYears(ds, n) {
  const d = new Date(ds + "T12:00:00");
  d.setFullYear(d.getFullYear() + n);
  return d.getFullYear() + "-" + String(d.getMonth()+1).padStart(2,"0") + "-" + String(d.getDate()).padStart(2,"0");
}
function addMonthsYM(ym, n) {
  const d = new Date(+ym.slice(0,4), +ym.slice(5,7)-1+n, 1);
  return d.getFullYear() + "-" + String(d.getMonth()+1).padStart(2,"0");
}
function ymLabel(ym) { return new Date(+ym.slice(0,4), +ym.slice(5,7)-1, 1).toLocaleString("en-US",{month:"long",year:"numeric"}); }
function daysInMonth(ym) { return new Date(+ym.slice(0,4), +ym.slice(5,7), 0).getDate(); }
function firstDOW(ym)    { return new Date(+ym.slice(0,4), +ym.slice(5,7)-1, 1).getDay(); }
function clampDate(ds)   { return ds<DATA_MIN?DATA_MIN:ds>DATA_MAX?DATA_MAX:ds; }
function dateLabel(ds)   { return new Date(ds+"T12:00:00").toLocaleString("en-US",{month:"long",day:"numeric",year:"numeric"}); }
function dateShort(ds)   { return new Date(ds+"T12:00:00").toLocaleString("en-US",{month:"short",day:"numeric",year:"numeric"}); }

function rangeLabel(s, e) {
  if (s===e) return dateLabel(s);
  const sd=new Date(s+"T12:00:00"), ed=new Date(e+"T12:00:00");
  if (sd.getFullYear()===ed.getFullYear() && sd.getMonth()===ed.getMonth())
    return sd.toLocaleString("en-US",{month:"long"})+" "+sd.getDate()+"–"+ed.getDate()+", "+sd.getFullYear();
  if (sd.getFullYear()===ed.getFullYear())
    return sd.toLocaleString("en-US",{month:"short",day:"numeric"})+" – "+ed.toLocaleString("en-US",{month:"short",day:"numeric"})+", "+sd.getFullYear();
  return dateShort(s)+" – "+dateShort(e);
}

// ── State ─────────────────────────────────────────────────────
function defaultState() {
  try {
    const saved = localStorage.getItem('td_range');
    if (saved) {
      const p = JSON.parse(saved);
      if (p.mainStart && p.mainEnd && p.cmpStart && p.cmpEnd) {
        return { mainStart:clampDate(p.mainStart), mainEnd:clampDate(p.mainEnd),
                 cmpStart:clampDate(p.cmpStart),   cmpEnd:clampDate(p.cmpEnd),
                 compareOn: p.compareOn !== false };
      }
    }
  } catch(e) {}
  const t=new Date(), y=t.getFullYear(), m=t.getMonth();
  const le=new Date(y,m,0), ls=new Date(y,m-1,1);
  const fmt=d=>d.getFullYear()+"-"+String(d.getMonth()+1).padStart(2,"0")+"-"+String(d.getDate()).padStart(2,"0");
  const mS=clampDate(fmt(ls)), mE=clampDate(fmt(le));
  return { mainStart:mS, mainEnd:mE, cmpStart:clampDate(addYears(mS,-1)), cmpEnd:clampDate(addYears(mE,-1)), compareOn:true };
}

let applied      = defaultState();
let st           = {...applied};
let navLeft      = addMonthsYM(DATA_MAX, -1);
let clicking     = false;
let hoverDate    = null;
let selectingFor = "main";   // "main" or "cmp"

// ── Aggregation ──────────────────────────────────────────────
function aggregate(startD, endD) {
  const keys = DAYS.filter(d => d>=startD && d<=endD);
  if (!keys.length) return null;
  const s = {
    net_sales:0, orders:0,
    meta_spend:0, meta_clicks:0, meta_impressions:0, meta_reach:0, meta_purchase_value:0,
    google_spend:0, google_clicks:0, google_impressions:0, google_conversions_value:0,
    reddit_spend:0, reddit_clicks:0, reddit_purchase_value:0, bing_spend:0, bing_clicks:0, bing_conversions_value:0,
    ga4_users:0, ga4_sessions:0,
  };
  for (const k of keys) { const d=DATA[k]||{}; for (const f of Object.keys(s)) s[f]+=(d[f]||0); }

  s.aov = s.orders>0 ? s.net_sales/s.orders : null;

  // For reach, users, sessions: always use monthly API totals (deduped) when range starts on the 1st.
  // Daily sums overcount — reach/users deduplicate within a month, so daily summing inflates by ~1.7x.
  // Partial months show MTD totals through the last daily refresh.
  if (startD.endsWith("-01")) {
    let reach=0, users=0, sessions=0, ok=true;
    let ym=startD.slice(0,7), endYM=endD.slice(0,7);
    while (ym<=endYM) {
      if (MONTHLY_META_REACH[ym]==null || MONTHLY_GA4[ym]==null) { ok=false; break; }
      reach   += MONTHLY_META_REACH[ym];
      users   += MONTHLY_GA4[ym].users;
      sessions+= MONTHLY_GA4[ym].sessions;
      ym=addMonthsYM(ym,1);
    }
    if (ok) { s.meta_reach=reach; s.ga4_users=users; s.ga4_sessions=sessions; }
  }

  // Klaviyo: show for any range starting on the 1st — partial months show MTD totals.
  if (startD.endsWith("-01")) {
    let kEmails=0, kRev=0, kOk=true;
    let kym=startD.slice(0,7), kendYM=endD.slice(0,7);
    while (kym<=kendYM) {
      if (MONTHLY_KLAVIYO[kym]==null) { kOk=false; break; }
      kEmails += MONTHLY_KLAVIYO[kym].emails_sent;
      kRev    += MONTHLY_KLAVIYO[kym].revenue;
      kym=addMonthsYM(kym,1);
    }
    s.klaviyo_emails_sent = kOk ? kEmails : null;
    s.klaviyo_revenue     = kOk ? kRev    : null;
  }

  const radS = REDDIT_ON ? s.reddit_spend : 0;
  s.total_ad_spend = s.meta_spend + s.google_spend + s.bing_spend + radS;
  s.roas = s.total_ad_spend>0 ? s.net_sales/s.total_ad_spend : null;
  s.cac  = s.orders>0 ? s.total_ad_spend/s.orders : null;
  s.cvr  = s.ga4_sessions>0 ? (s.orders/s.ga4_sessions*100) : null;
  s.meta_roas   = s.meta_spend>0 ? s.meta_purchase_value/s.meta_spend : null;
  s.google_roas = s.google_spend>0 ? s.google_conversions_value/s.google_spend : null;
  s.bing_roas   = s.bing_spend>0 ? s.bing_conversions_value/s.bing_spend : null;
  s.reddit_roas = (REDDIT_ON && s.reddit_spend>0) ? s.reddit_purchase_value/s.reddit_spend : null;
  if (!REDDIT_ON) { s.reddit_spend=null; s.reddit_clicks=null; s.reddit_purchase_value=null; }
  return s;
}

// ── Calendar ─────────────────────────────────────────────────
function dayClass(ds) {
  let rS=st.mainStart, rE=st.mainEnd;
  let cS=st.cmpStart,  cE=st.cmpEnd;
  if (clicking && hoverDate) {
    if (selectingFor==="main") { const s=[st.mainStart,hoverDate].sort(); rS=s[0]; rE=s[1]; }
    else                       { const s=[st.cmpStart, hoverDate].sort(); cS=s[0]; cE=s[1]; }
  }
  if (ds>=rS && ds<=rE) {
    const atS=ds===rS, atE=ds===rE;
    if (atS&&atE) return "range-start range-end";
    if (atS) return "range-start";
    if (atE) return "range-end";
    return "in-range";
  }
  if (st.compareOn && ds>=cS && ds<=cE) {
    const atS=ds===cS, atE=ds===cE;
    if (atS&&atE) return "cmp-start cmp-end";
    if (atS) return "cmp-start";
    if (atE) return "cmp-end";
    return "cmp-in-range";
  }
  return "";
}

function buildCalHTML(ym, showPrev, showNext) {
  const fdow=firstDOW(ym), days=daysInMonth(ym);
  const y=ym.slice(0,4), mo=ym.slice(5,7);
  let html=`
    <div class="cal-nav">
      <button ${showPrev?`onclick="navCal(-1)"`:`class="invisible"`}>&#8249;</button>
      <span class="cal-month-label">${ymLabel(ym)}</span>
      <button ${showNext?`onclick="navCal(1)"`:`class="invisible"`}>&#8250;</button>
    </div>
    <div class="cal-grid">
      <span class="cal-dow">Su</span><span class="cal-dow">Mo</span><span class="cal-dow">Tu</span>
      <span class="cal-dow">We</span><span class="cal-dow">Th</span><span class="cal-dow">Fr</span>
      <span class="cal-dow">Sa</span>`;
  for (let i=0;i<fdow;i++) html+=`<span class="cal-day empty"></span>`;
  for (let d=1;d<=days;d++) {
    const ds=`${y}-${mo}-${String(d).padStart(2,"0")}`;
    const hasData = DAYS_SET.has(ds);
    const cls = dayClass(ds) + (hasData ? "" : " no-data");
    const handler = hasData ? `onclick="calClick('${ds}')" onmouseover="calHover('${ds}')"` : "";
    html+=`<span class="cal-day ${cls.trim()}" ${handler}>${d}</span>`;
  }
  html+=`</div>`;
  return html;
}

function renderCals() {
  const right = addMonthsYM(navLeft, 1);
  document.getElementById("cal-left").innerHTML  = buildCalHTML(navLeft, true,  false);
  document.getElementById("cal-right").innerHTML = buildCalHTML(right,   false, true);

  let rS=st.mainStart, rE=st.mainEnd;
  if (clicking && hoverDate) { const sorted=[st.mainStart,hoverDate].sort(); rS=sorted[0]; rE=sorted[1]; }
  document.getElementById("pp-main-tag").textContent = rangeLabel(rS, rE);

  // Active-edit highlight on the currently-selected period tag
  document.getElementById("pp-main-tag").className =
    "period-tag" + (selectingFor==="main" ? " active-edit" : "");

  const hint = clicking
    ? "Click to set end date"
    : (selectingFor==="main" ? "Selecting main period — click start date" : "Selecting compare period — click start date");
  document.getElementById("picker-hint").textContent = hint;

  const cmpTag=document.getElementById("pp-cmp-tag");
  const lyrBtn=document.getElementById("last-yr-btn");
  const sameDaysBtn=document.getElementById("same-days-btn");
  const prevBtn=document.getElementById("prev-period-btn");
  if (st.compareOn) {
    cmpTag.style.display="inline"; lyrBtn.style.display="inline"; sameDaysBtn.style.display="inline"; prevBtn.style.display="inline";
    cmpTag.textContent = rangeLabel(st.cmpStart, st.cmpEnd);
    cmpTag.className = "period-tag cmp-tag" + (selectingFor==="cmp" ? " active-edit" : "");
  } else {
    cmpTag.style.display="none"; lyrBtn.style.display="none"; sameDaysBtn.style.display="none"; prevBtn.style.display="none";
  }
  renderPresets();
}

function switchSelecting(which) {
  if (which==="cmp" && !st.compareOn) return;
  selectingFor=which; clicking=false; hoverDate=null;
  renderCals();
}

function navCal(n) { navLeft=addMonthsYM(navLeft,n); renderCals(); }

function calClick(ds) {
  if (!clicking) {
    if (selectingFor==="main") { st.mainStart=ds; st.mainEnd=ds; }
    else                       { st.cmpStart=ds;  st.cmpEnd=ds;  }
    clicking=true; hoverDate=ds;
  } else {
    if (selectingFor==="main") {
      const sorted=[st.mainStart,ds].sort();
      st.mainStart=sorted[0]; st.mainEnd=sorted[1];
      // Auto-update compare to last year, then switch to compare so user can adjust
      if (st.compareOn) {
        st.cmpStart=clampDate(addYears(st.mainStart,-1));
        st.cmpEnd  =clampDate(addYears(st.mainEnd,  -1));
        clicking=false; hoverDate=null;
        selectingFor="cmp";
        renderCals(); return;
      }
    } else {
      const sorted=[st.cmpStart,ds].sort();
      st.cmpStart=sorted[0]; st.cmpEnd=sorted[1];
    }
    clicking=false; hoverDate=null;
  }
  renderCals();
}

function calHover(ds) { if (clicking && ds!==hoverDate) { hoverDate=ds; renderCals(); } }
function toggleCompare(on) { st.compareOn=on; renderCals(); }
function setLastYear() {
  st.cmpStart=clampDate(addYears(st.mainStart,-1));
  st.cmpEnd  =clampDate(addYears(st.mainEnd,  -1));
  renderCals();
}
function setSameDaysLastYear() {
  // Go back exactly 52 weeks (364 days) — preserves day of week
  st.cmpStart=clampDate(addDays(st.mainStart,-364));
  st.cmpEnd  =clampDate(addDays(st.mainEnd,  -364));
  renderCals();
}
function setPrevPeriod() {
  const spanDays=Math.round((new Date(st.mainEnd)-new Date(st.mainStart))/86400000);
  const cmpEnd=addDays(st.mainStart,-1);
  const cmpStart=addDays(cmpEnd,-spanDays);
  st.cmpStart=clampDate(cmpStart); st.cmpEnd=clampDate(cmpEnd);
  renderCals();
}
function getPresets() {
  const t=new Date(), y=t.getFullYear(), m=t.getMonth();
  const fmt=d=>d.getFullYear()+"-"+String(d.getMonth()+1).padStart(2,"0")+"-"+String(d.getDate()).padStart(2,"0");
  return [
    { label:"This Month",    start:fmt(new Date(y,m,1)),   end:clampDate(DATA_MAX) },
    { label:"Last Month",    start:fmt(new Date(y,m-1,1)), end:fmt(new Date(y,m,0)) },
    { label:"Last 3 Months", start:fmt(new Date(y,m-3,1)), end:fmt(new Date(y,m,0)) },
    { label:"Last 6 Months", start:fmt(new Date(y,m-6,1)), end:fmt(new Date(y,m,0)) },
    { label:"Year to Date",  start:fmt(new Date(y,0,1)),   end:clampDate(DATA_MAX) },
    { label:"Last Year",     start:fmt(new Date(y-1,0,1)), end:fmt(new Date(y-1,11,31)) },
  ];
}
function activePresetIndex() {
  const ps=getPresets();
  for (let i=0;i<ps.length;i++) {
    if (clampDate(ps[i].start)===st.mainStart && clampDate(ps[i].end)===st.mainEnd) return i;
  }
  return -1;
}
function applyPreset(i) {
  const p=getPresets()[i];
  st.mainStart=clampDate(p.start); st.mainEnd=clampDate(p.end);
  if (st.compareOn) {
    st.cmpStart=clampDate(addYears(st.mainStart,-1));
    st.cmpEnd  =clampDate(addYears(st.mainEnd,  -1));
  }
  clicking=false; hoverDate=null; selectingFor="main";
  navLeft=addMonthsYM(st.mainEnd,-1);
  renderCals();
}
function renderPresets() {
  const sidebar=document.getElementById("presets-panel");
  const active=activePresetIndex();
  const ps=getPresets();
  sidebar.innerHTML='<div class="preset-group-label">Quick Select</div>';
  ps.forEach((p,i)=>{
    const btn=document.createElement("button");
    btn.className="preset-btn"+(i===active?" active":"");
    btn.textContent=p.label;
    btn.onclick=()=>applyPreset(i);
    sidebar.appendChild(btn);
  });
}

function openPicker() {
  st={...applied}; clicking=false; hoverDate=null; selectingFor="main";
  navLeft=addMonthsYM(applied.mainEnd,-1);
  document.getElementById("cmp-chk").checked=st.compareOn;
  document.getElementById("modal-backdrop").classList.add("open");
  renderCals();
}
function cancelPicker() {
  document.getElementById("modal-backdrop").classList.remove("open");
  clicking=false; hoverDate=null;
}
function applyPicker() {
  if (clicking) { clicking=false; hoverDate=null; }
  applied={...st};
  try { localStorage.setItem('td_range', JSON.stringify(applied)); } catch(e) {}
  document.getElementById("modal-backdrop").classList.remove("open");
  updateHeader(); renderCards(); renderAmazonCards();
}
function backdropClick(e) { if (e.target===document.getElementById("modal-backdrop")) cancelPicker(); }

let prodView = 'summary';

function setProdView(v) {
  prodView = v;
  document.querySelectorAll('.prod-view-btn').forEach(b => {
    b.classList.toggle('active', b.getAttribute('onclick').includes("'"+v+"'"));
  });
  renderProductTable();
}

function renderProductTable() {
  const start=applied.mainStart, end=applied.mainEnd;

  function getDivisor(s, e) {
    if (prodView==='summary') return 1;
    if (prodView==='daily')
      return Math.max(1, Math.round((new Date(e)-new Date(s))/86400000)+1);
    // monthly: count unique YYYY-MM with actual data in range
    const ms=new Set();
    Object.keys(PRODUCT_DATA).forEach(d=>{if(d>=s&&d<=e)ms.add(d.slice(0,7));});
    return Math.max(1, ms.size);
  }

  const div = getDivisor(start, end);
  const ordLabel = prodView==='daily' ? 'Avg Daily Orders' : prodView==='monthly' ? 'Avg/Month Orders' : 'Orders';
  const revLabel = prodView==='daily' ? 'Avg Daily Revenue' : prodView==='monthly' ? 'Avg/Month Revenue' : 'Net Sales';
  document.getElementById('prod-thead-row').innerHTML =
    `<th>Product</th><th class="right">${ordLabel}</th><th class="right">Orders Change</th><th class="right">${revLabel}</th><th class="right">Sales Change</th>`;

  const totals={};
  Object.entries(PRODUCT_DATA).forEach(([date, prods]) => {
    if (date<start || date>end) return;
    Object.entries(prods).forEach(([title, d]) => {
      if (!totals[title]) totals[title]={orders:0, revenue:0};
      totals[title].orders  += d.orders;
      totals[title].revenue += d.revenue;
    });
  });

  let cmpTotals=null;
  if (applied.compareOn) {
    const cDiv = getDivisor(applied.cmpStart, applied.cmpEnd);
    cmpTotals={};
    Object.entries(PRODUCT_DATA).forEach(([date, prods]) => {
      if (date<applied.cmpStart || date>applied.cmpEnd) return;
      Object.entries(prods).forEach(([title, d]) => {
        if (!cmpTotals[title]) cmpTotals[title]={orders:0, revenue:0};
        cmpTotals[title].orders  += d.orders;
        cmpTotals[title].revenue += d.revenue;
      });
    });
    if (cDiv>1) Object.values(cmpTotals).forEach(v=>{v.orders/=cDiv; v.revenue/=cDiv;});
  }

  // Sort by raw period revenue, then display values divided by divisor
  const sorted = Object.entries(totals)
    .sort((a,b) => b[1].revenue - a[1].revenue)
    .slice(0, 20);

  const tbody=document.getElementById("prod-tbody");
  if (!sorted.length) {
    tbody.innerHTML="<tr><td colspan='5' style='color:#9CA3AF;padding:14px'>No product data for this period.</td></tr>";
    return;
  }
  tbody.innerHTML=sorted.map(([title, raw]) => {
    const d = {orders: raw.orders/div, revenue: raw.revenue/div};
    const cmp = cmpTotals ? cmpTotals[title] : null;
    const ordBadge = cmp ? badge(d.orders, cmp.orders, true) : "";
    const revBadge = cmp ? badge(d.revenue, cmp.revenue, true) : "";
    const ordFmt = prodView==='summary' ? fmtInt(raw.orders) : d.orders.toFixed(1);
    return `<tr>
      <td>${title}</td>
      <td class="right">${ordFmt}</td>
      <td class="right">${ordBadge}</td>
      <td class="right">${fmtCur(d.revenue)}</td>
      <td class="right">${revBadge}</td>
    </tr>`;
  }).join("");
}

function updateHeader() {
  document.getElementById("date-btn-main").textContent = rangeLabel(applied.mainStart, applied.mainEnd);
  const cmpEl=document.getElementById("date-btn-cmp"), sepEl=document.getElementById("date-btn-sep");
  if (applied.compareOn) {
    cmpEl.textContent="vs "+rangeLabel(applied.cmpStart, applied.cmpEnd);
    cmpEl.style.display=""; sepEl.style.display="";
  } else {
    cmpEl.textContent=""; cmpEl.style.display="none"; sepEl.style.display="none";
  }
}

function renderCards() {
  const cur=aggregate(applied.mainStart, applied.mainEnd);
  const cmp=applied.compareOn ? aggregate(applied.cmpStart, applied.cmpEnd) : null;
  const grid=document.getElementById("card-grid");
  grid.innerHTML="";
  CARDS.forEach(card => {
    const cv=cur?cur[card.key]:null, cp=cmp?cmp[card.key]:null;
    const col=SOURCE_COLORS[card.source]||"#6B7280", lbl=SOURCE_LABELS[card.source]||card.source;
    let valHtml, compareHtml;
    if (cv==null) {
      valHtml='<span class="card-na">N/A</span>'; compareHtml="";
    } else {
      valHtml=`<div class="card-value">${card.fmt(cv)}</div>`;
      const b=badge(cv,cp,card.hib), cmpStr=cp!=null?card.fmt(cp):"N/A";
      compareHtml=`<div class="card-compare"><span class="card-compare-val">vs ${cmpStr}</span>${b}</div>`;
    }
    grid.innerHTML+=`
      <div class="card">
        <div class="card-source"><div class="source-dot" style="background:${col}"></div><span class="source-label">${lbl}</span></div>
        <div class="card-name">${card.name}</div>
        ${valHtml}${compareHtml}
      </div>`;
  });
  document.getElementById("prod-table-title").textContent = "Top Eyewear Products — "+rangeLabel(applied.mainStart, applied.mainEnd);
  renderProductTable();
}

function aggregateAmazon(startD, endD) {
  const s = {};
  AMAZON_CARDS.forEach(c => s[c.key] = null);

  // Amazon daily fields: sum from daily data (works for any date range)
  const keys = DAYS.filter(d => d >= startD && d <= endD);
  let adSpend=0, adSales=0, impressions=0, purchases=0, hasAds=false;
  let grossSales=0, hasGrossSales=false;
  for (const k of keys) {
    const d = DATA[k] || {};
    if ('amz_ad_spend' in d) {
      adSpend     += d.amz_ad_spend    || 0;
      adSales     += d.amz_ad_sales    || 0;
      impressions += d.amz_impressions || 0;
      purchases   += d.amz_purchases   || 0;
      hasAds = true;
    }
    if ('amz_gross_sales' in d) {
      grossSales += d.amz_gross_sales || 0;
      hasGrossSales = true;
    }
  }
  if (hasAds) {
    s.amz_ad_spend    = adSpend;
    s.amz_ad_sales    = adSales;
    s.amz_ad_roas     = adSpend > 0 ? adSales/adSpend : null;
    s.amz_impressions = impressions;
    s.amz_purchases   = purchases;
  }
  if (hasGrossSales) {
    s.amz_gross_sales = grossSales;
    s.amz_tacos       = (hasAds && adSpend > 0 && grossSales > 0) ? adSpend / grossSales : null;
  }

  // Amazon SC: monthly only (API returns monthly totals). Show for any range starting on the 1st.
  const startIsFirst = startD.endsWith("-01");
  if (startIsFirst) {
    let scSales=0, scOrders=0, scUnits=0, scOk=true;
    let ym=startD.slice(0,7), endYM=endD.slice(0,7);
    while (ym <= endYM) {
      const sc = MONTHLY_AMAZON_SC[ym];
      if (sc == null) { scOk=false; break; }
      scSales  += sc.amz_sc_sales  || 0;
      scOrders += sc.amz_sc_orders || 0;
      scUnits  += sc.amz_sc_units  || 0;
      ym = addMonthsYM(ym, 1);
    }
    if (scOk) { s.amz_sc_sales=scSales; s.amz_sc_orders=scOrders; s.amz_sc_units=scUnits; }
  }

  return s;
}

function renderAmazonCards() {
  const cur = aggregateAmazon(applied.mainStart, applied.mainEnd);
  const cmp = applied.compareOn ? aggregateAmazon(applied.cmpStart, applied.cmpEnd) : null;
  const grid = document.getElementById("amazon-card-grid");
  grid.innerHTML = "";
  AMAZON_CARDS.forEach(card => {
    const cv=cur?cur[card.key]:null, cp=cmp?cmp[card.key]:null;
    const col=SOURCE_COLORS.amazon, lbl=SOURCE_LABELS.amazon;
    let valHtml, compareHtml;
    if (cv==null) {
      valHtml='<span class="card-na">N/A</span>'; compareHtml="";
    } else {
      valHtml=`<div class="card-value">${card.fmt(cv)}</div>`;
      const b=badge(cv,cp,card.hib), cmpStr=cp!=null?card.fmt(cp):"N/A";
      compareHtml=`<div class="card-compare"><span class="card-compare-val">vs ${cmpStr}</span>${b}</div>`;
    }
    grid.innerHTML+=`
      <div class="card">
        <div class="card-source"><div class="source-dot" style="background:${col}"></div><span class="source-label">${lbl}</span></div>
        <div class="card-name">${card.name}</div>
        ${valHtml}${compareHtml}
      </div>`;
  });
}

updateHeader();
renderCards();
renderAmazonCards();
</script>
</body>
</html>
"""


# ============================================================
# HTML GENERATOR
# ============================================================

def generate_html(daily: Dict[str, Dict], products: Dict[str, Dict], output: str, monthly_meta_reach: Dict[str, int] = None, monthly_ga4: Dict[str, Dict] = None, monthly_klaviyo: Dict[str, Dict] = None, monthly_amazon_ads: Dict[str, Dict] = None, monthly_amazon_sc: Dict[str, Dict] = None) -> None:
    def clean(obj):
        if isinstance(obj, dict): return {k: clean(v) for k, v in obj.items()}
        if isinstance(obj, list): return [clean(i) for i in obj]
        if isinstance(obj, float) and (obj != obj): return None
        return obj

    data_clean     = {ds: clean(v) for ds, v in daily.items()}
    products_clean = {ds: clean(v) for ds, v in products.items()}
    days_sorted    = sorted(data_clean.keys(), reverse=True)
    now            = dt.datetime.now(ZoneInfo("America/New_York"))
    generated_at   = now.strftime("%B %d, %Y at %I:%M %p ET")
    generated_iso  = now.isoformat(timespec="seconds")  # includes UTC offset so JS new Date() parses correctly
    reach_clean       = monthly_meta_reach or {}
    ga4_clean         = monthly_ga4 or {}
    klaviyo_clean     = monthly_klaviyo or {}
    amazon_ads_clean  = monthly_amazon_ads or {}
    amazon_sc_clean   = monthly_amazon_sc or {}

    html = (
        HTML_TEMPLATE
        .replace("__DATA_JSON__",              json.dumps(data_clean))
        .replace("__DAYS_JSON__",              json.dumps(days_sorted))
        .replace("__PRODUCT_JSON__",           json.dumps(products_clean))
        .replace("__MONTHLY_REACH_JSON__",     json.dumps(reach_clean))
        .replace("__MONTHLY_GA4_JSON__",       json.dumps(ga4_clean))
        .replace("__MONTHLY_KLAVIYO_JSON__",   json.dumps(klaviyo_clean))
        .replace("__MONTHLY_AMAZON_ADS_JSON__",json.dumps(amazon_ads_clean))
        .replace("__MONTHLY_AMAZON_SC_JSON__", json.dumps(amazon_sc_clean))
        .replace("__GENERATED_AT__",           generated_at)
        .replace("__GENERATED_AT_ISO__",       generated_iso)
    )

    with open(output, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\nDone! Dashboard written to: {output}")
    print(f"  Days: {days_sorted[-1]} through {days_sorted[0]} ({len(days_sorted)} days)")


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Generate Tifosi marketing dashboard HTML.")
    parser.add_argument("--months",     type=int, default=HISTORY_MONTHS,
                        help="History depth in months (default 13; use with --full)")
    parser.add_argument("--full",       action="store_true",
                        help="Fetch full history from scratch (required on first run)")
    parser.add_argument("--cache-only", action="store_true",
                        help="Regenerate HTML from cache without any API calls")
    parser.add_argument("--create-reports-only", action="store_true",
                        help="Create Amazon Ads async reports and save IDs to history file — no dashboard generation")
    parser.add_argument("--output",     default=OUTPUT_HTML, help="Output HTML path")
    args = parser.parse_args()

    # ── Cache-only mode ──────────────────────────────────────
    if args.cache_only:
        cache = load_cache()
        if not cache or "daily" not in cache:
            print("ERROR: No valid daily cache found. Run without --cache-only first.")
            return
        print(f"[cache] Loaded {len(cache['daily'])} days.")
        generate_html(cache["daily"], cache.get("products", {}), args.output, cache.get("monthly_meta_reach", {}), cache.get("monthly_ga4", {}), cache.get("monthly_klaviyo", {}), cache.get("monthly_amazon_ads", {}), cache.get("monthly_amazon_sc", {}))
        return

    # ── Create-reports-only mode (runs at 6 AM ET via separate cron) ────────
    if args.create_reports_only:
        end   = dt.date.today()
        start = end - dt.timedelta(days=REFRESH_DAYS - 1)
        print(f"\n[Amazon Ads] Creating reports for {start} to {end}...")
        new_specs = create_amazon_ads_reports(start, end)
        if new_specs:
            if os.path.exists(_AMZ_HISTORY_FILE):
                with open(_AMZ_HISTORY_FILE) as f:
                    hist = json.load(f)
            else:
                hist = {}
            # Append (don't replace) so any prior still-pending IDs are preserved
            existing = hist.get("pending_amazon_reports", [])
            existing_ids = {s["reportId"] for s in existing}
            added = [s for s in new_specs if s["reportId"] not in existing_ids]
            hist["pending_amazon_reports"] = existing + added
            with open(_AMZ_HISTORY_FILE, "w") as f:
                json.dump(hist, f, indent=2, default=str)
            print(f"[Amazon Ads] {len(added)} new report specs saved. "
                  f"{len(hist['pending_amazon_reports'])} total pending.")
        else:
            print("[Amazon Ads] No reports created (credentials missing or API error).")
        return

    cache             = load_cache()
    existing_daily    = cache.get("daily",    {}) if cache else {}
    existing_products = cache.get("products", {}) if cache else {}

    # ── Full history mode ─────────────────────────────────────
    if args.full or not existing_daily:
        end   = dt.date.today()
        start = history_start(args.months)
        print(f"\nFull history fetch: {start} to {end} ({(end-start).days+1} days)...\n")

        shopify = fetch_shopify(start, end, batch_by_month=True)
        meta    = fetch_meta(start, end)
        google  = fetch_google(start, end)
        ga4     = fetch_ga4(start, end)
        msads   = fetch_msads(start, end)
        reddit  = fetch_reddit(start, end)
        amz_ads_daily = fetch_amazon_ads(start, end)

        new_daily       = merge_daily(start, end, shopify, meta, google, ga4, msads, reddit, amz_ads_daily)
        new_products    = fetch_shopify_products(start, end, batch_by_month=True)
        monthly_reach   = fetch_meta_monthly_reach(start, end)
        monthly_ga4     = fetch_ga4_monthly(start, end)
        monthly_klaviyo = fetch_klaviyo_monthly(start, end)
        monthly_amazon_sc = fetch_amazon_sc_monthly(start, end)
        save_cache({"daily": new_daily, "products": new_products, "monthly_meta_reach": monthly_reach, "monthly_ga4": monthly_ga4, "monthly_klaviyo": monthly_klaviyo, "monthly_amazon_ads": {}, "monthly_amazon_sc": monthly_amazon_sc})
        generate_html(new_daily, new_products, args.output, monthly_reach, monthly_ga4, monthly_klaviyo, {}, monthly_amazon_sc)
        return

    # ── Daily refresh mode (default) ─────────────────────────
    end   = dt.date.today()
    start = end - dt.timedelta(days=REFRESH_DAYS - 1)
    print(f"\nRefreshing last {REFRESH_DAYS} days ({start} to {end})...\n")

    # Phase 1: Download any Amazon Ads reports created on the previous run.
    # Reports are created at end of each run and typically complete within minutes,
    # so by the next daily run (24h later) they are always ready.
    existing_pending = []
    if os.path.exists(_AMZ_HISTORY_FILE):
        try:
            with open(_AMZ_HISTORY_FILE) as f:
                _h = json.load(f)
            existing_pending = _h.get("pending_amazon_reports", [])
        except Exception:
            pass
    amz_ads_daily: Dict[str, Dict] = {}
    still_pending: List[Dict] = []
    if existing_pending:
        print(f"[Amazon Ads] Found {len(existing_pending)} pending reports from previous run...")
        amz_ads_daily, still_pending = download_amazon_ads_reports(existing_pending)
        # Write still-pending IDs back so next create-reports run doesn't re-create them
        if os.path.exists(_AMZ_HISTORY_FILE):
            with open(_AMZ_HISTORY_FILE) as f:
                _hist = json.load(f)
        else:
            _hist = {}
        _hist["pending_amazon_reports"] = still_pending
        with open(_AMZ_HISTORY_FILE, "w") as f:
            json.dump(_hist, f, indent=2, default=str)
    else:
        print("[Amazon Ads] No pending reports — skipping download phase.")

    shopify = fetch_shopify(start, end, batch_by_month=False)
    meta    = fetch_meta(start, end)
    google  = fetch_google(start, end)
    ga4     = fetch_ga4(start, end)
    msads   = fetch_msads(start, end)
    reddit  = fetch_reddit(start, end)

    refreshed          = merge_daily(start, end, shopify, meta, google, ga4, msads, reddit, amz_ads_daily)
    refreshed_products = fetch_shopify_products(start, end)
    # Field-level merge: preserve existing fields (e.g. amz_ad_spend from history)
    # when the current fetch didn't return data for that source.
    for ds, new_row in refreshed.items():
        if ds not in existing_daily:
            existing_daily[ds] = new_row
        else:
            existing_daily[ds].update(new_row)
    existing_products.update(refreshed_products)

    existing_reach = cache.get("monthly_meta_reach", {}) if cache else {}
    new_reach      = fetch_meta_monthly_reach(start, end)
    existing_reach.update(new_reach)

    existing_ga4m  = cache.get("monthly_ga4", {}) if cache else {}
    new_ga4m       = fetch_ga4_monthly(start, end)
    existing_ga4m.update(new_ga4m)

    existing_klav  = cache.get("monthly_klaviyo", {}) if cache else {}
    new_klav       = fetch_klaviyo_monthly(start, end)
    existing_klav.update(new_klav)

    existing_amz_sc = cache.get("monthly_amazon_sc", {}) if cache else {}
    new_amz_sc      = fetch_amazon_sc_monthly(history_start(args.months), dt.date.today())
    existing_amz_sc.update(new_amz_sc)

    new_gross = fetch_amazon_sc_daily_gross_sales(history_start(args.months), dt.date.today())
    for ds, amount in new_gross.items():
        if ds not in existing_daily:
            existing_daily[ds] = {}
        existing_daily[ds]["amz_gross_sales"] = amount

    save_cache({"daily": existing_daily, "products": existing_products, "monthly_meta_reach": existing_reach, "monthly_ga4": existing_ga4m, "monthly_klaviyo": existing_klav, "monthly_amazon_ads": {}, "monthly_amazon_sc": existing_amz_sc})
    generate_html(existing_daily, existing_products, args.output, existing_reach, existing_ga4m, existing_klav, {}, existing_amz_sc)


if __name__ == "__main__":
    main()
