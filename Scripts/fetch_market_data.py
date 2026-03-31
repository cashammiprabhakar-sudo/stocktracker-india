#!/usr/bin/env python3
"""
Fnikar — Market Data Fetcher
Runs via GitHub Actions (or locally) to fetch complete stock fundamentals
using yfinance (server-side, no CORS issues).
Output: public/market_data.json

Symbol Discovery (in priority order):
  1. Firestore — reads every user's portfolio and collects all unique symbols.
     Requires FIREBASE_SERVICE_ACCOUNT env var (JSON string of service account key).
  2. Fallback   — uses the hardcoded FALLBACK_STOCKS dict below.
     Safe for local dev / first-run before Firestore credentials are configured.
"""

import yfinance as yf
import json
import time
import sys
import os
from datetime import datetime

# ── YAHOO OVERRIDES ───────────────────────────────────────────────────────────
# Maps portfolio symbols → Yahoo Finance tickers where they differ.
# Add any new special cases here (numeric BSE codes, renamed tickers, etc.)
YAHOO_OVERRIDES = {
    # Numeric BSE codes
    "532281": "HCLTECH.NS",
    "524715": "SUNPHARMA.NS",
    "533248": "GPPL.NS",
    "538567": "GULFOILLUB.NS",
    "538666": "SHARDACROP.NS",
    "513262": "SSWL.NS",
    "540124": "GNA.NS",
    "543300": "SONACOMS.NS",
    "544289": "NTPCGREEN.NS",
    "500285": "SPICEJET.NS",
    "500314": "ORIENTHOT.NS",
    "500400": "TATAPOWER.NS",
    "100312": "ONGC.NS",
    "100113": "SAIL.NS",
    "532699": "ROYALHOTEL.NS",
    "531340": "531340.BO",
    # Rights entitlements / special symbols
    "RELIANCEP1": "RELIANCE.NS",
    "UPLPP1":     "UPL.NS",
    "AIRTELPP":   "AIRTELPP.NS",
    # Renamed / aliased tickers
    "IL&FSENGG":  "ILFSENGG.NS",
    "TMPV":       "TATAMTRDVR.NS",
    "GATEWAY":    "GDL.NS",
}

# ── FALLBACK SYMBOL LIST ──────────────────────────────────────────────────────
# Used when Firestore credentials are not available (local dev / first run).
# Keys = portfolio symbol, Values = Yahoo Finance ticker.
FALLBACK_STOCKS = {
    "ADANIPORTS": "ADANIPORTS.NS",
    "ADANIGREEN": "ADANIGREEN.NS",
    "IMAGICAA":   "IMAGICAA.NS",
    "ADVANIHOTR": "ADVANIHOTR.NS",
    "AEGISLOG":   "AEGISLOG.NS",
    "ALOKINDS":   "ALOKINDS.NS",
    "ARVIND":     "ARVIND.NS",
    "AXISBANK":   "AXISBANK.NS",
    "BAJAJHIND":  "BAJAJHIND.NS",
    "BANDHANBNK": "BANDHANBNK.NS",
    "BANKBARODA": "BANKBARODA.NS",
    "BANKINDIA":  "BANKINDIA.NS",
    "UFBL":       "UFBL.NS",
    "531340":     "531340.BO",
    "AIRTELPP":   "AIRTELPP.NS",
    "BIKAJI":     "BIKAJI.NS",
    "ZYDUSLIFE":  "ZYDUSLIFE.NS",
    "CANBK":      "CANBK.NS",
    "CASTROLIND": "CASTROLIND.NS",
    "COALINDIA":  "COALINDIA.NS",
    "COCHINSHIP": "COCHINSHIP.NS",
    "DAMCAPITAL": "DAMCAPITAL.NS",
    "DCW":        "DCW.NS",
    "DREAMFOLKS": "DREAMFOLKS.NS",
    "DWARKESH":   "DWARKESH.NS",
    "EKC":        "EKC.NS",
    "FEDERALBNK": "FEDERALBNK.NS",
    "NYKAA":      "NYKAA.NS",
    "FRETAIL":    "FRETAIL.NS",
    "GAIL":       "GAIL.NS",
    "GATEWAY":    "GDL.NS",
    "GESHIP":     "GESHIP.NS",
    "GMRAIRPORT": "GMRAIRPORT.NS",
    "540124":     "GNA.NS",
    "533248":     "GPPL.NS",
    "538567":     "GULFOILLUB.NS",
    "532281":     "HCLTECH.NS",
    "HDFCBANK":   "HDFCBANK.NS",
    "HEROMOTOCO": "HEROMOTOCO.NS",
    "HAL":        "HAL.NS",
    "HINDMOTORS": "HINDMOTORS.NS",
    "HYUNDAI":    "HYUNDAI.NS",
    "ICICIBANK":  "ICICIBANK.NS",
    "IDEA":       "IDEA.NS",
    "IL&FSENGG":  "ILFSENGG.NS",
    "INDUSINDBK": "INDUSINDBK.NS",
    "INDIANB":    "INDIANB.NS",
    "IOC":        "IOC.NS",
    "IPL":        "IPL.NS",
    "IRCTC":      "IRCTC.NS",
    "NAUKRI":     "NAUKRI.NS",
    "INFY":       "INFY.NS",
    "ITC":        "ITC.NS",
    "ITCHOTELS":  "ITCHOTELS.NS",
    "JPPOWER":    "JPPOWER.NS",
    "JAMNAAUTO":  "JAMNAAUTO.NS",
    "JETAIRWAYS": "JETAIRWAYS.NS",
    "JIOFIN":     "JIOFIN.NS",
    "JUSTDIAL":   "JUSTDIAL.NS",
    "KPIL":       "KPIL.NS",
    "LEMONTREE":  "LEMONTREE.NS",
    "LICI":       "LICI.NS",
    "LTFOODS":    "LTFOODS.NS",
    "LUPIN":      "LUPIN.NS",
    "POONAWALLA": "POONAWALLA.NS",
    "MAWANASUG":  "MAWANASUG.NS",
    "MAXHEALTH":  "MAXHEALTH.NS",
    "MAZDOCK":    "MAZDOCK.NS",
    "MOIL":       "MOIL.NS",
    "MSTCLTD":    "MSTCLTD.NS",
    "MUKTAARTS":  "MUKTAARTS.NS",
    "NSLNISP":    "NSLNISP.NS",
    "NDTV":       "NDTV.NS",
    "NTPC":       "NTPC.NS",
    "544289":     "NTPCGREEN.NS",
    "PAYTM":      "PAYTM.NS",
    "100312":     "ONGC.NS",
    "500314":     "ORIENTHOT.NS",
    "PETRONET":   "PETRONET.NS",
    "PNCINFRA":   "PNCINFRA.NS",
    "PFC":        "PFC.NS",
    "PRAJIND":    "PRAJIND.NS",
    "PNB":        "PNB.NS",
    "PVRINOX":    "PVRINOX.NS",
    "RAJESHEXPO": "RAJESHEXPO.NS",
    "MID150BEES": "MID150BEES.NS",
    "RCOM":       "RCOM.NS",
    "RELIGARE":   "RELIGARE.NS",
    "RELIANCEP1": "RELIANCE.NS",
    "RPOWER":     "RPOWER.NS",
    "RENUKA":     "RENUKA.NS",
    "532699":     "ROYALHOTEL.NS",
    "REC":        "REC.NS",
    "100113":     "SAIL.NS",
    "SBISILVER":  "SBISILVER.NS",
    "538666":     "SHARDACROP.NS",
    "543300":     "SONACOMS.NS",
    "500285":     "SPICEJET.NS",
    "SBIN":       "SBIN.NS",
    "513262":     "SSWL.NS",
    "524715":     "SUNPHARMA.NS",
    "SUNTV":      "SUNTV.NS",
    "TMPV":       "TATAMTRDVR.NS",
    "500400":     "TATAPOWER.NS",
    "TATASTEEL":  "TATASTEEL.NS",
    "TATATECH":   "TATATECH.NS",
    "TCS":        "TCS.NS",
    "TRIDENT":    "TRIDENT.NS",
    "UNIONBANK":  "UNIONBANK.NS",
    "UPLPP1":     "UPL.NS",
    "VBL":        "VBL.NS",
    "VTL":        "VTL.NS",
    "VEDL":       "VEDL.NS",
    "WIPRO":      "WIPRO.NS",
    "YESBANK":    "YESBANK.NS",
    "ETERNAL":    "ETERNAL.NS",
}


# ── SYMBOL RESOLUTION ─────────────────────────────────────────────────────────
def resolve_yahoo_symbol(symbol):
    """
    Maps a portfolio symbol to its Yahoo Finance ticker.
    Priority: YAHOO_OVERRIDES → append .NS (default for Indian stocks)
    """
    if symbol in YAHOO_OVERRIDES:
        return YAHOO_OVERRIDES[symbol]
    # Pure numeric → BSE code
    if symbol.isdigit():
        return symbol + ".BO"
    return symbol + ".NS"


# ── FIRESTORE SYMBOL DISCOVERY ────────────────────────────────────────────────
def get_symbols_from_firestore():
    """
    Connects to Firestore using FIREBASE_SERVICE_ACCOUNT env var (JSON string).
    Reads every user's portfolio and returns a dict {symbol: yahoo_ticker}.
    Returns None if credentials are not available.
    """
    sa_json = os.environ.get("FIREBASE_SERVICE_ACCOUNT")
    if not sa_json:
        return None

    try:
        import firebase_admin
        from firebase_admin import credentials, firestore as fs

        # Init only once
        if not firebase_admin._apps:
            sa_dict = json.loads(sa_json)
            cred = credentials.Certificate(sa_dict)
            firebase_admin.initialize_app(cred)

        db = fs.client()

        # Collect all unique symbols across all users' portfolios
        symbols = set()
        portfolios = db.collection("portfolios").stream()
        for portfolio_doc in portfolios:
            uid = portfolio_doc.id
            stocks_ref = db.collection("portfolios").document(uid).collection("stocks")
            for stock_doc in stocks_ref.stream():
                data = stock_doc.to_dict()
                sym = data.get("symbol", "").strip().upper()
                if sym:
                    symbols.add(sym)

        if not symbols:
            print("  ⚠ Firestore returned 0 symbols — falling back to hardcoded list.")
            return None

        print(f"  ✓ Found {len(symbols)} unique symbols across all Firestore portfolios.")
        return {sym: resolve_yahoo_symbol(sym) for sym in sorted(symbols)}

    except Exception as e:
        print(f"  ✗ Firestore error: {e}")
        return None


# ── HELPERS ───────────────────────────────────────────────────────────────────
def safe_float(val, decimals=2):
    try:
        if val is None or (isinstance(val, float) and str(val) in ("nan", "inf", "-inf")):
            return None
        return round(float(val), decimals)
    except Exception:
        return None

def safe_int(val):
    try:
        if val is None:
            return None
        return int(val)
    except Exception:
        return None

def rec_key_to_label(key):
    k = (key or "").lower()
    if k in ("strong_buy", "buy"):                   return "BUY"
    if k in ("hold", "neutral"):                     return "HOLD"
    if k in ("underperform", "sell", "strong_sell"): return "SELL"
    return None

def derive_signal(rec_key, vs_50dma):
    k = (rec_key or "").lower()
    d = vs_50dma or 0
    if k == "strong_buy":                 return "▲ Strong Buy"
    if k == "buy"     and d >= 0:         return "▲ Bullish"
    if k == "buy":                        return "▲ Buy"
    if k in ("sell", "strong_sell"):      return "▼ Bearish"
    if k == "underperform" and d < -5:    return "▼ Weak"
    if d >  5:                            return "▲ Above 50DMA"
    if d < -5:                            return "▼ Below 50DMA"
    return "◀ Neutral"


# ── FETCH ONE STOCK ───────────────────────────────────────────────────────────
def fetch_stock(symbol, yahoo_sym):
    try:
        t    = yf.Ticker(yahoo_sym)
        info = t.info or {}

        price = info.get("regularMarketPrice") or info.get("currentPrice")
        if not price:
            return {"available": False, "reason": "no_price"}

        # P/E
        pe = safe_float(info.get("trailingPE"))

        # Industry / sector
        industry = info.get("industry") or info.get("sector")

        # Company name
        name = info.get("longName") or info.get("shortName") or symbol

        # Recommendation key → label
        rec_key = info.get("recommendationKey") or ""
        rec     = rec_key_to_label(rec_key)

        # 50-DMA percentage delta (for signal)
        fifty_dma = info.get("fiftyDayAverage")
        vs_50dma  = safe_float((price - fifty_dma) / fifty_dma * 100) if fifty_dma else None

        signal = derive_signal(rec_key, vs_50dma)

        # 52-week change %
        change_52w = safe_float(
            info.get("52WeekChange", None) and info["52WeekChange"] * 100
        )

        # Analyst recommendation counts
        buy_recs = sell_recs = None
        try:
            recs_df = t.recommendations
            if recs_df is not None and not recs_df.empty:
                row = recs_df.iloc[0]
                buy_recs  = safe_int((row.get("strongBuy")  or 0) + (row.get("buy")       or 0))
                sell_recs = safe_int((row.get("strongSell") or 0) + (row.get("sell")      or 0))
        except Exception:
            # Fallback: estimate from mean rating + analyst count
            n    = info.get("numberOfAnalystOpinions")
            mean = info.get("recommendationMean")
            if n and mean:
                pct_buy  = max(0.0, 1.0 - (mean - 1) / 4)
                pct_sell = max(0.0, (mean - 1) / 4 * 0.5)
                buy_recs  = round(n * pct_buy)
                sell_recs = round(n * pct_sell)

        return {
            "available":      True,
            "name":           name,
            "industry":       industry,
            "price":          safe_float(price),
            "pe":             pe,
            "recommendation": rec,
            "signal":         signal,
            "buyRecs":        buy_recs,
            "sellRecs":       sell_recs,
            "targetPrice":    safe_float(info.get("targetMeanPrice")),
            "change52w":      change_52w,
            "marketCap":      info.get("marketCap"),
        }

    except Exception as e:
        return {"available": False, "reason": str(e)}


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    print(f"Fnikar Market Data Fetch — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    # ── Step 1: Discover symbols ──────────────────────────────────────────────
    print("\n📋 Discovering symbols…")
    stocks = get_symbols_from_firestore()

    if stocks:
        print(f"   Source: Firestore ({len(stocks)} symbols)\n")
    else:
        stocks = FALLBACK_STOCKS
        print(f"   Source: Hardcoded fallback ({len(stocks)} symbols)\n")
        print("   💡 Set FIREBASE_SERVICE_ACCOUNT secret in GitHub to enable")
        print("      automatic symbol discovery for all users.\n")

    # ── Step 2: Fetch market data ─────────────────────────────────────────────
    print(f"📡 Fetching data for {len(stocks)} symbols…\n")
    result   = {"generatedAt": datetime.now().isoformat(), "stocks": {}}
    ok_count = 0

    for i, (symbol, yahoo_sym) in enumerate(stocks.items(), 1):
        print(f"  [{i:3}/{len(stocks)}] {symbol:15} ({yahoo_sym}) … ", end="", flush=True)
        data = fetch_stock(symbol, yahoo_sym)
        result["stocks"][symbol] = data
        if data.get("available"):
            ok_count += 1
            price = data.get("price", "?")
            ind   = (data.get("industry") or "—")[:28]
            print(f"✓  ₹{price:>10}   {ind}")
        else:
            print(f"✗  {data.get('reason', '')}")
        time.sleep(0.4)   # gentle rate-limiting — avoids Yahoo Finance blocks

    # ── Step 3: Write output ──────────────────────────────────────────────────
    # Output path: one level up from scripts/ → market_data.json at repo root
    out_path = os.path.join(os.path.dirname(__file__), "..", "market_data.json")
    out_path = os.path.normpath(out_path)
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2, default=str)

    print(f"\n{'=' * 60}")
    print(f"✅ Done — {ok_count}/{len(stocks)} stocks fetched successfully.")
    print(f"   Written to {out_path}")

if __name__ == "__main__":
    main()
