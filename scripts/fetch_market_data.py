#!/usr/bin/env python3
"""
Fnikar — Market Data Fetcher
Runs via GitHub Actions (or locally) to fetch complete stock fundamentals
using yfinance (server-side, no CORS issues).
Output: market_data.json (repo root)

Symbol Discovery (in priority order):
  1. Firestore — reads every user's portfolio and collects all unique symbols.
     Requires FIREBASE_SERVICE_ACCOUNT env var (JSON string of service account key).
  2. Fallback   — uses the hardcoded FALLBACK_STOCKS dict below.
"""

import yfinance as yf
import json
import time
import os
from datetime import datetime

# ── YAHOO OVERRIDES ───────────────────────────────────────────────────────────
YAHOO_OVERRIDES = {
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
    "RELIANCEP1": "RELIANCE.NS",
    "UPLPP1":     "UPL.NS",
    "AIRTELPP":   "AIRTELPP.NS",
    "IL&FSENGG":  "ILFSENGG.NS",
    "TMPV":       "TATAMTRDVR.NS",
    "GATEWAY":    "GDL.NS",
}

# ── FALLBACK SYMBOL LIST (used when Firestore credentials not available) ──────
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


def resolve_yahoo_symbol(symbol):
    if symbol in YAHOO_OVERRIDES:
        return YAHOO_OVERRIDES[symbol]
    if symbol.isdigit():
        return symbol + ".BO"
    return symbol + ".NS"


def get_symbols_from_firestore():
    sa_json = os.environ.get("FIREBASE_SERVICE_ACCOUNT")
    if not sa_json:
        return None
    try:
        import firebase_admin
        from firebase_admin import credentials, firestore as fs
        if not firebase_admin._apps:
            sa_dict = json.loads(sa_json)
            cred = credentials.Certificate(sa_dict)
            firebase_admin.initialize_app(cred)
        db = fs.client()
        symbols = set()
        for portfolio_doc in db.collection("portfolios").stream():
            uid = portfolio_doc.id
            for stock_doc in db.collection("portfolios").document(uid).collection("stocks").stream():
                sym = (stock_doc.to_dict().get("symbol") or "").strip().upper()
                if sym:
                    symbols.add(sym)
        if not symbols:
            print("  Warning: Firestore returned 0 symbols, using fallback list.")
            return None
        print(f"  Found {len(symbols)} unique symbols across all Firestore portfolios.")
        return {sym: resolve_yahoo_symbol(sym) for sym in sorted(symbols)}
    except Exception as e:
        print(f"  Firestore error: {e}")
        return None


def safe_float(val, decimals=2):
    try:
        if val is None or (isinstance(val, float) and str(val) in ("nan", "inf", "-inf")):
            return None
        return round(float(val), decimals)
    except Exception:
        return None

def safe_int(val):
    try:
        return None if val is None else int(val)
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
    if k == "strong_buy":              return "\u25b2 Strong Buy"
    if k == "buy"     and d >= 0:      return "\u25b2 Bullish"
    if k == "buy":                     return "\u25b2 Buy"
    if k in ("sell", "strong_sell"):   return "\u25bc Bearish"
    if k == "underperform" and d < -5: return "\u25bc Weak"
    if d >  5:                         return "\u25b2 Above 50DMA"
    if d < -5:                         return "\u25bc Below 50DMA"
    return "\u25c4 Neutral"


def fetch_stock(symbol, yahoo_sym):
    try:
        t    = yf.Ticker(yahoo_sym)
        info = t.info or {}
        price = info.get("regularMarketPrice") or info.get("currentPrice")
        if not price:
            return {"available": False, "reason": "no_price"}
        pe        = safe_float(info.get("trailingPE"))
        industry  = info.get("industry") or info.get("sector")
        name      = info.get("longName") or info.get("shortName") or symbol
        rec_key   = info.get("recommendationKey") or ""
        rec       = rec_key_to_label(rec_key)
        fifty_dma = info.get("fiftyDayAverage")
        vs_50dma  = safe_float((price - fifty_dma) / fifty_dma * 100) if fifty_dma else None
        signal    = derive_signal(rec_key, vs_50dma)
        raw_52w   = info.get("52WeekChange")
        change_52w = safe_float(raw_52w * 100) if raw_52w is not None else None
        buy_recs = sell_recs = None
        try:
            recs_df = t.recommendations
            if recs_df is not None and not recs_df.empty:
                row = recs_df.iloc[0]
                buy_recs  = safe_int((row.get("strongBuy") or 0)  + (row.get("buy")       or 0))
                sell_recs = safe_int((row.get("strongSell") or 0) + (row.get("sell")      or 0))
        except Exception:
            n    = info.get("numberOfAnalystOpinions")
            mean = info.get("recommendationMean")
            if n and mean:
                buy_recs  = round(n * max(0.0, 1.0 - (mean - 1) / 4))
                sell_recs = round(n * max(0.0, (mean - 1) / 4 * 0.5))
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


def main():
    print(f"Fnikar Market Data Fetch — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    print("\nDiscovering symbols...")
    stocks = get_symbols_from_firestore()
    if stocks:
        print(f"  Source: Firestore ({len(stocks)} symbols)\n")
    else:
        stocks = FALLBACK_STOCKS
        print(f"  Source: Hardcoded fallback ({len(stocks)} symbols)\n")

    print(f"Fetching data for {len(stocks)} symbols...\n")
    result   = {"generatedAt": datetime.now().isoformat(), "stocks": {}}
    ok_count = 0

    for i, (symbol, yahoo_sym) in enumerate(stocks.items(), 1):
        print(f"  [{i:3}/{len(stocks)}] {symbol:15} ({yahoo_sym}) ... ", end="", flush=True)
        data = fetch_stock(symbol, yahoo_sym)
        result["stocks"][symbol] = data
        if data.get("available"):
            ok_count += 1
            print(f"OK  price={data.get('price')}  {(data.get('industry') or '')[:28]}")
        else:
            print(f"SKIP  {data.get('reason', '')}")
        time.sleep(0.4)

    # Write to repo root (script lives in scripts/, so go one level up)
    out_path = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "market_data.json"))
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2, default=str)

    print(f"\n{'=' * 60}")
    print(f"Done — {ok_count}/{len(stocks)} stocks fetched.")
    print(f"Written to {out_path}")

if __name__ == "__main__":
    main()
