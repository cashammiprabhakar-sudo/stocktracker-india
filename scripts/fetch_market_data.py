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
# Maps portfolio symbols → Yahoo Finance tickers where they differ from SYMBOL.NS
YAHOO_OVERRIDES = {
    "532281":     "HCLTECH.NS",
    "524715":     "SUNPHARMA.NS",
    "533248":     "GPPL.NS",
    "538567":     "GULFOILLUB.NS",
    "538666":     "SHARDACROP.NS",
    "513262":     "SSWL.NS",
    "540124":     "GNA.NS",
    "543300":     "SONACOMS.NS",
    "544289":     "NTPCGREEN.NS",
    "500285":     "SPICEJET.NS",
    "500314":     "ORIENTHOT.NS",
    "500400":     "TATAPOWER.NS",
    "100312":     "ONGC.NS",
    "100113":     "SAIL.NS",
    "532699":     "ROYALHOTEL.NS",
    "531340":     "531340.BO",
    "RELIANCEP1": "RELIANCE.NS",
    "UPLPP1":     "UPL.NS",
    "AIRTELPP":   "BHARTIARTL.NS",   # rights entitlement → parent stock
    "IL&FSENGG":  "ILFSENGG.NS",
    "TMPV":       "TATAMTRDVR.NS",
    "GATEWAY":    "GDL.NS",
    "REC":        "RECLTD.NS",        # Yahoo Finance uses RECLTD not REC
}

# ── FALLBACK SYMBOL LIST ──────────────────────────────────────────────────────
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
    "AIRTELPP":   "BHARTIARTL.NS",
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
    "REC":        "RECLTD.NS",
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
    # ── Added manually (new portfolio stocks not yet auto-discovered via Firestore) ──
    "AMARAJABAT": "AMARAJABAT.NS",
    "BERGEPAINT": "BERGEPAINT.NS",
}


# ── SYMBOL RESOLUTION ─────────────────────────────────────────────────────────
def resolve_yahoo_symbol(symbol):
    if symbol in YAHOO_OVERRIDES:
        return YAHOO_OVERRIDES[symbol]
    if symbol.isdigit():
        return symbol + ".BO"
    return symbol + ".NS"


# ── FIRESTORE SYMBOL DISCOVERY ────────────────────────────────────────────────
def get_symbols_from_firestore():
    """Returns dict of {symbol: yahoo_sym} discovered from Firestore, or empty dict on failure."""
    sa_json = os.environ.get("FIREBASE_SERVICE_ACCOUNT")
    if not sa_json:
        print("  FIREBASE_SERVICE_ACCOUNT secret not set — Firestore discovery skipped.")
        print("  ► To enable auto-discovery of new stocks, add FIREBASE_SERVICE_ACCOUNT")
        print("    as a GitHub Actions secret (Settings → Secrets → Actions).")
        return {}
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
            print("  Warning: Firestore portfolios collection is empty.")
            return {}
        print(f"  Firestore: found {len(symbols)} unique symbols across all portfolios.")
        return {sym: resolve_yahoo_symbol(sym) for sym in sorted(symbols)}
    except Exception as e:
        print(f"  Firestore error ({type(e).__name__}): {e}")
        print("  ► Check that the service account has 'Cloud Datastore User' role in GCP.")
        return {}


# ── COMMENTARY GENERATOR ──────────────────────────────────────────────────────
def generate_commentary(name, industry, rec, pe, change_52w, rec_key, vs_50dma):
    """
    Generates a 2-3 sentence investment commentary combining:
    - Stock's own performance (52w change, valuation)
    - Macro theme relevant to its industry (Iran war, AI penetration, etc.)
    - Analyst recommendation context
    """
    parts = []
    ind = (industry or "").lower()

    # ── 1. Performance context ────────────────────────────────────────────────
    if change_52w is not None:
        if change_52w > 30:
            parts.append(f"{name} has been a strong performer, delivering {change_52w:.1f}% returns over the past year and significantly outpacing the broader market.")
        elif change_52w > 10:
            parts.append(f"{name} has posted healthy gains of {change_52w:.1f}% over the past year, broadly in line with or ahead of the Nifty.")
        elif change_52w > -10:
            parts.append(f"{name} has traded largely sideways over the past year, returning {change_52w:+.1f}%, reflecting a period of consolidation.")
        elif change_52w > -30:
            parts.append(f"{name} has underperformed over the past year, declining {abs(change_52w):.1f}%, which may reflect sector headwinds or company-specific challenges.")
        else:
            parts.append(f"{name} has faced significant pressure, falling {abs(change_52w):.1f}% over the past year — a sharp underperformance that warrants close scrutiny of fundamentals.")

    # ── 2. Valuation context ──────────────────────────────────────────────────
    if pe is not None and pe > 0:
        if pe < 8:
            parts.append(f"At a trailing P/E of just {pe:.1f}x, the stock is trading at a deep discount, suggesting the market may be pricing in persistent earnings risk or it is significantly undervalued.")
        elif pe < 15:
            parts.append(f"A trailing P/E of {pe:.1f}x makes the stock attractively valued relative to most peers, offering a reasonable margin of safety.")
        elif pe < 25:
            parts.append(f"Trading at {pe:.1f}x trailing earnings, the valuation is fair and reflects steady but not spectacular growth expectations.")
        elif pe < 45:
            parts.append(f"The premium valuation of {pe:.1f}x P/E suggests the market is pricing in strong earnings growth ahead — execution will be key to sustaining this multiple.")
        else:
            parts.append(f"At {pe:.1f}x trailing P/E, the stock carries a rich valuation that leaves little room for earnings disappointment.")

    # ── 3. Macro theme by industry ────────────────────────────────────────────

    # Oil, Gas, Energy — Iran war tailwind
    if any(x in ind for x in ["oil", "gas", "energy", "petro", "refin", "lpg"]):
        parts.append(
            "Geopolitical tensions in the Middle East — particularly escalating Iran-related risks — "
            "are keeping Brent crude elevated above $85/bbl, a direct tailwind for upstream E&P "
            "and oilfield services companies. Downstream refiners face margin compression from high "
            "feedstock costs, though marketing margins on petrol and diesel provide a partial buffer."
        )

    # Aviation / Logistics — Iran war headwind
    elif any(x in ind for x in ["aviation", "airline", "airport"]):
        parts.append(
            "Elevated crude oil prices driven by Middle East geopolitical risk are a meaningful headwind "
            "for aviation, where fuel accounts for 25-35% of operating costs. However, strong domestic "
            "air travel demand post-pandemic and improving load factors help offset the fuel cost pressure."
        )

    elif any(x in ind for x in ["logistics", "shipping", "freight", "transport"]):
        parts.append(
            "Middle East tensions are disrupting Red Sea shipping routes, pushing freight rates higher "
            "and benefiting Indian shipping and port companies. Longer voyage distances are increasing "
            "ton-mile demand, supporting fleet utilisation and charter rates."
        )

    # IT / Software — AI penetration
    elif any(x in ind for x in ["software", "information tech", "it service", "computer", "tech"]):
        parts.append(
            "AI penetration is the defining theme for Indian IT — large-cap players are investing "
            "heavily in GenAI service lines and proprietary platforms to capture the next wave of "
            "enterprise digital transformation spend. Companies that successfully transition from "
            "traditional outsourcing to AI-augmented delivery will command superior margin profiles "
            "and higher deal win rates over the medium term."
        )

    # Banking / Finance — AI + rate cycle
    elif any(x in ind for x in ["bank", "banking", "finance", "nbfc", "financial service", "insurance", "capital market"]):
        parts.append(
            "Indian banks are deploying AI across credit underwriting, fraud detection, and hyper-personalised "
            "cross-selling, meaningfully reducing cost-to-income ratios. With the RBI beginning its rate "
            "easing cycle, NIM pressure may emerge near-term, but improving asset quality and "
            "AI-driven operating leverage support a constructive medium-term outlook."
        )

    # Pharma / Healthcare — AI drug discovery
    elif any(x in ind for x in ["pharma", "health", "hospital", "medical", "drug", "biotech"]):
        parts.append(
            "AI-driven drug discovery is compressing R&D timelines and costs for Indian pharma companies, "
            "while strong US generics demand and a healthy domestic prescription market provide dual "
            "revenue engines. India's CDMO opportunity is also expanding as global innovators diversify "
            "manufacturing away from China."
        )

    # Telecom — 5G + AI
    elif any(x in ind for x in ["telecom", "communication", "wireless"]):
        parts.append(
            "5G monetisation and AI-powered network management are the key re-rating triggers for Indian "
            "telecom. The sector's consolidation into a two-and-a-half player market has restored pricing "
            "discipline, with ARPU expansion likely as users migrate to premium data plans. Enterprise "
            "5G use cases in manufacturing and logistics add a longer-term growth layer."
        )

    # Auto — EV transition + input costs
    elif any(x in ind for x in ["auto", "vehicle", "motor", "tyre", "automobile"]):
        parts.append(
            "Indian auto is navigating two simultaneous transitions: the EV shift, which is compressing "
            "near-term margins as companies invest in new platforms and supply chains, and higher commodity "
            "input costs linked to elevated energy prices from Middle East uncertainty. Domestic demand "
            "remains robust, particularly in the premium and SUV segments, providing volume support."
        )

    # Infrastructure / Construction / Cement
    elif any(x in ind for x in ["infrastructure", "construction", "cement", "engineer", "epc", "road", "highway"]):
        parts.append(
            "India's government capex supercycle — with record spending on roads, railways, ports, and "
            "urban infrastructure — is driving strong and sustained order inflows for EPC players and "
            "cement companies. AI-based project monitoring and resource optimisation are emerging as "
            "competitive differentiators among the larger infrastructure firms."
        )

    # Metals / Steel / Mining
    elif any(x in ind for x in ["metal", "steel", "iron", "mining", "aluminum", "copper", "zinc"]):
        parts.append(
            "Global metal markets face a complex backdrop: China's stimulus-driven demand recovery "
            "supports steel and base metals, while Middle East supply disruptions add volatility to "
            "energy-linked inputs. Indian steel producers benefit from domestic infrastructure demand "
            "but remain exposed to cheap import pressures and coking coal price swings."
        )

    # Consumer / FMCG / Retail
    elif any(x in ind for x in ["consumer", "retail", "fmcg", "food", "beverage", "packaged"]):
        parts.append(
            "A recovering rural economy and moderating food inflation are tailwinds for FMCG volume "
            "growth after two subdued years. AI-driven demand forecasting and supply chain optimisation "
            "are helping consumer companies reduce working capital and improve on-shelf availability. "
            "The key risk is margin pressure from a weaker rupee inflating imported input costs."
        )

    # Power / Utilities / Renewables
    elif any(x in ind for x in ["power", "utility", "electric", "renewable", "solar", "wind"]):
        parts.append(
            "India's power sector is at an inflection point — surging electricity demand from data centres "
            "(driven by AI workloads), EV charging, and industrial expansion is straining the grid and "
            "creating a supercycle for both conventional and renewable generation capacity. Middle East "
            "tensions add urgency to India's energy security strategy, accelerating domestic renewable deployment."
        )

    # Real Estate
    elif any(x in ind for x in ["real estate", "realty", "property", "housing"]):
        parts.append(
            "Indian residential real estate is in a multi-year upcycle supported by rising incomes, "
            "urbanisation, and the end of the distress cycle post-RERA. AI-powered sales analytics and "
            "dynamic pricing are helping developers optimise inventory turns. Lower interest rates in "
            "the coming quarters could provide an additional demand catalyst."
        )

    # Default macro
    else:
        parts.append(
            "India's macro fundamentals remain among the strongest globally — GDP growth of 6.5-7%, "
            "a structural domestic consumption story, and a government committed to capital formation "
            "provide a resilient backdrop. Geopolitical risks from the Middle East and global trade "
            "uncertainty are the key external variables to watch, alongside the pace of AI adoption "
            "reshaping competitive dynamics across industries."
        )

    # ── 4. Recommendation summary ─────────────────────────────────────────────
    if rec == "BUY":
        parts.append(
            "Analyst consensus is BUY — the current price is seen as offering an attractive risk-reward "
            "for medium-to-long-term investors willing to look through near-term volatility."
        )
    elif rec == "SELL":
        parts.append(
            "Analyst consensus leans SELL at current levels, suggesting the risk-reward has deteriorated "
            "and patient investors may find a better entry point after further consolidation."
        )
    elif rec == "HOLD":
        parts.append(
            "Analyst consensus is HOLD — existing investors are advised to stay the course while "
            "awaiting a clearer earnings catalyst or a more compelling valuation entry point."
        )

    return " ".join(parts) if parts else None


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


# ── FETCH ONE STOCK (with .BO fallback) ───────────────────────────────────────
def fetch_stock(symbol, yahoo_sym):
    """Try primary ticker, then .BO fallback if no price found."""
    data = _fetch_ticker(symbol, yahoo_sym)
    # If primary failed and it was a .NS ticker, try .BO as fallback
    if not data.get("available") and yahoo_sym.endswith(".NS"):
        bo_sym = yahoo_sym.replace(".NS", ".BO")
        print(f"    (.NS failed, retrying as {bo_sym}) ", end="", flush=True)
        data = _fetch_ticker(symbol, bo_sym)
    return data


def _fetch_ticker(symbol, yahoo_sym):
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

        # Generate commentary
        commentary = generate_commentary(
            name, industry, rec, pe, change_52w, rec_key, vs_50dma
        )

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
            "commentary":     commentary,
        }
    except Exception as e:
        return {"available": False, "reason": str(e)}


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    print(f"Fnikar Market Data Fetch — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    print("\nDiscovering symbols...")
    # Always start with fallback, then MERGE Firestore symbols on top.
    # This ensures: (a) existing stocks are never lost if Firestore fails,
    # (b) any new stock a user adds to their portfolio is auto-discovered.
    firestore_syms = get_symbols_from_firestore()
    stocks = {**FALLBACK_STOCKS, **firestore_syms}   # Firestore overrides fallback for same key
    new_from_firestore = len(firestore_syms) - sum(1 for s in firestore_syms if s in FALLBACK_STOCKS)
    print(f"  Total: {len(stocks)} symbols  "
          f"(fallback={len(FALLBACK_STOCKS)}, firestore_new={max(0, new_from_firestore)})\n")

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
    out_path = os.path.normpath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "market_data.json")
    )
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2, default=str)

    print(f"\n{'=' * 60}")
    print(f"Done — {ok_count}/{len(stocks)} stocks fetched.")
    print(f"Written to {out_path}")

if __name__ == "__main__":
    main()
