#!/usr/bin/env python3
"""
Fnikar — Market Data Fetcher  v3  (Full NSE Coverage)
=====================================================
Runs via GitHub Actions (or locally) to fetch complete stock fundamentals
using yfinance (server-side, no CORS issues).
Output: market_data.json (written to repo root, then moved to public/ by workflow)

Symbol discovery — in priority order
--------------------------------------
1. NSE equity CSV  — server-side fetch of all ~1700 active EQ-series stocks.
2. COMPREHENSIVE_STOCKS — ~700 hardcoded stocks (Nifty 50/Next 50/Midcap 150
   + popular mid & smallcap) used when the NSE CSV fetch fails.
3. Firestore        — user portfolio symbols always included and processed FIRST
   (most recently added stocks are fetched earliest in the run).
"""

import yfinance as yf
import json
import time
import os
import csv
import io
import gzip
import urllib.request
from collections import OrderedDict
from datetime import datetime, timezone

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
    "AIRTELPP":   "BHARTIARTL.NS",
    "IL&FSENGG":  "ILFSENGG.NS",
    "TMPV":       "TATAMTRDVR.NS",
    "GATEWAY":    "GDL.NS",
    "REC":        "RECLTD.NS",
    "IPCA":       "IPCALAB.NS",
    "DRLA":       "LALPATHLAB.NS",
    "NH":         "NH.NS",
    "NARAYANHRUD":"NH.NS",
    # Renamed companies — old NSE symbol → new Yahoo Finance ticker
    "AMARAJABAT": "ARE&M.NS",   # Amara Raja Batteries → Amara Raja Energy & Mobility
}

# ── COMPREHENSIVE FALLBACK (~700 NSE EQ stocks) ───────────────────────────────
# Used when NSE CSV fetch fails. Covers Nifty 50, Next 50, Midcap 150, and
# hundreds of popular mid/smallcap stocks.
COMPREHENSIVE_STOCKS = {
    # ── NIFTY 50 ──────────────────────────────────────────────────────────────
    "ADANIENT":    "ADANIENT.NS",
    "ADANIPORTS":  "ADANIPORTS.NS",
    "APOLLOHOSP":  "APOLLOHOSP.NS",
    "ASIANPAINT":  "ASIANPAINT.NS",
    "AXISBANK":    "AXISBANK.NS",
    "BAJAJ-AUTO":  "BAJAJ-AUTO.NS",
    "BAJAJFINSV":  "BAJAJFINSV.NS",
    "BAJFINANCE":  "BAJFINANCE.NS",
    "BHARTIARTL":  "BHARTIARTL.NS",
    "BPCL":        "BPCL.NS",
    "BRITANNIA":   "BRITANNIA.NS",
    "CIPLA":       "CIPLA.NS",
    "COALINDIA":   "COALINDIA.NS",
    "DIVISLAB":    "DIVISLAB.NS",
    "DRREDDY":     "DRREDDY.NS",
    "EICHERMOT":   "EICHERMOT.NS",
    "ETERNAL":     "ETERNAL.NS",
    "GRASIM":      "GRASIM.NS",
    "HCLTECH":     "HCLTECH.NS",
    "HDFCBANK":    "HDFCBANK.NS",
    "HDFCLIFE":    "HDFCLIFE.NS",
    "HEROMOTOCO":  "HEROMOTOCO.NS",
    "HINDALCO":    "HINDALCO.NS",
    "HINDUNILVR":  "HINDUNILVR.NS",
    "ICICIBANK":   "ICICIBANK.NS",
    "INDUSINDBK":  "INDUSINDBK.NS",
    "INFY":        "INFY.NS",
    "ITC":         "ITC.NS",
    "JSWSTEEL":    "JSWSTEEL.NS",
    "KOTAKBANK":   "KOTAKBANK.NS",
    "LT":          "LT.NS",
    "LTIM":        "LTIM.NS",
    "M&M":         "M&M.NS",
    "MARUTI":      "MARUTI.NS",
    "NESTLEIND":   "NESTLEIND.NS",
    "NTPC":        "NTPC.NS",
    "ONGC":        "ONGC.NS",
    "POWERGRID":   "POWERGRID.NS",
    "RELIANCE":    "RELIANCE.NS",
    "SBICARD":     "SBICARD.NS",
    "SBILIFE":     "SBILIFE.NS",
    "SBIN":        "SBIN.NS",
    "SHRIRAMFIN":  "SHRIRAMFIN.NS",
    "SUNPHARMA":   "SUNPHARMA.NS",
    "TATACONSUM":  "TATACONSUM.NS",
    "TATAMOTORS":  "TATAMOTORS.NS",
    "TATASTEEL":   "TATASTEEL.NS",
    "TCS":         "TCS.NS",
    "TECHM":       "TECHM.NS",
    "TITAN":       "TITAN.NS",
    "TRENT":       "TRENT.NS",
    "ULTRACEMCO":  "ULTRACEMCO.NS",
    "WIPRO":       "WIPRO.NS",
    # ── NIFTY NEXT 50 ─────────────────────────────────────────────────────────
    "ABB":         "ABB.NS",
    "ADANIGREEN":  "ADANIGREEN.NS",
    "ADANIPOWER":  "ADANIPOWER.NS",
    "ALKEM":       "ALKEM.NS",
    "AMBUJACEM":   "AMBUJACEM.NS",
    "ATGL":        "ATGL.NS",
    "BANKBARODA":  "BANKBARODA.NS",
    "BERGEPAINT":  "BERGEPAINT.NS",
    "BEL":         "BEL.NS",
    "BHEL":        "BHEL.NS",
    "BOSCHLTD":    "BOSCHLTD.NS",
    "CHOLAFIN":    "CHOLAFIN.NS",
    "CUMMINSIND":  "CUMMINSIND.NS",
    "DABUR":       "DABUR.NS",
    "DLF":         "DLF.NS",
    "DMART":       "DMART.NS",
    "GAIL":        "GAIL.NS",
    "GODREJCP":    "GODREJCP.NS",
    "GODREJPROP":  "GODREJPROP.NS",
    "HAL":         "HAL.NS",
    "HAVELLS":     "HAVELLS.NS",
    "HFCL":        "HFCL.NS",
    "ICICIPRULI":  "ICICIPRULI.NS",
    "ICICIGI":     "ICICIGI.NS",
    "INDIGO":      "INDIGO.NS",
    "INDHOTEL":    "INDHOTEL.NS",
    "IOC":         "IOC.NS",
    "IRCTC":       "IRCTC.NS",
    "JIOFIN":      "JIOFIN.NS",
    "JSWENERGY":   "JSWENERGY.NS",
    "LICI":        "LICI.NS",
    "MARICO":      "MARICO.NS",
    "MAZDOCK":     "MAZDOCK.NS",
    "MAXHEALTH":   "MAXHEALTH.NS",
    "MOTHERSON":   "MOTHERSON.NS",
    "MPHASIS":     "MPHASIS.NS",
    "NHPC":        "NHPC.NS",
    "NAUKRI":      "NAUKRI.NS",
    "NYKAA":       "NYKAA.NS",
    "OBEROIRLTY":  "OBEROIRLTY.NS",
    "PATANJALI":   "PATANJALI.NS",
    "PETRONET":    "PETRONET.NS",
    "PERSISTENT":  "PERSISTENT.NS",
    "PIDILITIND":  "PIDILITIND.NS",
    "PFC":         "PFC.NS",
    "POLYCAB":     "POLYCAB.NS",
    "RECLTD":      "RECLTD.NS",
    "SHREECEM":    "SHREECEM.NS",
    "SIEMENS":     "SIEMENS.NS",
    "TATATECH":    "TATATECH.NS",
    "TORNTPOWER":  "TORNTPOWER.NS",
    "UPL":         "UPL.NS",
    "VEDL":        "VEDL.NS",
    "VOLTAS":      "VOLTAS.NS",
    "VBL":         "VBL.NS",
    "ZOMATO":      "ZOMATO.NS",
    # ── NIFTY MIDCAP 150 ──────────────────────────────────────────────────────
    "3MINDIA":     "3MINDIA.NS",
    "APLAPOLLO":   "APLAPOLLO.NS",
    "ABCAPITAL":   "ABCAPITAL.NS",
    "ABFRL":       "ABFRL.NS",
    "ACC":         "ACC.NS",
    "AEGISLOG":    "AEGISLOG.NS",
    "ANGELONE":    "ANGELONE.NS",
    "APARINDS":    "APARINDS.NS",
    "ASHOKLEY":    "ASHOKLEY.NS",
    "ASTRAL":      "ASTRAL.NS",
    "AUROPHARMA":  "AUROPHARMA.NS",
    "BALKRISIND":  "BALKRISIND.NS",
    "BANKINDIA":   "BANKINDIA.NS",
    "BATAINDIA":   "BATAINDIA.NS",
    "BAYERCROP":   "BAYERCROP.NS",
    "BIOCON":      "BIOCON.NS",
    "BLUEDART":    "BLUEDART.NS",
    "BLUESTAR":    "BLUESTAR.NS",
    "BRIGADE":     "BRIGADE.NS",
    "BSOFT":       "BSOFT.NS",
    "CAMS":        "CAMS.NS",
    "CANBK":       "CANBK.NS",
    "CARERATING":  "CARERATING.NS",
    "CASTROLIND":  "CASTROLIND.NS",
    "CEATLTD":     "CEATLTD.NS",
    "CENTURYTEX":  "CENTURYTEX.NS",
    "CESC":        "CESC.NS",
    "CHAMBLFERT":  "CHAMBLFERT.NS",
    "CDSL":        "CDSL.NS",
    "COFORGE":     "COFORGE.NS",
    "COLPAL":      "COLPAL.NS",
    "CONCOR":      "CONCOR.NS",
    "COROMANDEL":  "COROMANDEL.NS",
    "CROMPTON":    "CROMPTON.NS",
    "DALBHARAT":   "DALBHARAT.NS",
    "DAMCAPITAL":  "DAMCAPITAL.NS",
    "DCW":         "DCW.NS",
    "DCMSHRIRAM":  "DCMSHRIRAM.NS",
    "DEEPAKNTR":   "DEEPAKNTR.NS",
    "DELHIVERY":   "DELHIVERY.NS",
    "DEVYANI":     "DEVYANI.NS",
    "DREAMFOLKS":  "DREAMFOLKS.NS",
    "DWARKESH":    "DWARKESH.NS",
    "ECLERX":      "ECLERX.NS",
    "EIHOTEL":     "EIHOTEL.NS",
    "EKC":         "EKC.NS",
    "ELGIEQUIP":   "ELGIEQUIP.NS",
    "EMAMILTD":    "EMAMILTD.NS",
    "ESCORTS":     "ESCORTS.NS",
    "EXIDEIND":    "EXIDEIND.NS",
    "FINCABLES":   "FINCABLES.NS",
    "FORTIS":      "FORTIS.NS",
    "GESHIP":      "GESHIP.NS",
    "GLENMARK":    "GLENMARK.NS",
    "GMRAIRPORT":  "GMRAIRPORT.NS",
    "GNFC":        "GNFC.NS",
    "GRANULES":    "GRANULES.NS",
    "HAPPSTMNDS":  "HAPPSTMNDS.NS",
    "HINDPETRO":   "HINDPETRO.NS",
    "HONASA":      "HONASA.NS",
    "HUDCO":       "HUDCO.NS",
    "IDEA":        "IDEA.NS",
    "IDFCFIRSTB":  "IDFCFIRSTB.NS",
    "IEX":         "IEX.NS",
    "IGL":         "IGL.NS",
    "IMAGICAA":    "IMAGICAA.NS",
    "INDIAMART":   "INDIAMART.NS",
    "IOB":         "IOB.NS",
    "IPCALAB":     "IPCALAB.NS",
    "IREDA":       "IREDA.NS",
    "IRFC":        "IRFC.NS",
    "ISEC":        "ISEC.NS",
    "ITI":         "ITI.NS",
    "ITCHOTELS":   "ITCHOTELS.NS",
    "J&KBANK":     "J&KBANK.NS",
    "JAMNAAUTO":   "JAMNAAUTO.NS",
    "JBCHEPHARM":  "JBCHEPHARM.NS",
    "JETAIRWAYS":  "JETAIRWAYS.NS",
    "JIOFIN":      "JIOFIN.NS",
    "JINDALSAW":   "JINDALSAW.NS",
    "JKCEMENT":    "JKCEMENT.NS",
    "JPPOWER":     "JPPOWER.NS",
    "JSWINFRA":    "JSWINFRA.NS",
    "JUBLFOOD":    "JUBLFOOD.NS",
    "JUSTDIAL":    "JUSTDIAL.NS",
    "KALYANKJIL":  "KALYANKJIL.NS",
    "KAYNES":      "KAYNES.NS",
    "KIRLOSIND":   "KIRLOSIND.NS",
    "KNRCON":      "KNRCON.NS",
    "KOTAKBANK":   "KOTAKBANK.NS",
    "KPIL":        "KPIL.NS",
    "KPITTECH":    "KPITTECH.NS",
    "KRBL":        "KRBL.NS",
    "LALPATHLAB":  "LALPATHLAB.NS",
    "LAURUSLABS":  "LAURUSLABS.NS",
    "LEMONTREE":   "LEMONTREE.NS",
    "LICHSGFIN":   "LICHSGFIN.NS",
    "LTF":         "LTF.NS",
    "LTFOODS":     "LTFOODS.NS",
    "LTTS":        "LTTS.NS",
    "LUPIN":       "LUPIN.NS",
    "LUXIND":      "LUXIND.NS",
    "M&MFIN":      "M&MFIN.NS",
    "MANAPPURAM":  "MANAPPURAM.NS",
    "MAPMYINDIA":  "MAPMYINDIA.NS",
    "MARKSANS":    "MARKSANS.NS",
    "MAWANASUG":   "MAWANASUG.NS",
    "METROPOLIS":  "METROPOLIS.NS",
    "MFSL":        "MFSL.NS",
    "MGL":         "MGL.NS",
    "MID150BEES":  "MID150BEES.NS",
    "MIDHANI":     "MIDHANI.NS",
    "MOIL":        "MOIL.NS",
    "MRF":         "MRF.NS",
    "MRPL":        "MRPL.NS",
    "MSTCLTD":     "MSTCLTD.NS",
    "MUKTAARTS":   "MUKTAARTS.NS",
    "MUTHOOTFIN":  "MUTHOOTFIN.NS",
    "NALCO":       "NALCO.NS",
    "NATCOPHARM":  "NATCOPHARM.NS",
    "NAVINFLUOR":  "NAVINFLUOR.NS",
    "NDTV":        "NDTV.NS",
    "NH":          "NH.NS",
    "NMDC":        "NMDC.NS",
    "NSLNISP":     "NSLNISP.NS",
    "OFSS":        "OFSS.NS",
    "ORIENTHOT":   "ORIENTHOT.NS",
    "PAGEIND":     "PAGEIND.NS",
    "PAYTM":       "PAYTM.NS",
    "PCBL":        "PCBL.NS",
    "PGHH":        "PGHH.NS",
    "PHOENIXLTD":  "PHOENIXLTD.NS",
    "PNB":         "PNB.NS",
    "PNBHOUSING":  "PNBHOUSING.NS",
    "POLICYBZR":   "POLICYBZR.NS",
    "POONAWALLA":  "POONAWALLA.NS",
    "PRAJIND":     "PRAJIND.NS",
    "PRESTIGE":    "PRESTIGE.NS",
    "PVRINOX":     "PVRINOX.NS",
    "RADICO":      "RADICO.NS",
    "RAJESHEXPO":  "RAJESHEXPO.NS",
    "RAMCOCEM":    "RAMCOCEM.NS",
    "RAYMOND":     "RAYMOND.NS",
    "RBLBANK":     "RBLBANK.NS",
    "RCOM":        "RCOM.NS",
    "REDINGTON":   "REDINGTON.NS",
    "RELIGARE":    "RELIGARE.NS",
    "RENUKA":      "RENUKA.NS",
    "RITES":       "RITES.NS",
    "RPOWER":      "RPOWER.NS",
    "ROYALHOTEL":  "ROYALHOTEL.NS",
    "SAIL":        "SAIL.NS",
    "SBISILVER":   "SBISILVER.NS",
    "SCHAEFFLER":  "SCHAEFFLER.NS",
    "SCI":         "SCI.NS",
    "SJVN":        "SJVN.NS",
    "SKF":         "SKF.NS",
    "SOBHA":       "SOBHA.NS",
    "SOLARA":      "SOLARA.NS",
    "SOLARINDS":   "SOLARINDS.NS",
    "SONACOMS":    "SONACOMS.NS",
    "SPICEJET":    "SPICEJET.NS",
    "STLTECH":     "STLTECH.NS",
    "SUBROS":      "SUBROS.NS",
    "SUMICHEM":    "SUMICHEM.NS",
    "SUNDARMFIN":  "SUNDARMFIN.NS",
    "SUNDRMFAST":  "SUNDRMFAST.NS",
    "SUNINDMEDIA": "SUNINDMEDIA.NS",
    "SUNTV":       "SUNTV.NS",
    "SUPRAJIT":    "SUPRAJIT.NS",
    "SURYAROSNI":  "SURYAROSNI.NS",
    "SUZLON":      "SUZLON.NS",
    "TANLA":       "TANLA.NS",
    "TATACHEM":    "TATACHEM.NS",
    "TATACOMM":    "TATACOMM.NS",
    "TATAELXSI":   "TATAELXSI.NS",
    "TATAMTRDVR":  "TATAMTRDVR.NS",
    "TATAPOWER":   "TATAPOWER.NS",
    "THERMAX":     "THERMAX.NS",
    "THYROCARE":   "THYROCARE.NS",
    "TIINDIA":     "TIINDIA.NS",
    "TIMKEN":      "TIMKEN.NS",
    "TTKPRESTIG":  "TTKPRESTIG.NS",
    "TVSMOTOR":    "TVSMOTOR.NS",
    "TVSMOTORS":   "TVSMOTORS.NS",
    "UCOBANK":     "UCOBANK.NS",
    "UFBL":        "UFBL.NS",
    "UJJIVANSFB":  "UJJIVANSFB.NS",
    "UNIONBANK":   "UNIONBANK.NS",
    "VGUARD":      "VGUARD.NS",
    "VTL":         "VTL.NS",
    "WELCORP":     "WELCORP.NS",
    "WELSPUN":     "WELSPUN.NS",
    "WESTLIFE":    "WESTLIFE.NS",
    "WHIRLPOOL":   "WHIRLPOOL.NS",
    "WOCKPHARMA":  "WOCKPHARMA.NS",
    "YESBANK":     "YESBANK.NS",
    "ZYDUSLIFE":   "ZYDUSLIFE.NS",
    "ZYDUSWELL":   "ZYDUSWELL.NS",
    # ── EXISTING USER PORTFOLIO STOCKS (legacy fallback) ──────────────────────
    "ADVANIHOTR":  "ADVANIHOTR.NS",
    "ALOKINDS":    "ALOKINDS.NS",
    "AMARAJABAT":  "ARE&M.NS",
    "ARVIND":      "ARVIND.NS",
    "BAJAJHIND":   "BAJAJHIND.NS",
    "BANDHANBNK":  "BANDHANBNK.NS",
    "BIKAJI":      "BIKAJI.NS",
    "COCHINSHIP":  "COCHINSHIP.NS",
    "FEDERALBNK":  "FEDERALBNK.NS",
    "FRETAIL":     "FRETAIL.NS",
    "INDIANB":     "INDIANB.NS",
    "IPL":         "IPL.NS",
    "JKCEMENT":    "JKCEMENT.NS",
    "JSWSTEEL":    "JSWSTEEL.NS",
    "LTIM":        "LTIM.NS",
    "MID150BEES":  "MID150BEES.NS",
    "NSLNISP":     "NSLNISP.NS",
    "NTPCGREEN":   "NTPCGREEN.NS",
    "PVRINOX":     "PVRINOX.NS",
    "VBL":         "VBL.NS",
    # ── SPECIAL BSE / OVERRIDE MAPPINGS ──────────────────────────────────────
    "531340":      "531340.BO",
    "532281":      "HCLTECH.NS",
    "524715":      "SUNPHARMA.NS",
    "533248":      "GPPL.NS",
    "538567":      "GULFOILLUB.NS",
    "538666":      "SHARDACROP.NS",
    "513262":      "SSWL.NS",
    "540124":      "GNA.NS",
    "543300":      "SONACOMS.NS",
    "544289":      "NTPCGREEN.NS",
    "500285":      "SPICEJET.NS",
    "500314":      "ORIENTHOT.NS",
    "500400":      "TATAPOWER.NS",
    "100312":      "ONGC.NS",
    "100113":      "SAIL.NS",
    "532699":      "ROYALHOTEL.NS",
    "RELIANCEP1":  "RELIANCE.NS",
    "UPLPP1":      "UPL.NS",
    "AIRTELPP":    "BHARTIARTL.NS",
    "IL&FSENGG":   "ILFSENGG.NS",
    "TMPV":        "TATAMTRDVR.NS",
    "GATEWAY":     "GDL.NS",
    "REC":         "RECLTD.NS",
}


# ── NSE EQUITY LIST FETCH (server-side, no CORS) ──────────────────────────────
def get_nse_all_stocks():
    """
    Fetches all active NSE EQ-series stocks from the NSE archives CSV.
    Returns dict {symbol: 'SYMBOL.NS'} — typically ~1700 stocks.
    Raises an exception on failure so caller can fall back to COMPREHENSIVE_STOCKS.
    """
    url = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Referer": "https://www.nseindia.com/",
            "Connection": "keep-alive",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read()
        enc = resp.headers.get("Content-Encoding", "")
        content = gzip.decompress(raw).decode("utf-8", errors="ignore") if "gzip" in enc else raw.decode("utf-8", errors="ignore")

    stocks = {}
    reader = csv.reader(io.StringIO(content))
    next(reader, None)  # skip header
    for row in reader:
        if len(row) < 3:
            continue
        sym    = row[0].strip().strip('"')
        series = row[2].strip().strip('"')
        if not sym or series != "EQ":
            continue
        # Apply override if known, else default to .NS or .BO for numeric codes
        if sym in YAHOO_OVERRIDES:
            stocks[sym] = YAHOO_OVERRIDES[sym]
        elif sym.isdigit():
            stocks[sym] = sym + ".BO"
        else:
            stocks[sym] = sym + ".NS"

    if len(stocks) < 500:
        raise RuntimeError(f"NSE CSV returned only {len(stocks)} stocks — possible fetch error")
    return stocks


# ── BSE ADDITIONAL STOCKS FETCH (server-side) ────────────────────────────────
def get_bse_additional_stocks(already_covered):
    """
    Fetches all active BSE equity stocks NOT already covered by the NSE list.
    Uses SCRIPCODE.BO format for Yahoo Finance (e.g. '500325.BO').
    already_covered: set of symbols/codes already in base_stocks — these are skipped.
    Raises an exception on failure so caller can safely continue with NSE-only.
    """
    url = (
        "https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w"
        "?Group=&Scripcode=&industry=&segment=Equity&status=Active"
    )
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Referer": "https://www.bseindia.com/",
            "Origin": "https://www.bseindia.com",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read()
        enc = resp.headers.get("Content-Encoding", "")
        content = (
            gzip.decompress(raw).decode("utf-8", errors="ignore")
            if "gzip" in enc
            else raw.decode("utf-8", errors="ignore")
        )
    data = json.loads(content)
    # API may return list directly or wrap it: {"Table": [...]}
    if isinstance(data, dict):
        data = data.get("Table") or data.get("data") or data.get("Data") or []

    stocks = {}
    for item in data:
        # Scrip code — BSE numeric ID (e.g. 500325)
        scrip_code = str(
            item.get("SCRIP_CD") or item.get("Scripcode") or item.get("scrip_cd") or ""
        ).strip()
        # NSE-equivalent symbol (e.g. RELIANCE) — used for dedup check
        scrip_id = (
            item.get("Scrip_Id") or item.get("SCRIP_ID") or item.get("scrip_id") or ""
        ).strip().upper()

        if not scrip_code:
            continue
        # Skip if NSE symbol already covered
        if scrip_id and scrip_id in already_covered:
            continue
        # Skip if the numeric code itself is already covered (manual overrides)
        if scrip_code in already_covered:
            continue

        stocks[scrip_code] = scrip_code + ".BO"

    if len(stocks) < 100:
        raise RuntimeError(
            f"BSE API returned only {len(stocks)} new stocks — possible fetch or dedup error"
        )
    return stocks


# ── SYMBOL RESOLUTION ─────────────────────────────────────────────────────────
def resolve_yahoo_symbol(symbol):
    if symbol in YAHOO_OVERRIDES:
        return YAHOO_OVERRIDES[symbol]
    if symbol.isdigit():
        return symbol + ".BO"
    return symbol + ".NS"


# ── FIRESTORE SYMBOL DISCOVERY ────────────────────────────────────────────────
def get_symbols_from_firestore():
    """
    Returns an OrderedDict of {symbol: yahoo_ticker} discovered from all user
    portfolios in Firestore, ordered by addedAt DESCENDING (most recently added
    stocks appear first — so new additions are enriched in the earliest batch).
    Returns empty OrderedDict on any failure.
    """
    sa_json = os.environ.get("FIREBASE_SERVICE_ACCOUNT")
    if not sa_json:
        print("  FIREBASE_SERVICE_ACCOUNT secret not set — Firestore discovery skipped.")
        print("  ► Add FIREBASE_SERVICE_ACCOUNT to GitHub Actions secrets to enable auto-discovery.")
        return OrderedDict()
    try:
        import firebase_admin
        from firebase_admin import credentials, firestore as fs
        if not firebase_admin._apps:
            cred = credentials.Certificate(json.loads(sa_json))
            firebase_admin.initialize_app(cred)
        db = fs.client()

        # Collect (sort_key, symbol) across all user portfolios
        all_entries = []
        portfolio_count = 0
        for portfolio_doc in db.collection("portfolios").stream():
            portfolio_count += 1
            uid = portfolio_doc.id
            for stock_doc in (
                db.collection("portfolios")
                .document(uid)
                .collection("stocks")
                .stream()
            ):
                data = stock_doc.to_dict()
                sym  = (data.get("symbol") or "").strip().upper()
                if not sym:
                    continue
                # Convert Firestore Timestamp → float for sorting
                added_at = data.get("addedAt")
                if hasattr(added_at, "timestamp"):
                    sort_key = added_at.timestamp()
                elif isinstance(added_at, datetime):
                    sort_key = added_at.timestamp()
                else:
                    sort_key = 0.0
                all_entries.append((sort_key, sym))

        if not all_entries:
            print(f"  Firestore: {portfolio_count} portfolios found but no stock symbols.")
            return OrderedDict()

        # Sort newest-first, deduplicate preserving order
        all_entries.sort(key=lambda x: x[0], reverse=True)
        seen, ordered = set(), []
        for _, sym in all_entries:
            if sym not in seen:
                seen.add(sym)
                ordered.append(sym)

        print(f"  Firestore: {len(ordered)} unique symbols across {portfolio_count} portfolios (newest-first order).")
        return OrderedDict((sym, resolve_yahoo_symbol(sym)) for sym in ordered)

    except Exception as e:
        print(f"  Firestore error ({type(e).__name__}): {e}")
        print("  ► Check that the service account has 'Cloud Datastore User' role in GCP.")
        return OrderedDict()


# ── COMMENTARY GENERATOR ──────────────────────────────────────────────────────
def generate_commentary(name, industry, rec, pe, change_52w, rec_key, vs_50dma):
    parts = []
    ind = (industry or "").lower()

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
            parts.append(f"{name} has faced significant pressure, falling {abs(change_52w):.1f}% over the past year — a sharp underperformance that warrants close scrutiny.")

    if pe is not None and pe > 0:
        if pe < 8:
            parts.append(f"At a trailing P/E of just {pe:.1f}x, the stock appears significantly undervalued or the market is pricing in persistent earnings risk.")
        elif pe < 15:
            parts.append(f"A trailing P/E of {pe:.1f}x makes the stock attractively valued relative to most peers.")
        elif pe < 25:
            parts.append(f"Trading at {pe:.1f}x trailing earnings, the valuation is fair and reflects steady growth expectations.")
        elif pe < 45:
            parts.append(f"The premium valuation of {pe:.1f}x P/E suggests the market is pricing in strong earnings growth ahead.")
        else:
            parts.append(f"At {pe:.1f}x trailing P/E, the stock carries a rich valuation that leaves little room for earnings disappointment.")

    if any(x in ind for x in ["oil", "gas", "energy", "petro", "refin", "lpg"]):
        parts.append("Geopolitical tensions in the Middle East — particularly escalating Iran-related risks — are keeping Brent crude elevated, a direct tailwind for upstream E&P and oilfield services. Downstream refiners face margin compression from high feedstock costs, though marketing margins provide a partial buffer.")
    elif any(x in ind for x in ["aviation", "airline", "airport"]):
        parts.append("Elevated crude oil prices driven by Middle East geopolitical risk are a meaningful headwind for aviation, where fuel accounts for 25–35% of operating costs. Strong domestic air travel demand and improving load factors help offset the fuel cost pressure.")
    elif any(x in ind for x in ["logistics", "shipping", "freight", "transport"]):
        parts.append("Middle East tensions are disrupting Red Sea shipping routes, pushing freight rates higher and benefiting Indian shipping and port companies. Longer voyage distances are increasing ton-mile demand, supporting fleet utilisation.")
    elif any(x in ind for x in ["software", "information tech", "it service", "computer", "tech"]):
        parts.append("AI penetration is the defining theme for Indian IT — large-cap players are investing heavily in GenAI service lines to capture the next wave of enterprise digital transformation spend. Companies that transition from traditional outsourcing to AI-augmented delivery will command superior margins over the medium term.")
    elif any(x in ind for x in ["bank", "banking", "finance", "nbfc", "financial service", "insurance", "capital market"]):
        parts.append("Indian banks are deploying AI across credit underwriting and fraud detection, meaningfully reducing cost-to-income ratios. With the RBI beginning its rate easing cycle, NIM pressure may emerge near-term, but improving asset quality and AI-driven operating leverage support a constructive medium-term outlook.")
    elif any(x in ind for x in ["pharma", "health", "hospital", "medical", "drug", "biotech"]):
        parts.append("AI-driven drug discovery is compressing R&D timelines for Indian pharma, while strong US generics demand and a healthy domestic prescription market provide dual revenue engines. India's CDMO opportunity is also expanding as global innovators diversify manufacturing away from China.")
    elif any(x in ind for x in ["telecom", "communication", "wireless"]):
        parts.append("5G monetisation and AI-powered network management are the key re-rating triggers for Indian telecom. The sector's consolidation into a two-and-a-half player market has restored pricing discipline, with ARPU expansion likely as users migrate to premium data plans.")
    elif any(x in ind for x in ["auto", "vehicle", "motor", "tyre", "automobile"]):
        parts.append("Indian auto is navigating two simultaneous transitions: the EV shift compressing near-term margins, and higher commodity input costs linked to elevated energy prices. Domestic demand remains robust, particularly in the premium and SUV segments.")
    elif any(x in ind for x in ["infrastructure", "construction", "cement", "engineer", "epc", "road", "highway"]):
        parts.append("India's government capex supercycle — record spending on roads, railways, ports, and urban infrastructure — is driving strong and sustained order inflows for EPC players and cement companies.")
    elif any(x in ind for x in ["metal", "steel", "iron", "mining", "aluminum", "copper", "zinc"]):
        parts.append("Global metal markets face a complex backdrop: China's stimulus-driven demand recovery supports steel and base metals, while Middle East supply disruptions add volatility to energy-linked inputs. Indian steel producers benefit from domestic infrastructure demand.")
    elif any(x in ind for x in ["consumer", "retail", "fmcg", "food", "beverage", "packaged"]):
        parts.append("A recovering rural economy and moderating food inflation are tailwinds for FMCG volume growth. AI-driven demand forecasting is helping consumer companies reduce working capital and improve on-shelf availability.")
    elif any(x in ind for x in ["power", "utility", "electric", "renewable", "solar", "wind"]):
        parts.append("India's power sector is at an inflection point — surging electricity demand from data centres (driven by AI workloads), EV charging, and industrial expansion is straining the grid and creating a supercycle for both conventional and renewable generation capacity.")
    elif any(x in ind for x in ["real estate", "realty", "property", "housing"]):
        parts.append("Indian residential real estate is in a multi-year upcycle supported by rising incomes, urbanisation, and the end of the distress cycle post-RERA. Lower interest rates in the coming quarters could provide an additional demand catalyst.")
    else:
        parts.append("India's macro fundamentals remain among the strongest globally — GDP growth of 6.5–7%, a structural domestic consumption story, and a government committed to capital formation provide a resilient backdrop. Geopolitical risks from the Middle East and global trade uncertainty are the key external variables to watch.")

    if rec == "BUY":
        parts.append("Analyst consensus is BUY — the current price is seen as offering an attractive risk-reward for medium-to-long-term investors willing to look through near-term volatility.")
    elif rec == "SELL":
        parts.append("Analyst consensus leans SELL at current levels, suggesting the risk-reward has deteriorated and patient investors may find a better entry point after further consolidation.")
    elif rec == "HOLD":
        parts.append("Analyst consensus is HOLD — existing investors are advised to stay the course while awaiting a clearer earnings catalyst or a more compelling valuation entry point.")

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
    data = _fetch_ticker(symbol, yahoo_sym)
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
                buy_recs  = safe_int((row.get("strongBuy") or 0)  + (row.get("buy")  or 0))
                sell_recs = safe_int((row.get("strongSell") or 0) + (row.get("sell") or 0))
        except Exception:
            n    = info.get("numberOfAnalystOpinions")
            mean = info.get("recommendationMean")
            if n and mean:
                buy_recs  = round(n * max(0.0, 1.0 - (mean - 1) / 4))
                sell_recs = round(n * max(0.0, (mean - 1) / 4 * 0.5))

        commentary = generate_commentary(name, industry, rec, pe, change_52w, rec_key, vs_50dma)

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
    print(f"Fnikar Market Data Fetch v3 — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 68)

    # ── Step 1: Get all NSE stocks ─────────────────────────────────────────────
    print("\n[Step 1] Fetching all NSE equity symbols from NSE archives CSV…")
    base_stocks = {}
    try:
        base_stocks = get_nse_all_stocks()
        print(f"  ✓ NSE CSV: {len(base_stocks)} EQ-series stocks loaded.")
    except Exception as e:
        print(f"  ✗ NSE CSV fetch failed: {e}")
        print(f"  ► Falling back to COMPREHENSIVE_STOCKS ({len(COMPREHENSIVE_STOCKS)} stocks).")
        base_stocks = dict(COMPREHENSIVE_STOCKS)

    # ── Step 1b: Get BSE-only stocks (not already on NSE) ─────────────────────
    print("\n[Step 1b] Fetching BSE-only stocks (not already covered by NSE)…")
    try:
        bse_additional = get_bse_additional_stocks(set(base_stocks.keys()))
        print(f"  ✓ BSE additional: {len(bse_additional)} BSE-only stocks added.")
        base_stocks.update(bse_additional)
        print(f"  Total base coverage: {len(base_stocks)} stocks (NSE + BSE combined)")
    except Exception as e:
        print(f"  ✗ BSE fetch failed: {e}")
        print(f"  ► Continuing with NSE/comprehensive stocks only — no BSE addition this run.")

    # ── Step 2: Get Firestore portfolio symbols (newest-first order) ───────────
    print("\n[Step 2] Discovering user portfolio symbols from Firestore…")
    firestore_syms = get_symbols_from_firestore()

    # ── Step 3: Build final processing order ──────────────────────────────────
    # Priority 1 — Firestore symbols (newest additions processed FIRST so new
    #              stocks get data in the earliest possible run)
    # Priority 2 — NSE/comprehensive base list (fills in the rest)
    # Priority 3 — COMPREHENSIVE_STOCKS entries not covered by NSE or Firestore
    # Priority 4 — Legacy override entries (numeric BSE codes, etc.)
    all_stocks: OrderedDict = OrderedDict()

    for sym, yahoo in firestore_syms.items():
        all_stocks[sym] = YAHOO_OVERRIDES.get(sym, yahoo)

    for sym, yahoo in base_stocks.items():
        if sym not in all_stocks:
            all_stocks[sym] = YAHOO_OVERRIDES.get(sym, yahoo)

    for sym, yahoo in COMPREHENSIVE_STOCKS.items():
        if sym not in all_stocks:
            all_stocks[sym] = yahoo

    for sym, yahoo in YAHOO_OVERRIDES.items():
        if sym not in all_stocks:
            all_stocks[sym] = yahoo

    priority_count = len(firestore_syms)
    total = len(all_stocks)
    print(f"\n[Step 3] Final symbol list: {total} stocks")
    print(f"  First {priority_count} = user portfolio stocks (Firestore, newest-first)")
    print(f"  Remaining {total - priority_count} = full NSE + BSE combined coverage")

    # ── Step 4: Fetch data ─────────────────────────────────────────────────────
    print(f"\n[Step 4] Fetching market data for {total} symbols…")
    print(f"  Estimated time: ~{total * 0.3 / 60:.0f}–{total * 0.35 / 60:.0f} minutes\n")

    result   = {"generatedAt": datetime.now().isoformat(), "stocks": {}}
    ok_count = 0

    for i, (symbol, yahoo_sym) in enumerate(all_stocks.items(), 1):
        label = " [PRIORITY]" if i <= priority_count else ""
        print(f"  [{i:4}/{total}] {symbol:15} ({yahoo_sym}){label} … ", end="", flush=True)
        data = fetch_stock(symbol, yahoo_sym)
        result["stocks"][symbol] = data
        if data.get("available"):
            ok_count += 1
            print(f"OK  ₹{data.get('price')}  {(data.get('industry') or '')[:28]}")
        else:
            print(f"SKIP  {data.get('reason', '')}")
        time.sleep(0.3)

    # ── Write output ───────────────────────────────────────────────────────────
    out_path = os.path.normpath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "market_data.json")
    )
    with open(out_path, "w") as f:
        json.dump(result, f, separators=(",", ":"), default=str)

    size_kb = os.path.getsize(out_path) / 1024
    print(f"\n{'=' * 68}")
    print(f"Done — {ok_count}/{total} stocks fetched  ({ok_count/total*100:.1f}% success rate)")
    print(f"Written to {out_path}  ({size_kb:.0f} KB)")

if __name__ == "__main__":
    main()
