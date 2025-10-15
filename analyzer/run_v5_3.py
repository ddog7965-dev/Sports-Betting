import os, sys, pandas as pd, numpy as np, hashlib
from datetime import datetime, timezone

PROPS_URL = os.environ.get("PROPS_URL","").strip()
ROW_LIMIT = int(os.environ.get("ROW_LIMIT","5000"))

if not PROPS_URL:
    print("FAIL: PROPS_URL env var is required (GitHub Release CSV).")
    sys.exit(2)

# Load from GitHub Release (stream)
try:
    df = pd.read_csv(PROPS_URL)
    if ROW_LIMIT > 0:
        df = df.head(ROW_LIMIT)
except Exception as e:
    print(f"FAIL: Could not read CSV from {PROPS_URL}\n{e}")
    sys.exit(3)

required = ["game_id","week","player","team","market","selection","line","price","book","kickoff_utc"]
missing = [c for c in required if c not in df.columns]
if missing:
    print("FAIL: Missing required columns:", ", ".join(missing))
    print("Header seen:", ", ".join(df.columns.astype(str).tolist()))
    sys.exit(4)

def american_to_prob(o):
    try:
        o = float(o)
        return (-o)/(-o+100) if o<0 else 100/(o+100)
    except:
        return np.nan

df = df.dropna(subset=["player","market","selection","price","book"])
df["book"] = df["book"].astype(str).str.upper().str.strip()
df["book_prob"] = df["price"].apply(american_to_prob)

gcols = ["player","team","market","selection","line","kickoff_utc"]
fair = df.groupby(gcols, dropna=False)["book_prob"].median().rename("fair_prob")
df = df.merge(fair, on=gcols, how="left")
df["ev_pct"] = (df["fair_prob"] - df["book_prob"]) * 100

def in_band(o):
    try:
        o = float(o)
        return (o >= -115) and (o <= 200)
    except: return False
df["in_band"] = df["price"].apply(in_band)

idx = df.groupby(gcols)["ev_pct"].idxmax()
best = df.loc[idx].copy()

singles = best[(best["in_band"]) & (best["ev_pct"]>=2.0)].copy()
singles = singles.sort_values(["ev_pct"], ascending=False).head(6)

best_pool = best[best["ev_pct"]>=1.0].sort_values("ev_pct", ascending=False).head(40).copy()
best_pool["game_key"] = best_pool["game_id"].astype(str)

def build_parlay(pool, n):
    chosen, seen = [], set()
    for _, r in pool.iterrows():
        if r["game_key"] in seen: continue
        chosen.append(r); seen.add(r["game_key"])
        if len(chosen)==n: break
    return pd.DataFrame(chosen)

def prob_to_american(p):
    if p<=0 or p>=1: return None
    dec = 1/p
    return round((dec-1)*100) if dec>=2 else round(-100/(dec-1))

def parlay_metrics(legs):
    if legs.empty: return (None, None)
    ip = float(np.prod(legs["book_prob"].astype(float)))
    return ip, prob_to_american(ip)

parlay3 = build_parlay(best_pool, 3)
parlay4 = build_parlay(best_pool.iloc[3:], 4)
ip3, us3 = parlay_metrics(parlay3)
ip4, us4 = parlay_metrics(parlay4)

def fmt(o):
    try: o=int(o); return f"+{o}" if o>0 else str(o)
    except: return str(o)

def show(df_in, name):
    cols = ["player","team","market","selection","line","book","price","fair_prob","book_prob","ev_pct","kickoff_utc"]
    d = df_in[cols].copy()
    d["price"] = d["price"].apply(fmt)
    for c in ["fair_prob","book_prob"]:
        d[c] = (d[c]*100).round(1)
    d["ev_pct"] = d["ev_pct"].round(1)
    print(f"\n{name}")
    print(d.rename(columns={
        "player":"Player","team":"Team","market":"Market","selection":"Pick",
        "line":"Line","book":"Book","price":"Price","fair_prob":"Fair%","book_prob":"Book%","ev_pct":"EV%","kickoff_utc":"Kickoff (UTC)"
    }).to_string(index=False))

sha = hashlib.sha256(pd.util.hash_pandas_object(df, index=False).values).hexdigest()[:16]
print("1) DATA SOURCE — GitHub Release")
print(f"   URL: {PROPS_URL}")
print(f"   Rows used: {len(df)}   ROW_LIMIT: {ROW_LIMIT}   SHA256*: {sha}")
print("   *hash of loaded rows only\n")

if len(singles):
    show(singles, "2) TOP SINGLES (EV≥2%, −115..+200)")
else:
    print("2) TOP SINGLES — None (band/EV filters too strict)")

if not parlay3.empty:
    show(parlay3, "3A) PARLAY (3 legs)")
    print(f"   → Win prob ~ {ip3:.3f}   US odds ≈ {us3}")
if not parlay4.empty:
    show(parlay4, "3B) PARLAY (4 legs)")
    print(f"   → Win prob ~ {ip4:.3f}   US odds ≈ {us4}")

print("\n4) NOTES")
print("   • EV uses cross-book median (vig-light).")
print("   • No-same-game rule used in example parlays to reduce correlation.")
print(f"   • Timestamp (UTC): {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
