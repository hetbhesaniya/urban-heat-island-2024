import os
import pandas as pd
from datetime import datetime
from meteostat import Stations, Hourly

ROOT = os.path.dirname(os.path.dirname(__file__))
CITIES_DEFAULT = os.path.join(ROOT, "data", "external", "uhi_cities.csv")
OUT = os.path.join(ROOT, "data", "raw", "temperatures.csv")

def fetch_city_hourly(lat, lon, start_dt, end_dt, radius_km=60, top_n=3, min_cov=0.85, force_nearest=False):
    st_all = Stations().nearby(lat, lon).fetch(50)        # station ID is the INDEX
    if st_all.empty:
        return pd.DataFrame(columns=["timestamp", "temp_c"])

    # filter by radius; fallback to global nearest if empty
    st = st_all[st_all["distance"] <= radius_km * 1000] if "distance" in st_all.columns else st_all
    if st.empty:
        st = st_all

    st = st.head(max(1, top_n))

    # hourly range in UTC; use 'h' (lowercase)
    full = pd.date_range(start=start_dt, end=end_dt, freq="h", tz="UTC").rename("timestamp")

    series = []
    # >>> get station IDs from the INDEX, not a column
    station_ids = st.index.tolist()
    for sid in station_ids:
        h = Hourly(sid, start_dt, end_dt, timezone="UTC").fetch()
        if h.empty or "temp" not in h.columns:
            continue
        h.index = h.index.tz_convert("UTC")
        h.index.name = "timestamp"
        s = h["temp"].rename(f"temp_{sid}").reindex(full)
        cov = s.notna().mean()
        if cov >= min_cov:
            series.append(s)

    if not series and (force_nearest or len(station_ids) == 1):
        sid = station_ids[0]
        h = Hourly(sid, start_dt, end_dt, timezone="UTC").fetch()
        if not h.empty and "temp" in h.columns:
            h.index = h.index.tz_convert("UTC")
            h.index.name = "timestamp"
            series.append(h["temp"].rename(f"temp_{sid}").reindex(full))

    if not series:
        return pd.DataFrame(columns=["timestamp", "temp_c"])

    stack = pd.concat(series, axis=1)
    mean_c = stack.mean(axis=1, skipna=True).to_frame("temp_c")
    mean_c.index.name = "timestamp"
    return mean_c.reset_index()

def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--cities", default=CITIES_DEFAULT)
    p.add_argument("--name-col", default="City Name")
    p.add_argument("--lat-col", default="Latitude")
    p.add_argument("--lon-col", default="Longitude")
    p.add_argument("--start", default="2024-01-01")
    p.add_argument("--end", default="2024-12-31 23:00")
    p.add_argument("--radius_km", type=int, default=60)
    p.add_argument("--top_n", type=int, default=3)
    p.add_argument("--min_coverage", type=float, default=0.85)
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--force-nearest", dest="force_nearest", action="store_true")
    args = p.parse_args()

    start_dt = datetime.fromisoformat(args.start)
    end_dt   = datetime.fromisoformat(args.end)

    cities = pd.read_csv(args.cities)
    for c in [args.name_col, args.lat_col, args.lon_col]:
        if c not in cities.columns:
            raise SystemExit(f"Missing column: {c}")

    if args.limit:
        cities = cities.head(args.limit)

    out_frames = []
    for i, row in cities.iterrows():
        name = str(row[args.name_col]); lat = float(row[args.lat_col]); lon = float(row[args.lon_col])
        print(f"[{int(i)+1}/{len(cities)}] {name}: ({lat:.4f}, {lon:.4f})")
        df = fetch_city_hourly(lat, lon, start_dt, end_dt,
                               radius_km=args.radius_km, top_n=args.top_n,
                               min_cov=args.min_coverage, force_nearest=args.force_nearest)
        if df.empty:
            print("  -> skipped (no usable station data)")
            continue
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert("UTC").dt.strftime("%Y-%m-%d %H:%M:%S")
        df["zone_id"] = name
        out_frames.append(df[["timestamp","zone_id","temp_c"]])

    if not out_frames:
        raise SystemExit("No cities produced data. Try --radius_km 500, --top_n 1, --min_coverage 0.5, --force-nearest.")
    final = pd.concat(out_frames, ignore_index=True).sort_values(["zone_id","timestamp"])
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    final.to_csv(OUT, index=False)
    print(f"Wrote {len(final):,} rows -> {OUT}")

if __name__ == "__main__":
    main()
