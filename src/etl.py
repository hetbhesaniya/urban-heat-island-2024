import os
import numpy as np
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(__file__))
RAW = os.path.join(ROOT, "data", "raw", "temperatures.csv")
PROC = os.path.join(ROOT, "data", "processed")
TAB  = os.path.join(ROOT, "reports", "tableau")
os.makedirs(PROC, exist_ok=True)
os.makedirs(TAB, exist_ok=True)

def robust_z(series: pd.Series) -> pd.Series:
    x = series.astype(float)
    med = np.nanmedian(x)
    mad = np.nanmedian(np.abs(x - med))
    if not np.isfinite(mad) or mad == 0:
        return pd.Series(np.zeros(len(x)), index=series.index, dtype=float)
    return 0.6745 * (x - med) / mad

def main():
    # 1) load + basic time fields
    df = pd.read_csv(RAW)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.sort_values(["zone_id","timestamp"]).drop_duplicates(["zone_id","timestamp"])
    df["date"]    = df["timestamp"].dt.floor("D")
    df["hour"]    = df["timestamp"].dt.hour
    df["weekday"] = df["timestamp"].dt.weekday
    df["month"]   = df["timestamp"].dt.month

    # 2) outlier handling within (zone, month) using robust z-score; replace with 24h rolling median
    def _clean(g: pd.DataFrame) -> pd.DataFrame:
        z = robust_z(g["temp_c"])
        g["is_outlier"] = np.abs(z) > 3.5
        g = g.set_index("timestamp")
        med24 = g["temp_c"].rolling("24h", min_periods=6).median()
        g["temp_c_clean"] = np.where(g["is_outlier"], med24, g["temp_c"])
        return g.reset_index()

    df = df.groupby(["zone_id","month"], group_keys=False).apply(_clean)
    df["temp_c_clean"] = df.groupby("zone_id")["temp_c_clean"].transform(lambda s: s.bfill().ffill())

    # 3) rolling windows per zone (time-aware)
    df = df.set_index("timestamp")
    def _roll(g):
        g = g.sort_index()
        g["roll24_mean"]   = g["temp_c_clean"].rolling("24h",  min_periods=6).mean()
        g["roll168_mean"]  = g["temp_c_clean"].rolling("168h", min_periods=24).mean()
        g["roll24_median"] = g["temp_c_clean"].rolling("24h",  min_periods=6).median()
        return g
    df = df.groupby("zone_id", group_keys=False).apply(_roll).reset_index()

    # 4) hourly seasonality removal (zone, weekday, hour)
    key = ["zone_id","weekday","hour"]
    seasonal = df.groupby(key)["temp_c_clean"].mean().rename("seasonal_mean").reset_index()
    hourly = df.merge(seasonal, on=key, how="left")
    hourly["deseasonalized"] = hourly["temp_c_clean"] - hourly["seasonal_mean"]
    hourly["is_night"] = hourly["hour"].isin([21,22,23,0,1,2,3,4,5])

    # 5) daily aggregates
    daily = hourly.groupby(["zone_id","date"], as_index=False).agg(
        mean_temp_c=("temp_c_clean","mean"),
        max_temp_c=("temp_c_clean","max"),
        min_temp_c=("temp_c_clean","min"),
        mean_deseasonalized=("deseasonalized","mean"),
        prop_outliers=("is_outlier","mean"),
    )

    # 6) nighttime heat retention: avg(21–05) minus prior day avg(15–18)
    night = hourly[hourly["is_night"]].groupby(["zone_id","date"], as_index=False)["temp_c_clean"].mean()
    night = night.rename(columns={"temp_c_clean":"night_avg"})
    aft   = hourly[hourly["hour"].isin([15,16,17,18])].groupby(["zone_id","date"], as_index=False)["temp_c_clean"].mean()
    aft   = aft.rename(columns={"temp_c_clean":"aft_avg"})
    aft["date"] = pd.to_datetime(aft["date"]) + pd.Timedelta(days=1)
    retention = night.merge(aft, on=["zone_id","date"], how="left")
    retention["night_retention"] = retention["night_avg"] - retention["aft_avg"]
    daily = daily.merge(retention[["zone_id","date","night_retention"]], on=["zone_id","date"], how="left")

    # 7) hotspots: top 5% deseasonalized each zone
    def _hot(g):
        thr = g["deseasonalized"].quantile(0.95)
        return g[g["deseasonalized"] >= thr]
    hotspots = hourly.groupby("zone_id", group_keys=False).apply(_hot)[
        ["timestamp","zone_id","temp_c_clean","deseasonalized","hour","weekday"]
    ].rename(columns={"temp_c_clean":"temp_c"})

    # 8) save — parquet for analysts; CSV for Tableau
    try:
        hourly.to_parquet(os.path.join(PROC,"zone_hourly.parquet"), index=False)
        daily.to_parquet(os.path.join(PROC,"zone_daily.parquet"), index=False)
    except Exception:
        hourly.to_csv(os.path.join(PROC,"zone_hourly.csv"), index=False)
        daily.to_csv(os.path.join(PROC,"zone_daily.csv"), index=False)

    he = hourly.copy()
    he["timestamp"] = he["timestamp"].dt.tz_convert(None)  # naive UTC for Tableau
    he.to_csv(os.path.join(TAB,"zone_hourly.csv"), index=False)

    de = daily.copy()
    de["date"] = pd.to_datetime(de["date"]).dt.date.astype(str)
    de.to_csv(os.path.join(TAB,"zone_daily.csv"), index=False)

    hs = hotspots.copy()
    hs["timestamp"] = hs["timestamp"].dt.tz_convert(None)
    hs.to_csv(os.path.join(TAB,"hotspots.csv"), index=False)

    # 9) intervention windows: coolest & least anomalous hours per zone
    hour_stats = hourly.groupby(["zone_id","hour"], as_index=False).agg(
        avg_temp=("temp_c_clean","mean"),
        avg_deseasonalized=("deseasonalized","mean")
    )
    ret_summary = retention.groupby("zone_id", as_index=False)["night_retention"].mean()
    hour_stats = hour_stats.merge(ret_summary, on="zone_id", how="left")
    hour_stats["rank_coolest"] = hour_stats.groupby("zone_id")["avg_temp"].rank(method="dense", ascending=True)
    hour_stats["rank_low_deseason"] = hour_stats.groupby("zone_id")["avg_deseasonalized"].rank(method="dense", ascending=True)
    hour_stats["suggested_window_score"] = (hour_stats["rank_coolest"] + hour_stats["rank_low_deseason"]) / 2.0
    hour_stats.sort_values(["zone_id","suggested_window_score","hour"], inplace=True)
    hour_stats.to_csv(os.path.join(TAB,"intervention_windows.csv"), index=False)

    print("ETL complete -> reports/tableau/{zone_hourly,zone_daily,hotspots,intervention_windows}.csv")

if __name__ == "__main__":
    main()
