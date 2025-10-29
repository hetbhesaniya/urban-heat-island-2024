# Urban Heat Island — Phoenix 2024

**Live dashboard:** [<TABLEAU_LINK>](https://public.tableau.com/app/profile/het.bhesaniya/viz/Book1_17425054766780/Dashboard1?publish=yes)

## What’s inside
- **Hourly trend** (cleaned) + **24h moving average**
- **Weekday × hour anomaly heat map** (baseline removed per weekday×hour; center = 0 °C)
- **Nighttime heat retention** (21:00–05:00 minus prior 15:00–18:00), shown as **7-day MA (weekly)**
- **Hot hours timeline** with an **anomaly threshold** slider

## Data & method (brief)
- **Source:** Meteostat hourly weather for Phoenix, 2024 (via `meteostat` Python library)
- **Cleaning/ETL (src/etl.py):**
  - outlier handling
  - 24h rolling mean & deseasonalization by weekday×hour
  - daily aggregates for retention metric
- **Tableau inputs:** `reports/tableau/zone_hourly.csv`, `reports/tableau/zone_daily.csv`, `reports/tableau/hotspots.csv`

## Key Insights (Phoenix 2024)

- **Urban heat persistence:** nighttime heat retention stays positive through June–September, showing slower cooling overnight.  
- **Peak heat hours:** 14:00–18:00 show the highest hourly anomalies, especially on weekdays.  
- **Weekday vs weekend:** minimal difference—urban form, not traffic, drives most of the heat.  
- **Hot hours:** anomalies ≥ 2 °C cluster in late afternoons across summer months.  
- **Intervention window:** 05:00–08:00 remains the coolest and best for outdoor activity or cooling strategies.  

These trends confirm the classic *Urban Heat Island* pattern—high afternoon peaks and sustained overnight warmth in dense city zones.

## Reproduce locally
```bash
# macOS / Python 3.12
python3.12 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Build processed tables + Tableau CSVs
python src/etl.py
