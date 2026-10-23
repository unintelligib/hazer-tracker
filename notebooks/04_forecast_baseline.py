# Databricks notebook source
# MAGIC %md
# MAGIC # Forecast baseline evaluation
# MAGIC
# MAGIC Question: does a simple model beat "the next hours look like now" when forecasting the island-average PM2.5 (`pm25_avg` in `gold_hourly_island`)?
# MAGIC
# MAGIC - Baselines: **persistence** (forecast = latest reading) and **same hour yesterday** (the reading 24 hours before the target hour).
# MAGIC - Model: ridge regression on recent lags and the target hour of day, one model per horizon.
# MAGIC - The split is by time. The last part of the history is held out, and training rows whose target falls inside the test period are dropped, so nothing leaks from test into training.
# MAGIC - Metrics: MAE and RMSE in µg/m³, on the same test rows for every method.
# MAGIC
# MAGIC This notebook only reads data. It does not write any tables.

# COMMAND ----------

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor

dbutils.widgets.text("horizons", "1,3,6,24", "Forecast horizons in hours (max 24)")
dbutils.widgets.text("test_fraction", "0.25", "Share of history held out for testing")
dbutils.widgets.text("ridge_alpha", "1.0", "Ridge penalty")
dbutils.widgets.text("high_pm25", "56", "High-hour threshold, ug/m3 (start of NEA 1-hr PM2.5 Elevated band)")

HORIZONS = [int(h) for h in dbutils.widgets.get("horizons").split(",")]
TEST_FRACTION = float(dbutils.widgets.get("test_fraction"))
RIDGE_ALPHA = float(dbutils.widgets.get("ridge_alpha"))
HIGH_PM25 = float(dbutils.widgets.get("high_pm25"))
assert all(1 <= h <= 24 for h in HORIZONS), "horizons must be between 1 and 24"
assert 0.05 <= TEST_FRACTION <= 0.6, "test_fraction should be between 0.05 and 0.6"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load and describe the data

# COMMAND ----------

raw = spark.sql("""
    SELECT reading_ts, reading_hour_sgt, pm25_avg, regions_reported
    FROM workspace.hazer.gold_hourly_island
""").toPandas()
raw = raw.drop_duplicates("reading_ts").sort_values("reading_ts").reset_index(drop=True)

HOUR = pd.Timedelta(hours=1)
idx = pd.date_range(raw["reading_ts"].min(), raw["reading_ts"].max(), freq=HOUR)

# Readings are expected exactly on the hour. Anything off the hour would be silently
# dropped by the reindex below, so count them first.
off_grid = int((raw["reading_ts"] != raw["reading_ts"].dt.floor("60min")).sum())
print(f"Readings not on the hour: {off_grid} (expect 0)")

# Put the series on a complete hourly index. A missing hour becomes NaN, so lags and
# targets never silently line up with the wrong hour.
series = raw.set_index("reading_ts")["pm25_avg"].astype(float).reindex(idx)

# SGT hour of day for every step of the complete index, counted from the first row.
sgt_hour = pd.Series((int(raw["reading_hour_sgt"].iloc[0]) + np.arange(len(idx))) % 24, index=idx)

n_days = (idx[-1] - idx[0]) / pd.Timedelta(days=1)
print(f"First reading: {idx[0]}   Last reading: {idx[-1]}   (timestamps as returned by Spark)")
print(f"Hours in range: {len(idx)}  (about {n_days:.1f} days)")
print(f"Hours with a PM2.5 value: {int(series.notna().sum())}")
print(f"Missing hours: {int(series.isna().sum())}")
print()
print("regions_reported per hour (number of hours at each count):")
print(raw["regions_reported"].value_counts().sort_index().to_string())
print()
print("pm25_avg summary (ug/m3):")
print(series.describe().round(1).to_string())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Features, baselines and a ridge model

# COMMAND ----------

LAGS = (1, 2, 3, 6, 24)
FEATURES = ["now"] + [f"lag{lag}" for lag in LAGS] + ["sin_hour", "cos_hour"]


def build_frame(h):
    """One row per forecast origin t. The target is the reading at t + h."""
    d = pd.DataFrame({"now": series})
    for lag in LAGS:
        d[f"lag{lag}"] = series.shift(lag)
    target_hour = (sgt_hour + h) % 24
    d["sin_hour"] = np.sin(2 * np.pi * target_hour / 24)
    d["cos_hour"] = np.cos(2 * np.pi * target_hour / 24)
    d["target"] = series.shift(-h)
    # Same hour yesterday, relative to the target time: the reading at t + h - 24.
    # For h = 24 this is the reading at t, so it matches persistence exactly.
    d["yesterday"] = series.shift(24 - h)
    return d


def fit_ridge(X, y, alpha):
    mu = X.mean(axis=0)
    sd = X.std(axis=0)
    sd[sd == 0] = 1.0
    Z = np.column_stack([np.ones(len(X)), (X - mu) / sd])
    penalty = alpha * np.eye(Z.shape[1])
    penalty[0, 0] = 0.0  # do not penalise the intercept
    w = np.linalg.solve(Z.T @ Z + penalty, Z.T @ y)
    return mu, sd, w


def predict_ridge(model, X):
    mu, sd, w = model
    Z = np.column_stack([np.ones(len(X)), (X - mu) / sd])
    return Z @ w


def mae(a, b):
    return float(np.mean(np.abs(a - b)))


def rmse(a, b):
    return float(np.sqrt(np.mean((a - b) ** 2)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Evaluate on the held-out period

# COMMAND ----------

rows = []
for h in HORIZONS:
    d = build_frame(h).dropna(subset=FEATURES + ["target", "yesterday"])
    if len(d) < 30:
        print(f"+{h}h: only {len(d)} usable rows, skipping")
        continue

    split_ts = d.index[int(len(d) * (1 - TEST_FRACTION))]
    train = d[d.index + h * HOUR < split_ts]  # target must also fall before the test period
    test = d[d.index >= split_ts]
    if len(train) < 20 or len(test) < 5:
        print(f"+{h}h: train rows {len(train)}, test rows {len(test)}, skipping")
        continue

    model = fit_ridge(train[FEATURES].to_numpy(), train["target"].to_numpy(), RIDGE_ALPHA)
    pred = predict_ridge(model, test[FEATURES].to_numpy())
    # Gradient boosting predicts the change from now, not the level. Tree models cannot
    # predict outside the range they were trained on, and haze levels in the test period
    # can exceed anything in training; changes are much more alike across episodes.
    gbm = HistGradientBoostingRegressor(
        learning_rate=0.05, max_iter=300, max_leaf_nodes=15, min_samples_leaf=20, random_state=0
    )
    gbm.fit(train[FEATURES].to_numpy(), (train["target"] - train["now"]).to_numpy())
    pred_gbm = test["now"].to_numpy() + gbm.predict(test[FEATURES].to_numpy())
    y = test["target"].to_numpy()
    cal_start = train.index[int(len(train) * 0.8)]
    fit = train[train.index + h * HOUR < cal_start]
    cal = train[train.index >= cal_start]
    cal_model = fit_ridge(fit[FEATURES].to_numpy(), fit["target"].to_numpy(), RIDGE_ALPHA)
    cal_level = np.maximum(cal["now"].to_numpy(), 5.0)
    rel_resid = (cal["target"].to_numpy() - predict_ridge(cal_model, cal[FEATURES].to_numpy())) / cal_level
    tail = 0.10 if h <= 3 else 0.05
    q_lo, q_hi = np.quantile(rel_resid, [tail, 1 - tail])
    test_level = np.maximum(test["now"].to_numpy(), 5.0)
    lower, upper = pred + q_lo * test_level, pred + q_hi * test_level
    coverage = float(np.mean((y >= lower) & (y <= upper)))
    # Same errors on high hours only: test hours whose actual reading is at or above HIGH_PM25.
    high = y >= HIGH_PM25
    n_high = int(high.sum())
    now_test = test["now"].to_numpy()

    rows.append({
        "horizon_h": h,
        "train_rows": len(train),
        "test_rows": len(test),
        "mae_persistence": mae(y, test["now"].to_numpy()),
        "mae_yesterday": mae(y, test["yesterday"].to_numpy()),
        "mae_ridge": mae(y, pred),
        "mae_gbm": mae(y, pred_gbm),
        "rmse_persistence": rmse(y, test["now"].to_numpy()),
        "rmse_ridge": rmse(y, pred),
        "coverage": coverage,
        "range_width": float(np.mean(upper - lower)),
        "high_rows": n_high,
        "mae_persistence_high": mae(y[high], now_test[high]) if n_high else np.nan,
        "mae_ridge_high": mae(y[high], pred[high]) if n_high else np.nan,
        "mae_gbm_high": mae(y[high], pred_gbm[high]) if n_high else np.nan,
        "coverage_high": float(np.mean((y[high] >= lower[high]) & (y[high] <= upper[high]))) if n_high else np.nan,
    })

results = pd.DataFrame(rows)
if results.empty:
    print("No horizon had enough usable history. Backfill more days (aim for at least 7) and re-run.")
else:
    # Positive means ridge has a lower MAE than persistence.
    results["ridge_vs_persistence"] = 1 - results["mae_ridge"] / results["mae_persistence"]
    results["ridge_vs_persistence_high"] = 1 - results["mae_ridge_high"] / results["mae_persistence_high"]
    results["gbm_vs_persistence"] = 1 - results["mae_gbm"] / results["mae_persistence"]
    results["gbm_vs_persistence_high"] = 1 - results["mae_gbm_high"] / results["mae_persistence_high"]
    display(results.round(3))
    if results["test_rows"].min() < 48:
        print("Warning: fewer than 48 test rows at some horizon. Treat these results as indicative only.")
    print("Note: at +24h the 'yesterday' baseline is identical to persistence.")
    print(f"High hours: actual pm25_avg >= {HIGH_PM25:g}. Fewer than ~30 high_rows at a horizon means those columns are noisy.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading the table
# MAGIC
# MAGIC - `ridge_vs_persistence` above 0 means the model has a lower average error than persistence at that horizon. Near 0 means no real gain.
# MAGIC - Persistence is hard to beat at +1h. A model usually only wins clearly at longer horizons.
# MAGIC - With only a few days of history the test period is small and the numbers are noisy. More backfilled days make this table more trustworthy.
# MAGIC - **This is one split, so it can mislead.** If the held-out days contain a haze episode that the training days never saw, ridge can look much worse than persistence (on made-up data with a single episode, ridge's error was sometimes 2 to 4 times persistence's). A negative number at one horizon does not prove the model is bad. Before trusting a verdict, change `test_fraction` (for example 0.2 and 0.35) and see whether the winner changes.
# MAGIC - `_high` columns repeat the comparison on test hours whose actual reading is at or above `high_pm25` (default 56, the start of NEA's 1-hr PM2.5 Elevated band). In Oct 2026 all of these hours came from one haze episode, so they describe that episode, not haze in general.
# MAGIC - **Gradient boosting was tried and rejected.** It predicts the change from now so it can forecast above its training range, but it still lost to persistence at every horizon (by 8-32% over all hours and 23-68% on high hours, test_fraction 0.25). With only one earlier haze episode (Sep 2019) to learn from, it learned that episode's quick clear-ups and kept forecasting a drop that did not come in Oct 2026. Ridge is kept.
# MAGIC - **Ridge penalty checked.** At test_fraction 0.25, `ridge_alpha` 0 (plain linear regression) scored about the same as 10 (+4.7 / +3.9 / +6.7 / +10.0% vs persistence over all hours). 100 was worse at every horizon and fell below persistence at +1h (-2.6 / +1.6 / +5.6 / +9.7% over all hours, -4 to -8% on high hours). The job keeps 10: a small penalty keeps the closely correlated lags stable.
# MAGIC - Next steps: more haze history, and weather and smoke features, tested against these numbers.
