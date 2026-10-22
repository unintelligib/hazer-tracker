"""Short-range forecast of island-average PM2.5, with a likely range.

Ridge regression on the latest reading, recent lags and the target hour of day, one
model per horizon. The likely range is scaled by the current level: the errors of a
calibration model are measured on the most recent part of the history, as a fraction
of max(now, LEVEL_FLOOR), and their tail quantiles are applied to the new forecast.

This is the method evaluated in notebooks/04_forecast_baseline.py. It is plain numpy
and pandas, so it can be tested without Spark. Used by notebooks/06_forecast.py.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

LAGS = (1, 2, 3, 6, 24)
FEATURES = ["now"] + [f"lag{lag}" for lag in LAGS] + ["sin_hour", "cos_hour"]
HOUR = pd.Timedelta(hours=1)
LEVEL_FLOOR = 5.0  # ug/m3; keeps the range from collapsing to zero width on very clean hours
CAL_SHARE = 0.2  # latest share of history used to measure the range
MIN_ROWS = 100  # fewer usable rows than this and a horizon is skipped


def tail_for(horizon_h: int) -> float:
    """Tail probability on each side of the likely range (wider at longer horizons)."""
    return 0.10 if horizon_h <= 3 else 0.05


def hourly_series(raw: pd.DataFrame) -> tuple[pd.Series, pd.Series, int]:
    """Put pm25_avg on a complete hourly index.

    ``raw`` needs columns reading_ts, reading_hour_sgt and pm25_avg. A missing hour becomes
    NaN, so lags never line up with the wrong hour. Returns (series, sgt_hour, off_grid),
    where off_grid counts readings not exactly on the hour (these are dropped).
    """
    raw = raw.drop_duplicates("reading_ts").sort_values("reading_ts").reset_index(drop=True)
    off_grid = int((raw["reading_ts"] != raw["reading_ts"].dt.floor("60min")).sum())
    idx = pd.date_range(raw["reading_ts"].min(), raw["reading_ts"].max(), freq=HOUR)
    series = raw.set_index("reading_ts")["pm25_avg"].astype(float).reindex(idx)
    first_hour = int(raw["reading_hour_sgt"].iloc[0])
    sgt_hour = pd.Series((first_hour + np.arange(len(idx))) % 24, index=idx)
    return series, sgt_hour, off_grid


def build_frame(series: pd.Series, sgt_hour: pd.Series, h: int) -> pd.DataFrame:
    """One row per forecast origin t. The target is the reading at t + h."""
    d = pd.DataFrame({"now": series})
    for lag in LAGS:
        d[f"lag{lag}"] = series.shift(lag)
    target_hour = (sgt_hour + h) % 24
    d["sin_hour"] = np.sin(2 * np.pi * target_hour / 24)
    d["cos_hour"] = np.cos(2 * np.pi * target_hour / 24)
    d["target"] = series.shift(-h)
    return d


def fit_ridge(X: np.ndarray, y: np.ndarray, alpha: float):
    mu = X.mean(axis=0)
    sd = X.std(axis=0)
    sd[sd == 0] = 1.0
    Z = np.column_stack([np.ones(len(X)), (X - mu) / sd])
    penalty = alpha * np.eye(Z.shape[1])
    penalty[0, 0] = 0.0  # do not penalise the intercept
    w = np.linalg.solve(Z.T @ Z + penalty, Z.T @ y)
    return mu, sd, w


def predict_ridge(model, X: np.ndarray) -> np.ndarray:
    mu, sd, w = model
    Z = np.column_stack([np.ones(len(X)), (X - mu) / sd])
    return Z @ w


@dataclass
class HorizonModel:
    horizon_h: int
    model: tuple
    q_lo: float
    q_hi: float
    train_rows: int


def fit_horizon(series: pd.Series, sgt_hour: pd.Series, h: int, alpha: float) -> HorizonModel | None:
    """Fit the forecast model and its likely range for one horizon. None if too little data."""
    d = build_frame(series, sgt_hour, h).dropna(subset=FEATURES + ["target"])
    if len(d) < MIN_ROWS:
        return None
    model = fit_ridge(d[FEATURES].to_numpy(), d["target"].to_numpy(), alpha)

    # Range: a second model fit on the earlier history, its errors measured on the latest
    # CAL_SHARE. Rows whose target falls inside the calibration period are left out of the fit.
    cal_start = d.index[int(len(d) * (1 - CAL_SHARE))]
    fit = d[d.index + h * HOUR < cal_start]
    cal = d[d.index >= cal_start]
    cal_model = fit_ridge(fit[FEATURES].to_numpy(), fit["target"].to_numpy(), alpha)
    level = np.maximum(cal["now"].to_numpy(), LEVEL_FLOOR)
    rel_resid = (cal["target"].to_numpy() - predict_ridge(cal_model, cal[FEATURES].to_numpy())) / level
    tail = tail_for(h)
    q_lo, q_hi = np.quantile(rel_resid, [tail, 1 - tail])
    return HorizonModel(h, model, float(q_lo), float(q_hi), len(d))


def forecast_latest(series: pd.Series, sgt_hour: pd.Series, horizons, alpha: float):
    """Forecast every horizon from the latest hour that has a reading.

    Returns (origin_ts, rows, skipped). rows is a list of dicts, one per horizon. skipped
    maps a horizon to the reason it was not forecast. Forecast and lower bound are kept
    at or above zero.
    """
    origin = series.last_valid_index()
    rows, skipped = [], {}
    if origin is None:
        return None, rows, {h: "no readings" for h in horizons}
    for h in horizons:
        x = build_frame(series, sgt_hour, h).loc[origin, FEATURES]
        if x.isna().any():
            missing = ", ".join(x.index[x.isna()])
            skipped[h] = f"latest hour is missing {missing}"
            continue
        hm = fit_horizon(series, sgt_hour, h, alpha)
        if hm is None:
            skipped[h] = f"fewer than {MIN_ROWS} usable rows"
            continue
        now = float(x["now"])
        pred = float(predict_ridge(hm.model, x.to_numpy(dtype=float)[None, :])[0])
        level = max(now, LEVEL_FLOOR)
        rows.append({
            "horizon_h": int(h),
            "pm25_now": now,
            "pm25_forecast": max(pred, 0.0),
            "pm25_lower": max(pred + hm.q_lo * level, 0.0),
            "pm25_upper": max(pred + hm.q_hi * level, 0.0),
            "range_tail": tail_for(h),
            "train_rows": hm.train_rows,
        })
    return origin, rows, skipped
