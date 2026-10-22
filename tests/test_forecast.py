import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from hazer import forecast as fc  # noqa: E402


def make_raw(n_hours=24 * 40, seed=0, start="2026-08-01 00:00", start_hour_sgt=8):
    """Synthetic hourly PM2.5: daily cycle plus AR(1) noise, always positive."""
    rng = np.random.default_rng(seed)
    ts = pd.date_range(start, periods=n_hours, freq="h")
    noise = np.zeros(n_hours)
    for i in range(1, n_hours):
        noise[i] = 0.9 * noise[i - 1] + rng.normal(0, 2)
    hour_sgt = (start_hour_sgt + np.arange(n_hours)) % 24
    pm25 = 20 + 5 * np.sin(2 * np.pi * hour_sgt / 24) + noise
    return pd.DataFrame({"reading_ts": ts, "reading_hour_sgt": hour_sgt, "pm25_avg": np.clip(pm25, 1, None)})


def test_hourly_series_fills_gaps_with_nan():
    raw = make_raw(48).drop(index=[10, 11])
    series, sgt_hour, off_grid = fc.hourly_series(raw)
    assert len(series) == 48
    assert series.isna().sum() == 2
    assert off_grid == 0
    assert sgt_hour.iloc[0] == 8 and sgt_hour.iloc[16] == 0


def test_build_frame_aligns_lags_and_target():
    raw = make_raw(60)
    series, sgt_hour, _ = fc.hourly_series(raw)
    d = fc.build_frame(series, sgt_hour, 3)
    t = series.index[30]
    assert d.loc[t, "lag24"] == series.iloc[6]
    assert d.loc[t, "target"] == series.iloc[33]


def test_forecast_rows_are_ordered_and_non_negative():
    series, sgt_hour, _ = fc.hourly_series(make_raw())
    origin, rows, skipped = fc.forecast_latest(series, sgt_hour, [1, 3, 6, 24], alpha=10)
    assert origin == series.index[-1]
    assert skipped == {}
    assert [r["horizon_h"] for r in rows] == [1, 3, 6, 24]
    for r in rows:
        assert 0 <= r["pm25_lower"] <= r["pm25_forecast"] <= r["pm25_upper"]
        assert r["pm25_now"] == pytest.approx(series.iloc[-1])


def test_range_is_wider_at_longer_horizons():
    series, sgt_hour, _ = fc.hourly_series(make_raw())
    _, rows, _ = fc.forecast_latest(series, sgt_hour, [1, 24], alpha=10)
    width = {r["horizon_h"]: r["pm25_upper"] - r["pm25_lower"] for r in rows}
    assert width[24] > width[1]


def test_skips_when_a_lag_of_the_latest_hour_is_missing():
    raw = make_raw()
    raw = raw.drop(index=len(raw) - 1 - 24)  # the reading 24 hours before the latest one
    series, sgt_hour, _ = fc.hourly_series(raw)
    origin, rows, skipped = fc.forecast_latest(series, sgt_hour, [1], alpha=10)
    assert rows == []
    assert "lag24" in skipped[1]


def test_origin_is_latest_hour_with_a_reading():
    raw = make_raw()
    raw.loc[len(raw) - 1, "pm25_avg"] = np.nan  # e.g. only PSI arrived for the latest hour
    series, sgt_hour, _ = fc.hourly_series(raw)
    origin, rows, _ = fc.forecast_latest(series, sgt_hour, [1], alpha=10)
    assert origin == series.index[-2]
    assert len(rows) == 1


def test_skips_with_too_little_history():
    series, sgt_hour, _ = fc.hourly_series(make_raw(24 * 3))
    _, rows, skipped = fc.forecast_latest(series, sgt_hour, [1], alpha=10)
    assert rows == []
    assert "usable rows" in skipped[1]
