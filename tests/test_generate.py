from __future__ import annotations

import copy

import pytest
from pyspark.sql import functions as F

from cartline.config import Config, ConfigError
from cartline.generate import CATEGORIES, month_path, session_probabilities
from cartline.schema import EVENT_TYPES
from cartline.silver import silver_frame


def test_session_probabilities_match_the_configured_mix(config):
    probabilities = session_probabilities(config)
    assert 1.0 < probabilities["mean_size"] < 20.0
    assert 0.0 < probabilities["cart"] < 1.0
    assert 0.0 < probabilities["purchase_given_cart"] < 1.0
    assert probabilities["purchase_given_cart"] + probabilities["remove_given_cart"] <= 1.0


def test_impossible_mix_is_rejected(raw_config):
    broken = copy.deepcopy(raw_config)
    broken["generate"]["purchase_rate"] = 0.4
    broken["generate"]["cart_rate"] = 0.05
    with pytest.raises(ConfigError):
        session_probabilities(Config.from_dict(broken, "."))


def test_categories_have_sane_price_bands():
    for code, weight, low, high in CATEGORIES:
        assert weight > 0
        assert 0 < low < high
        assert code == code.lower()


def test_month_path_points_at_generated_data(lake):
    path = month_path(lake, lake.source.months[0])
    assert path.exists()


def test_generated_mix_is_close_to_the_config(lake, spark):
    events = silver_frame(spark, lake)
    total = events.count()
    counts = {
        row["event_type"]: row["count"] for row in events.groupBy("event_type").count().collect()
    }
    assert set(counts) <= set(EVENT_TYPES)
    assert counts["purchase"] / total == pytest.approx(lake.generate.purchase_rate, abs=0.01)
    assert counts["cart"] / total == pytest.approx(lake.generate.cart_rate, abs=0.01)
    assert counts["view"] / total > 0.85


def test_black_friday_is_the_busiest_day(lake, spark):
    events = silver_frame(spark, lake)
    busiest = events.groupBy("event_date").count().orderBy(F.desc("count")).first()["event_date"]
    assert busiest == lake.generate.black_friday
