from __future__ import annotations

from datetime import date

import pytest

from cartline.cli import _dates, build_parser

COMMANDS = {
    "generate",
    "ingest",
    "silver",
    "dimension",
    "gold",
    "quality",
    "landing",
    "stream",
    "pipeline",
}


def test_parser_exposes_every_command():
    parser = build_parser()
    actions = [action for action in parser._actions if action.choices]
    assert set(actions[0].choices) == COMMANDS


def test_dates_are_parsed():
    assert _dates(["2019-10-01", "2019-11-30"]) == (date(2019, 10, 1), date(2019, 11, 30))
    assert _dates(None) == ()


def test_bad_date_is_rejected():
    with pytest.raises(ValueError):
        _dates(["01-10-2019"])


def test_ingest_takes_repeated_months():
    args = build_parser().parse_args(["ingest", "--month", "2019-Oct", "--month", "2019-Nov"])
    assert args.months == ["2019-Oct", "2019-Nov"]


def test_config_defaults_to_the_project_file():
    assert build_parser().parse_args(["quality"]).config == "config.toml"
