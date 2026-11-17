from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime

from cartline.audit import new_run_id
from cartline.config import Config
from cartline.dimension import build_dimension
from cartline.generate import generate
from cartline.gold import build_gold
from cartline.ingest import ingest
from cartline.quality import report
from cartline.session import build_session
from cartline.silver import build_silver
from cartline.stream import run_stream, seed_landing


def _dates(values: list[str] | None) -> tuple:
    if not values:
        return ()
    return tuple(datetime.strptime(value, "%Y-%m-%d").date() for value in values)


def _emit(payload) -> None:
    if isinstance(payload, list):
        for item in payload:
            print(json.dumps(item, default=str))
        return
    print(json.dumps(payload, default=str))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="cartline")
    parser.add_argument("--config", default="config.toml")
    commands = parser.add_subparsers(dest="command", required=True)

    generator = commands.add_parser("generate")
    generator.add_argument("--events", type=int, default=0)
    generator.add_argument("--force", action="store_true")

    ingester = commands.add_parser("ingest")
    ingester.add_argument("--month", action="append", dest="months")

    silver = commands.add_parser("silver")
    silver.add_argument("--date", action="append", dest="dates")

    commands.add_parser("dimension")

    gold = commands.add_parser("gold")
    gold.add_argument("--date", action="append", dest="dates")

    commands.add_parser("quality")

    landing = commands.add_parser("landing")
    landing.add_argument("--events", type=int, default=20000)
    landing.add_argument("--files", type=int, default=4)

    stream = commands.add_parser("stream")
    stream.add_argument("--timeout", type=int, default=0)

    pipeline = commands.add_parser("pipeline")
    pipeline.add_argument("--month", action="append", dest="months")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    config = Config.load(args.config)
    spark = build_session(config, args.command)
    run_id = new_run_id()
    status = 0
    try:
        if args.command == "generate":
            _emit(generate(spark, config, events=args.events, force=args.force))
        elif args.command == "ingest":
            _emit(ingest(spark, config, tuple(args.months or ()), run_id))
        elif args.command == "silver":
            _emit(build_silver(spark, config, _dates(args.dates), run_id))
        elif args.command == "dimension":
            _emit(build_dimension(spark, config, run_id))
        elif args.command == "gold":
            _emit(build_gold(spark, config, _dates(args.dates), run_id))
        elif args.command == "landing":
            _emit(seed_landing(spark, config, events=args.events, files=args.files))
        elif args.command == "stream":
            _emit(run_stream(spark, config, timeout=args.timeout, run_id=run_id))
        elif args.command == "quality":
            payload = report(spark, config, run_id)
            _emit({key: payload[key] for key in ("run_id", "passed", "failed", "warned")})
            for check in payload["checks"]:
                if not check["passed"]:
                    _emit(check)
            if payload["failed"] and config.quality.fail_on_error:
                status = 1
        elif args.command == "pipeline":
            _emit(ingest(spark, config, tuple(args.months or ()), run_id))
            _emit(build_silver(spark, config, (), run_id))
            _emit(build_gold(spark, config, (), run_id))
            payload = report(spark, config, run_id)
            _emit({key: payload[key] for key in ("run_id", "passed", "failed", "warned")})
            if payload["failed"] and config.quality.fail_on_error:
                status = 1
    finally:
        spark.stop()
    return status


if __name__ == "__main__":
    sys.exit(main())
