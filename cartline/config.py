from __future__ import annotations

import hashlib
import json
import tomllib
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from pathlib import Path

MEMORY_PATTERN = ("k", "m", "g", "t")


class ConfigError(ValueError):
    pass


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ConfigError(message)


def _memory(value: str, name: str) -> str:
    text = str(value).strip().lower()
    _require(len(text) > 1, f"{name} must look like 4g, got {value!r}")
    _require(text[-1] in MEMORY_PATTERN, f"{name} must end with k, m, g or t, got {value!r}")
    _require(
        text[:-1].isdigit() and int(text[:-1]) > 0, f"{name} must be a positive size, got {value!r}"
    )
    return text


def _parse_date(value: str, name: str) -> date:
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date()
    except ValueError as exc:
        raise ConfigError(f"{name} must be YYYY-MM-DD, got {value!r}") from exc


def _rate(value: float, name: str) -> float:
    number = float(value)
    _require(0.0 <= number <= 1.0, f"{name} must be between 0 and 1, got {value!r}")
    return number


@dataclass(frozen=True)
class Paths:
    source: Path
    landing: Path
    lake: Path
    checkpoints: Path
    reports: Path

    @property
    def bronze_events(self) -> Path:
        return self.lake / "bronze" / "events"

    @property
    def bronze_quarantine(self) -> Path:
        return self.lake / "bronze" / "quarantine"

    @property
    def silver_events(self) -> Path:
        return self.lake / "silver" / "events"

    @property
    def stream_events(self) -> Path:
        return self.lake / "silver" / "stream_events"

    @property
    def stream_traffic(self) -> Path:
        return self.lake / "silver" / "stream_traffic"

    @property
    def gold(self) -> Path:
        return self.lake / "gold"

    @property
    def audit(self) -> Path:
        return self.lake / "_audit" / "runs"

    @property
    def quality(self) -> Path:
        return self.lake / "_audit" / "quality"

    def gold_table(self, name: str) -> Path:
        return self.gold / name


@dataclass(frozen=True)
class SourceConfig:
    months: tuple[str, ...]
    file_pattern: str

    def file_name(self, month: str) -> str:
        return self.file_pattern.format(month=month)


@dataclass(frozen=True)
class SparkConfig:
    app_name: str
    master: str
    driver_memory: str
    shuffle_partitions: int
    max_partition_bytes: str
    parquet_compression: str
    adaptive: bool
    adaptive_skew_join: bool
    local_dir: str
    log_level: str


@dataclass(frozen=True)
class IngestConfig:
    max_quarantine_rate: float
    max_records_per_file: int


@dataclass(frozen=True)
class SilverConfig:
    session_gap_minutes: int
    dedup_keys: tuple[str, ...]
    output_partitions: int


@dataclass(frozen=True)
class DimensionConfig:
    attributes: tuple[str, ...]


@dataclass(frozen=True)
class GoldConfig:
    rfm_buckets: int
    top_products: int
    output_partitions: int


@dataclass(frozen=True)
class StreamConfig:
    watermark_minutes: int
    max_files_per_trigger: int
    trigger: str
    processing_time_seconds: int


@dataclass(frozen=True)
class QualityConfig:
    fail_on_error: bool
    max_null_brand_rate: float
    max_null_category_rate: float


@dataclass(frozen=True)
class GenerateConfig:
    events: int
    products: int
    users: int
    seed: int
    start_date: date
    end_date: date
    output_partitions: int
    duplicate_rate: float
    null_session_rate: float
    null_brand_rate: float
    null_category_rate: float
    bad_row_rate: float
    cart_rate: float
    remove_rate: float
    purchase_rate: float
    black_friday: date
    black_friday_boost: float

    @property
    def days(self) -> int:
        return (self.end_date - self.start_date).days + 1


@dataclass(frozen=True)
class Config:
    name: str
    paths: Paths
    source: SourceConfig
    spark: SparkConfig
    ingest: IngestConfig
    silver: SilverConfig
    dimension: DimensionConfig
    gold: GoldConfig
    stream: StreamConfig
    quality: QualityConfig
    generate: GenerateConfig
    raw: dict = field(default_factory=dict, repr=False, compare=False)

    @classmethod
    def load(cls, path: str | Path = "config.toml") -> Config:
        config_path = Path(path)
        _require(config_path.is_file(), f"config file not found: {config_path}")
        with config_path.open("rb") as handle:
            raw = tomllib.load(handle)
        return cls.from_dict(raw, config_path.parent)

    @classmethod
    def from_dict(cls, raw: dict, base: str | Path = ".") -> Config:
        root = Path(base).resolve()
        for section in (
            "project",
            "paths",
            "source",
            "spark",
            "ingest",
            "silver",
            "dimension",
            "gold",
            "stream",
            "quality",
            "generate",
        ):
            _require(section in raw, f"missing [{section}] section in config")

        paths_raw = raw["paths"]
        paths = Paths(
            source=root / paths_raw["source"],
            landing=root / paths_raw["landing"],
            lake=root / paths_raw["lake"],
            checkpoints=root / paths_raw["checkpoints"],
            reports=root / paths_raw["reports"],
        )

        source_raw = raw["source"]
        months = tuple(str(month) for month in source_raw["months"])
        _require(len(months) > 0, "source.months must list at least one month")
        _require(len(set(months)) == len(months), "source.months must not repeat a month")
        _require(
            "{month}" in source_raw["file_pattern"], "source.file_pattern must contain {month}"
        )
        source = SourceConfig(months=months, file_pattern=str(source_raw["file_pattern"]))

        spark_raw = raw["spark"]
        _require(
            int(spark_raw["shuffle_partitions"]) > 0, "spark.shuffle_partitions must be positive"
        )
        _require(
            str(spark_raw["parquet_compression"]).lower() in {"snappy", "zstd", "gzip", "none"},
            "spark.parquet_compression must be snappy, zstd, gzip or none",
        )
        _require(
            str(spark_raw["log_level"]).upper()
            in {"ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN"},
            "spark.log_level is not a Spark log level",
        )
        spark = SparkConfig(
            app_name=str(spark_raw["app_name"]),
            master=str(spark_raw["master"]),
            driver_memory=_memory(spark_raw["driver_memory"], "spark.driver_memory"),
            shuffle_partitions=int(spark_raw["shuffle_partitions"]),
            max_partition_bytes=_memory(
                spark_raw["max_partition_bytes"], "spark.max_partition_bytes"
            ),
            parquet_compression=str(spark_raw["parquet_compression"]).lower(),
            adaptive=bool(spark_raw["adaptive"]),
            adaptive_skew_join=bool(spark_raw["adaptive_skew_join"]),
            local_dir=(root / str(spark_raw["local_dir"])).as_posix()
            if str(spark_raw["local_dir"])
            else "",
            log_level=str(spark_raw["log_level"]).upper(),
        )

        ingest_raw = raw["ingest"]
        ingest = IngestConfig(
            max_quarantine_rate=_rate(
                ingest_raw["max_quarantine_rate"], "ingest.max_quarantine_rate"
            ),
            max_records_per_file=int(ingest_raw["max_records_per_file"]),
        )
        _require(ingest.max_records_per_file > 0, "ingest.max_records_per_file must be positive")

        silver_raw = raw["silver"]
        dedup_keys = tuple(str(key) for key in silver_raw["dedup_keys"])
        _require(len(dedup_keys) > 0, "silver.dedup_keys must not be empty")
        silver = SilverConfig(
            session_gap_minutes=int(silver_raw["session_gap_minutes"]),
            dedup_keys=dedup_keys,
            output_partitions=int(silver_raw["output_partitions"]),
        )
        _require(silver.session_gap_minutes > 0, "silver.session_gap_minutes must be positive")
        _require(silver.output_partitions > 0, "silver.output_partitions must be positive")

        attributes = tuple(str(name) for name in raw["dimension"]["attributes"])
        _require(len(attributes) > 0, "dimension.attributes must not be empty")
        dimension = DimensionConfig(attributes=attributes)

        gold_raw = raw["gold"]
        gold = GoldConfig(
            rfm_buckets=int(gold_raw["rfm_buckets"]),
            top_products=int(gold_raw["top_products"]),
            output_partitions=int(gold_raw["output_partitions"]),
        )
        _require(2 <= gold.rfm_buckets <= 10, "gold.rfm_buckets must be between 2 and 10")
        _require(gold.top_products > 0, "gold.top_products must be positive")
        _require(gold.output_partitions > 0, "gold.output_partitions must be positive")

        stream_raw = raw["stream"]
        _require(
            str(stream_raw["trigger"]) in {"availableNow", "processingTime"},
            "stream.trigger must be availableNow or processingTime",
        )
        stream = StreamConfig(
            watermark_minutes=int(stream_raw["watermark_minutes"]),
            max_files_per_trigger=int(stream_raw["max_files_per_trigger"]),
            trigger=str(stream_raw["trigger"]),
            processing_time_seconds=int(stream_raw["processing_time_seconds"]),
        )
        _require(stream.watermark_minutes > 0, "stream.watermark_minutes must be positive")
        _require(stream.max_files_per_trigger > 0, "stream.max_files_per_trigger must be positive")
        _require(
            stream.processing_time_seconds > 0, "stream.processing_time_seconds must be positive"
        )

        quality_raw = raw["quality"]
        quality = QualityConfig(
            fail_on_error=bool(quality_raw["fail_on_error"]),
            max_null_brand_rate=_rate(
                quality_raw["max_null_brand_rate"], "quality.max_null_brand_rate"
            ),
            max_null_category_rate=_rate(
                quality_raw["max_null_category_rate"], "quality.max_null_category_rate"
            ),
        )

        generate_raw = raw["generate"]
        start = _parse_date(generate_raw["start_date"], "generate.start_date")
        end = _parse_date(generate_raw["end_date"], "generate.end_date")
        _require(end >= start, "generate.end_date must not be before generate.start_date")
        black_friday = _parse_date(generate_raw["black_friday"], "generate.black_friday")
        _require(
            start <= black_friday <= end, "generate.black_friday must fall inside the date range"
        )
        generate = GenerateConfig(
            events=int(generate_raw["events"]),
            products=int(generate_raw["products"]),
            users=int(generate_raw["users"]),
            seed=int(generate_raw["seed"]),
            start_date=start,
            end_date=end,
            output_partitions=int(generate_raw["output_partitions"]),
            duplicate_rate=_rate(generate_raw["duplicate_rate"], "generate.duplicate_rate"),
            null_session_rate=_rate(
                generate_raw["null_session_rate"], "generate.null_session_rate"
            ),
            null_brand_rate=_rate(generate_raw["null_brand_rate"], "generate.null_brand_rate"),
            null_category_rate=_rate(
                generate_raw["null_category_rate"], "generate.null_category_rate"
            ),
            bad_row_rate=_rate(generate_raw["bad_row_rate"], "generate.bad_row_rate"),
            cart_rate=_rate(generate_raw["cart_rate"], "generate.cart_rate"),
            remove_rate=_rate(generate_raw["remove_rate"], "generate.remove_rate"),
            purchase_rate=_rate(generate_raw["purchase_rate"], "generate.purchase_rate"),
            black_friday=black_friday,
            black_friday_boost=float(generate_raw["black_friday_boost"]),
        )
        _require(generate.events > 0, "generate.events must be positive")
        _require(generate.products > 0, "generate.products must be positive")
        _require(generate.users > 0, "generate.users must be positive")
        _require(generate.output_partitions > 0, "generate.output_partitions must be positive")
        _require(
            generate.black_friday_boost >= 1.0, "generate.black_friday_boost must be at least 1"
        )
        _require(
            generate.cart_rate + generate.remove_rate + generate.purchase_rate < 1.0,
            "generate cart, remove and purchase rates must sum to less than 1",
        )

        return cls(
            name=str(raw["project"]["name"]),
            paths=paths,
            source=source,
            spark=spark,
            ingest=ingest,
            silver=silver,
            dimension=dimension,
            gold=gold,
            stream=stream,
            quality=quality,
            generate=generate,
            raw=raw,
        )

    @property
    def fingerprint(self) -> str:
        payload = json.dumps(self.raw, sort_keys=True, default=str).encode()
        return hashlib.sha256(payload).hexdigest()[:12]

    def as_dict(self) -> dict:
        return asdict(self)
