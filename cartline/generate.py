from __future__ import annotations

import hashlib
from datetime import date, timedelta
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from cartline.config import Config, ConfigError
from cartline.lake import exists, spark_path

CATEGORIES = (
    ("electronics.smartphone", 22.0, 90.0, 1600.0),
    ("electronics.audio.headphone", 6.0, 8.0, 420.0),
    ("electronics.video.tv", 5.0, 150.0, 2400.0),
    ("electronics.clocks", 4.0, 20.0, 700.0),
    ("electronics.tablet", 3.0, 80.0, 1100.0),
    ("electronics.telephone", 2.0, 15.0, 120.0),
    ("computers.notebook", 6.0, 250.0, 2600.0),
    ("computers.desktop", 2.0, 200.0, 2000.0),
    ("computers.peripherals.mouse", 2.0, 5.0, 90.0),
    ("computers.peripherals.keyboard", 1.5, 8.0, 160.0),
    ("computers.components.videocards", 1.5, 90.0, 1300.0),
    ("appliances.kitchen.refrigerators", 4.0, 180.0, 1900.0),
    ("appliances.kitchen.washer", 3.5, 150.0, 1200.0),
    ("appliances.kitchen.microwave", 2.0, 40.0, 400.0),
    ("appliances.environment.vacuum", 2.5, 30.0, 600.0),
    ("appliances.personal.hair_cutter", 1.5, 10.0, 180.0),
    ("apparel.shoes", 3.0, 15.0, 260.0),
    ("apparel.shoes.keds", 1.5, 12.0, 140.0),
    ("apparel.trousers", 1.0, 10.0, 130.0),
    ("apparel.tshirt", 1.0, 5.0, 70.0),
    ("furniture.living_room.sofa", 2.0, 200.0, 1800.0),
    ("furniture.bedroom.bed", 1.5, 120.0, 1400.0),
    ("furniture.kitchen.chair", 1.0, 20.0, 260.0),
    ("auto.accessories.player", 2.0, 30.0, 600.0),
    ("auto.accessories.alarm", 1.0, 25.0, 380.0),
    ("kids.toys", 2.5, 5.0, 220.0),
    ("kids.carriage", 1.0, 90.0, 900.0),
    ("kids.bicycle", 1.0, 60.0, 500.0),
    ("sport.bicycle", 1.5, 100.0, 1500.0),
    ("sport.trainer", 1.0, 40.0, 900.0),
    ("sport.diving", 0.5, 20.0, 700.0),
    ("construction.tools.drill", 1.5, 25.0, 450.0),
    ("construction.tools.light", 1.0, 10.0, 200.0),
    ("construction.tools.welding", 0.5, 60.0, 800.0),
    ("country_yard.furniture", 0.8, 40.0, 700.0),
    ("medicine.tools.tonometer", 0.8, 20.0, 190.0),
    ("stationery.cartrige", 1.2, 8.0, 150.0),
    ("accessories.bag", 1.0, 12.0, 300.0),
    ("accessories.wallet", 0.7, 8.0, 160.0),
    ("home.kitchen.kitchen_set", 0.9, 30.0, 500.0),
)

BRANDS = (
    "samsung",
    "apple",
    "xiaomi",
    "huawei",
    "lg",
    "sony",
    "bosch",
    "oppo",
    "lenovo",
    "acer",
    "asus",
    "hp",
    "dell",
    "canon",
    "nikon",
    "philips",
    "beko",
    "midea",
    "casio",
    "artel",
    "indesit",
    "elenberg",
    "respect",
    "lucente",
    "nike",
    "adidas",
    "puma",
    "reebok",
    "zanussi",
    "electrolux",
    "gorenje",
    "ariston",
    "candy",
    "haier",
    "tefal",
    "braun",
    "panasonic",
    "toshiba",
    "sencor",
    "polaris",
    "redmond",
    "vitek",
    "scarlett",
    "garmin",
    "jbl",
    "marshall",
    "logitech",
    "a4tech",
    "genius",
    "defender",
)

USER_ID_BASE = 500_000_000
PRODUCT_ID_BASE = 1_000_000
CATEGORY_ID_BASE = 2_053_013_000_000_000_000
DAY_BUCKETS = 20_000
HOUR_BUCKETS = 2_400
MAX_SESSION_EVENTS = 20
SESSION_SHAPE = 2.5
PRODUCT_SHAPE = 2.2
USER_SHAPE = 1.7
MIN_GAP_SECONDS = 20
GAP_SPREAD_SECONDS = 160
PRICE_CHANGING_PRODUCTS = 3

HOUR_WEIGHTS = (
    0.35,
    0.22,
    0.16,
    0.14,
    0.18,
    0.30,
    0.55,
    0.95,
    1.35,
    1.60,
    1.70,
    1.65,
    1.55,
    1.50,
    1.45,
    1.40,
    1.38,
    1.42,
    1.50,
    1.55,
    1.45,
    1.20,
    0.85,
    0.55,
)


def _category_id(code: str) -> int:
    digest = hashlib.sha1(code.encode()).hexdigest()[:12]
    return CATEGORY_ID_BASE + int(digest, 16) % 900_000_000


def _session_sizes() -> tuple[float, float]:
    steps = 200_000
    total = 0
    at_least_two = 0
    for index in range(steps):
        u = (index + 0.5) / steps
        size = 1 + int((u**SESSION_SHAPE) * (MAX_SESSION_EVENTS - 1))
        total += size
        if size >= 2:
            at_least_two += 1
    return total / steps, at_least_two / steps


def session_probabilities(config: Config) -> dict[str, float]:
    mean_size, share_two = _session_sizes()
    scale = mean_size / share_two
    cart = config.generate.cart_rate * scale
    purchase = config.generate.purchase_rate * scale
    remove = config.generate.remove_rate * scale
    if cart >= 1.0:
        raise ConfigError("generate.cart_rate is too high for the session size distribution")
    if purchase + remove > cart:
        raise ConfigError("generate purchase and remove rates cannot exceed the cart rate")
    return {
        "mean_size": mean_size,
        "cart": cart,
        "purchase_given_cart": purchase / cart,
        "remove_given_cart": remove / cart,
    }


def _day_weights(config: Config) -> list[tuple[date, float]]:
    weights = []
    start = config.generate.start_date
    for offset in range(config.generate.days):
        day = start + timedelta(days=offset)
        weight = 1.0 if day.month == config.generate.start_date.month else 1.6
        if day.weekday() >= 5:
            weight *= 1.15
        gap = (config.generate.black_friday - day).days
        if 0 < gap <= 3:
            weight *= 1.35
        if day == config.generate.black_friday:
            weight *= config.generate.black_friday_boost
        if -3 <= gap < 0:
            weight *= 1.25
        weights.append((day, weight))
    return weights


def _bucket_rows(weights: list[float], buckets: int) -> list[tuple[int, int]]:
    total = sum(weights)
    cumulative = []
    running = 0.0
    for weight in weights:
        running += weight / total
        cumulative.append(running)
    rows = []
    index = 0
    for bucket in range(buckets):
        position = (bucket + 0.5) / buckets
        while index < len(cumulative) - 1 and position > cumulative[index]:
            index += 1
        rows.append((bucket, index))
    return rows


def _day_lookup(spark: SparkSession, config: Config) -> DataFrame:
    weights = _day_weights(config)
    rows = _bucket_rows([weight for _, weight in weights], DAY_BUCKETS)
    days = [day for day, _ in weights]
    frame = spark.createDataFrame(
        [(bucket, days[index]) for bucket, index in rows], "day_bucket int, event_day date"
    )
    return F.broadcast(frame)


def _hour_lookup(spark: SparkSession) -> DataFrame:
    rows = _bucket_rows(list(HOUR_WEIGHTS), HOUR_BUCKETS)
    frame = spark.createDataFrame(rows, "hour_bucket int, start_hour int")
    return F.broadcast(frame)


def catalog(spark: SparkSession, config: Config) -> DataFrame:
    settings = config.generate
    category_frame = spark.createDataFrame(
        [
            (index, code, _category_id(code), low, high)
            for index, (code, _, low, high) in enumerate(CATEGORIES)
        ],
        "category_index int, category_code string, category_id long, "
        "price_low double, price_high double",
    )
    brand_frame = spark.createDataFrame(
        [(index, brand) for index, brand in enumerate(BRANDS)], "brand_index int, brand string"
    )
    weights = [weight for _, weight, _, _ in CATEGORIES]
    category_rows = _bucket_rows(weights, len(CATEGORIES) * 50)
    category_pick = spark.createDataFrame(category_rows, "category_bucket int, category_index int")
    return (
        spark.range(0, settings.products, numPartitions=settings.output_partitions)
        .withColumnRenamed("id", "product_index")
        .withColumn(
            "category_bucket",
            (F.rand(settings.seed + 11) * len(category_rows)).cast("int"),
        )
        .join(F.broadcast(category_pick), "category_bucket")
        .join(F.broadcast(category_frame), "category_index")
        .withColumn("brand_index", (F.rand(settings.seed + 12) * len(BRANDS)).cast("int"))
        .join(F.broadcast(brand_frame), "brand_index")
        .withColumn("product_id", F.col("product_index") + F.lit(PRODUCT_ID_BASE))
        .withColumn(
            "base_price",
            F.round(
                F.col("price_low")
                + (F.col("price_high") - F.col("price_low"))
                * F.pow(F.rand(settings.seed + 13), 2.0),
                2,
            ),
        )
        .withColumn(
            "brand",
            F.when(
                F.rand(settings.seed + 14) < settings.null_brand_rate, F.lit(None).cast("string")
            ).otherwise(F.col("brand")),
        )
        .withColumn(
            "category_code",
            F.when(
                F.rand(settings.seed + 15) < settings.null_category_rate,
                F.lit(None).cast("string"),
            ).otherwise(F.col("category_code")),
        )
        .select(
            "product_index", "product_id", "category_id", "category_code", "brand", "base_price"
        )
    )


def _sessions(spark: SparkSession, config: Config, events: int) -> DataFrame:
    settings = config.generate
    probabilities = session_probabilities(config)
    count = max(1, int(events / probabilities["mean_size"]))
    session_hash = F.sha2(F.concat_ws(":", F.lit(settings.seed), F.col("session_index")), 256)
    return (
        spark.range(0, count, numPartitions=settings.output_partitions)
        .withColumnRenamed("id", "session_index")
        .withColumn("day_bucket", (F.rand(settings.seed + 1) * DAY_BUCKETS).cast("int"))
        .join(_day_lookup(spark, config), "day_bucket")
        .withColumn("hour_bucket", (F.rand(settings.seed + 2) * HOUR_BUCKETS).cast("int"))
        .join(_hour_lookup(spark), "hour_bucket")
        .withColumn(
            "user_id",
            (F.pow(F.rand(settings.seed + 3), USER_SHAPE) * settings.users).cast("long")
            + F.lit(USER_ID_BASE),
        )
        .withColumn(
            "session_events",
            (F.pow(F.rand(settings.seed + 4), SESSION_SHAPE) * (MAX_SESSION_EVENTS - 1)).cast("int")
            + F.lit(1),
        )
        .withColumn(
            "focus_index",
            (F.pow(F.rand(settings.seed + 5), PRODUCT_SHAPE) * settings.products).cast("long"),
        )
        .withColumn(
            "gap_seconds",
            F.lit(MIN_GAP_SECONDS) + (F.rand(settings.seed + 6) * GAP_SPREAD_SECONDS).cast("int"),
        )
        .withColumn("has_cart", F.rand(settings.seed + 7) < F.lit(probabilities["cart"]))
        .withColumn("cart_outcome", F.rand(settings.seed + 8))
        .withColumn(
            "start_epoch",
            F.unix_timestamp(F.col("event_day").cast("timestamp"))
            + F.col("start_hour") * F.lit(3600)
            + (F.rand(settings.seed + 9) * 3600).cast("int"),
        )
        .withColumn(
            "user_session",
            F.concat_ws(
                "-",
                F.substring(session_hash, 1, 8),
                F.substring(session_hash, 9, 4),
                F.substring(session_hash, 13, 4),
                F.substring(session_hash, 17, 4),
                F.substring(session_hash, 21, 12),
            ),
        )
        .select(
            "session_index",
            "user_id",
            "user_session",
            "session_events",
            "focus_index",
            "gap_seconds",
            "has_cart",
            "cart_outcome",
            "start_epoch",
        )
    )


def _events(spark: SparkSession, config: Config, events: int) -> DataFrame:
    settings = config.generate
    probabilities = session_probabilities(config)
    purchase_cut = probabilities["purchase_given_cart"]
    remove_cut = purchase_cut + probabilities["remove_given_cart"]
    sessions = _sessions(spark, config, events)
    step = F.col("step")
    last = F.col("session_events") - F.lit(1)
    exploded = (
        sessions.withColumn("step", F.explode(F.sequence(F.lit(0), last)))
        .withColumn(
            "event_epoch",
            F.col("start_epoch")
            + step * F.col("gap_seconds")
            + (F.rand(settings.seed + 21) * 5).cast("int"),
        )
        .withColumn("is_cart", F.col("has_cart") & (step == last - F.lit(1)))
        .withColumn("is_last", F.col("has_cart") & (step == last) & (last >= F.lit(1)))
        .withColumn(
            "event_type",
            F.when(F.col("is_cart"), F.lit("cart"))
            .when(
                F.col("is_last") & (F.col("cart_outcome") < F.lit(purchase_cut)), F.lit("purchase")
            )
            .when(
                F.col("is_last") & (F.col("cart_outcome") < F.lit(remove_cut)),
                F.lit("remove_from_cart"),
            )
            .otherwise(F.lit("view")),
        )
        .withColumn(
            "product_index",
            F.when(F.col("is_cart") | F.col("is_last"), F.col("focus_index"))
            .when(F.rand(settings.seed + 22) < F.lit(0.65), F.col("focus_index"))
            .otherwise(
                (F.col("focus_index") + (F.rand(settings.seed + 23) * 97).cast("long") + F.lit(1))
                % F.lit(settings.products)
            ),
        )
        .withColumn("event_time", F.timestamp_seconds(F.col("event_epoch")))
        .select("user_id", "user_session", "event_time", "event_type", "product_index")
    )
    products = catalog(spark, config)
    week = F.floor(
        F.datediff(F.to_date(F.col("event_time")), F.lit(settings.start_date)) / F.lit(7)
    )
    drift = F.when(
        F.col("product_id") % F.lit(10) < F.lit(PRICE_CHANGING_PRODUCTS),
        F.lit(1.0)
        + (((F.col("product_id") + week * F.lit(7)) % F.lit(9)) - F.lit(4)) * F.lit(0.02),
    ).otherwise(F.lit(1.0))
    return (
        exploded.join(F.broadcast(products), "product_index")
        .withColumn("price", F.round(F.col("base_price") * drift, 2))
        .select(
            "event_time",
            "event_type",
            "product_id",
            "category_id",
            "category_code",
            "brand",
            "price",
            "user_id",
            "user_session",
        )
    )


def _with_defects(frame: DataFrame, config: Config) -> DataFrame:
    settings = config.generate
    broken = F.rand(settings.seed + 31)
    third = settings.bad_row_rate / 3.0
    return (
        frame.withColumn(
            "user_session",
            F.when(
                F.rand(settings.seed + 32) < settings.null_session_rate,
                F.lit(None).cast("string"),
            ).otherwise(F.col("user_session")),
        )
        .withColumn(
            "event_time",
            F.when(broken < F.lit(third), F.lit(None).cast("timestamp")).otherwise(
                F.col("event_time")
            ),
        )
        .withColumn(
            "event_type",
            F.when(
                (broken >= F.lit(third)) & (broken < F.lit(2 * third)), F.lit("wishlist")
            ).otherwise(F.col("event_type")),
        )
        .withColumn(
            "price",
            F.when(
                (broken >= F.lit(2 * third)) & (broken < F.lit(settings.bad_row_rate)),
                F.col("price") * F.lit(-1.0),
            ).otherwise(F.col("price")),
        )
    )


def _as_source_rows(frame: DataFrame) -> DataFrame:
    stamp = F.when(F.col("event_time").isNull(), F.lit("not a timestamp")).otherwise(
        F.concat(F.date_format(F.col("event_time"), "yyyy-MM-dd HH:mm:ss"), F.lit(" UTC"))
    )
    return frame.select(
        stamp.alias("event_time"),
        F.col("event_type"),
        F.col("product_id").cast("string").alias("product_id"),
        F.col("category_id").cast("string").alias("category_id"),
        F.coalesce(F.col("category_code"), F.lit("")).alias("category_code"),
        F.coalesce(F.col("brand"), F.lit("")).alias("brand"),
        F.format_string("%.2f", F.col("price")).alias("price"),
        F.col("user_id").cast("string").alias("user_id"),
        F.coalesce(F.col("user_session"), F.lit("")).alias("user_session"),
        F.col("month"),
    )


def generate(spark: SparkSession, config: Config, events: int = 0, force: bool = False) -> dict:
    settings = config.generate
    target = events if events > 0 else settings.events
    destination = config.paths.source
    if exists(spark, destination) and not force:
        raise FileExistsError(f"{destination} already exists, pass --force to replace it")
    generated = _events(spark, config, target).filter(
        (F.col("event_time") >= F.lit(settings.start_date).cast("timestamp"))
        & (F.col("event_time") < F.date_add(F.lit(settings.end_date), 1).cast("timestamp"))
    )
    frame = _with_defects(generated, config)
    duplicated = frame.sample(settings.duplicate_rate, settings.seed + 41)
    combined = frame.unionByName(duplicated).withColumn(
        "month", F.date_format(F.col("event_time"), "yyyy-MMM")
    )
    rows = _as_source_rows(combined).withColumn(
        "month", F.coalesce(F.col("month"), F.lit(config.source.months[0]))
    )
    (
        rows.write.mode("overwrite")
        .partitionBy("month")
        .option("header", "true")
        .option("compression", "gzip")
        .csv(spark_path(destination))
    )
    return {"requested": target, "path": str(destination)}


def month_path(config: Config, month: str) -> Path:
    partitioned = config.paths.source / f"month={month}"
    if partitioned.exists():
        return partitioned
    return config.paths.source / config.source.file_name(month)
