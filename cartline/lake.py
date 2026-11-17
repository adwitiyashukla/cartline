from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession


def spark_path(path: str | Path) -> str:
    return Path(path).as_posix()


def exists(spark: SparkSession, path: str | Path) -> bool:
    jvm = spark.sparkContext._jvm
    target = jvm.org.apache.hadoop.fs.Path(spark_path(path))
    file_system = target.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    return bool(file_system.exists(target))


def read_table(spark: SparkSession, path: str | Path) -> DataFrame:
    return spark.read.parquet(spark_path(path))


def read_optional(spark: SparkSession, path: str | Path) -> DataFrame | None:
    if not exists(spark, path):
        return None
    return read_table(spark, path)


def write_table(
    frame: DataFrame,
    path: str | Path,
    partitions: tuple[str, ...] = (),
    mode: str = "overwrite",
    files: int = 0,
    spread: tuple[str, ...] = (),
) -> None:
    output = frame
    if files > 0 and spread:
        output = frame.repartition(files, *[frame[name] for name in spread])
    elif files > 0:
        output = frame.repartition(files)
    writer = output.write.mode(mode)
    if partitions:
        writer = writer.partitionBy(list(partitions))
    writer.parquet(spark_path(path))


def append_table(frame: DataFrame, path: str | Path, files: int = 1) -> None:
    write_table(frame, path, mode="append", files=files)


def count_files(spark: SparkSession, path: str | Path, suffix: str = ".parquet") -> int:
    if not exists(spark, path):
        return 0
    jvm = spark.sparkContext._jvm
    target = jvm.org.apache.hadoop.fs.Path(spark_path(path))
    file_system = target.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    total = 0
    stack = [target]
    while stack:
        current = stack.pop()
        for status in file_system.listStatus(current):
            if status.isDirectory():
                stack.append(status.getPath())
            elif status.getPath().getName().endswith(suffix):
                total += 1
    return total


def table_bytes(spark: SparkSession, path: str | Path) -> int:
    if not exists(spark, path):
        return 0
    jvm = spark.sparkContext._jvm
    target = jvm.org.apache.hadoop.fs.Path(spark_path(path))
    file_system = target.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    return int(file_system.getContentSummary(target).getLength())
