import os
from pathlib import Path

LOG4J2_CONFIG = Path(os.path.abspath(os.path.join(os.path.dirname(__file__), "log4j2.properties")))
if not LOG4J2_CONFIG.exists():
    LOG4J2_CONFIG.write_text(
        "status = error\n"
        "name = PropertiesConfig\n"
        "filters = threshold\n"
        "filter.threshold.type = ThresholdFilter\n"
        "filter.threshold.level = error\n"
        "\n"
        "appenders = console\n"
        "\n"
        "appender.console.type = Console\n"
        "appender.console.name = console\n"
        "appender.console.layout.type = PatternLayout\n"
        "appender.console.layout.pattern = %m%n\n"
        "\n"
        "rootLogger.level = error\n"
        "rootLogger.appenderRefs = console\n"
        "rootLogger.appenderRef.console.ref = console\n"
    )

os.environ.setdefault("SPARK_HOME", r"C:\spark-4.1.1-bin-hadoop3")

os.environ.setdefault(
    "PYSPARK_SUBMIT_ARGS",
    f"--conf spark.driver.extraJavaOptions=-Dlog4j2.configurationFile={LOG4J2_CONFIG.as_uri()} pyspark-shell"
)

from pyspark.sql import SparkSession


def _normalize_env_path(value):
    if not value:
        return None
    normalized = os.path.normpath(value.strip())
    return normalized if normalized else None


def _has_winutils(path):
    return os.path.exists(os.path.join(path, "bin", "winutils.exe"))


def _set_hadoop_env(path):
    os.environ["HADOOP_HOME"] = path
    os.environ.setdefault("hadoop.home.dir", path)


def _ensure_windows_hadoop_home():
    if os.name != "nt":
        return

    hadoop_home = _normalize_env_path(os.environ.get("HADOOP_HOME"))
    hadoop_home_dir = _normalize_env_path(os.environ.get("hadoop.home.dir"))

    for path in [hadoop_home, hadoop_home_dir]:
        if path and os.path.isdir(path) and _has_winutils(path):
            _set_hadoop_env(path)
            return

    candidates = [
        os.path.join(os.getcwd(), "hadoop"),
        r"C:\hadoop",
        r"C:\winutils",
    ]
    for candidate in candidates:
        candidate = _normalize_env_path(candidate)
        if candidate and os.path.isdir(candidate) and _has_winutils(candidate):
            _set_hadoop_env(candidate)
            return

    raise EnvironmentError(
        "Spark on Windows requires HADOOP_HOME or hadoop.home.dir pointing to a Hadoop installation "
        "containing winutils.exe.\n"
        "Install winutils.exe and set HADOOP_HOME=C:\\hadoop (or another local Hadoop path)."
    )


def get_spark():
    _ensure_windows_hadoop_home()

    spark = SparkSession.builder \
        .appName("DineVision-Spark") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    return spark