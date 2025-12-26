import os
from pyspark.sql import SparkSession
from src.main.utility.logging_config import logger

def spark_session():
    # Project root directory
    BASE_DIR = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../../../")
    )

    JAR_PATH = os.path.join(
        BASE_DIR,
        "jars",
        "mysql-connector-j-9.5.0.jar"
    )

    # Defensive check
    if not os.path.exists(JAR_PATH):
        raise FileNotFoundError(
            f"MySQL JDBC JAR not found at expected path: {JAR_PATH}"
        )

    # =========================
    # 🔐 PYTHON VERSION FIX (ADD HERE)
    # =========================
    python_path = os.path.join(
        BASE_DIR,
        ".venv",
        "bin",
        "python"
    )

    os.environ["PYSPARK_PYTHON"] = python_path
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

    # =========================
    # Spark Session
    # =========================
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("tanay_spark")
        .config("spark.jars", JAR_PATH)
        .config("spark.pyspark.python", python_path)
        .config("spark.pyspark.driver.python", python_path)
        .getOrCreate()
    )

    logger.info("Spark session created successfully")
    return spark
