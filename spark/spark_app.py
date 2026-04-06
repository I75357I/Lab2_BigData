import os
import sys
import json
import time
import logging
import psutil
from datetime import datetime

sys.path.insert(0, "/app/spark")
from spark_listener import register_spark_listener

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SETUP = sys.argv[1] if len(sys.argv) > 1 else "unknown"
EXPERIMENT = f"basic_{SETUP}"
LOG_FILE = f"/app/results/log_{EXPERIMENT}.log"
METRICS_FILE = f"/app/results/metrics_{EXPERIMENT}.json"

os.makedirs("/app/results", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE),
    ],
)
logger = logging.getLogger(__name__)


def mem_mb() -> float:
    return psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024


metrics: dict = {"experiment": EXPERIMENT, "timestamp": datetime.now().isoformat(), "steps": []}
_step_start: float = 0.0


def step_begin(name: str):
    global _step_start
    _step_start = time.time()
    logger.info(f"step {name} started, ram={mem_mb():.0f}mb")


def step_end(name: str):
    elapsed = time.time() - _step_start
    ram = mem_mb()
    metrics["steps"].append({"name": name, "time_s": round(elapsed, 3), "ram_mb": round(ram, 1)})
    logger.info(f"step {name} done in {elapsed:.3f}s, ram={ram:.0f}mb")
    return elapsed


logger.info(f"experiment: {EXPERIMENT}")

spark = (
    SparkSession.builder
    .appName(f"Lab2-{EXPERIMENT}")
    .master("local[*]")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
register_spark_listener(spark, logger)

logger.info(f"spark {spark.version}, cores={spark.sparkContext.defaultParallelism}")

total_start = time.time()

step_begin("load_data")
df = spark.read.csv(
    "hdfs://namenode:9000/data/dataset.csv",
    header=True,
    inferSchema=True,
)
row_count = df.count()
logger.info(f"rows={row_count:,}, columns={len(df.columns)}, partitions={df.rdd.getNumPartitions()}")
step_end("load_data")

step_begin("basic_stats")
df.describe("unit_price", "quantity", "total_amount", "customer_age").show()
step_end("basic_stats")

step_begin("sales_by_category")
cat_stats = (
    df.groupBy("category")
    .agg(
        F.count("transaction_id").alias("transactions"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.round(F.avg("total_amount"), 2).alias("avg_transaction"),
        F.round(F.avg("discount_pct"), 2).alias("avg_discount"),
    )
    .orderBy(F.desc("total_revenue"))
)
cat_stats.show()
step_end("sales_by_category")

step_begin("sales_by_region")
(
    df.groupBy("region")
    .agg(
        F.count("transaction_id").alias("transactions"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.round(F.avg("customer_age"), 1).alias("avg_age"),
    )
    .orderBy(F.desc("total_revenue"))
).show()
step_end("sales_by_region")

step_begin("top_products")
(
    df.groupBy("category", "product_name")
    .agg(
        F.round(F.sum("total_amount"), 2).alias("revenue"),
        F.sum("quantity").alias("units_sold"),
    )
    .orderBy(F.desc("revenue"))
    .limit(20)
).show()
step_end("top_products")

step_begin("monthly_trends")
(
    df.withColumn("year_month", F.date_format(F.col("date"), "yyyy-MM"))
    .groupBy("year_month")
    .agg(
        F.count("transaction_id").alias("transactions"),
        F.round(F.sum("total_amount"), 2).alias("revenue"),
    )
    .orderBy("year_month")
).show(30)
step_end("monthly_trends")

step_begin("age_group_analysis")
(
    df.withColumn(
        "age_group",
        F.when(F.col("customer_age") < 25, "18-24")
         .when(F.col("customer_age") < 35, "25-34")
         .when(F.col("customer_age") < 45, "35-44")
         .when(F.col("customer_age") < 55, "45-54")
         .otherwise("55+"),
    )
    .groupBy("age_group")
    .agg(
        F.count("transaction_id").alias("transactions"),
        F.round(F.avg("total_amount"), 2).alias("avg_spend"),
        F.round(F.sum("total_amount"), 2).alias("total_spend"),
    )
    .orderBy("age_group")
).show()
step_end("age_group_analysis")

step_begin("payment_analysis")
(
    df.groupBy("payment_method")
    .agg(
        F.count("transaction_id").alias("count"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.round(F.avg("total_amount"), 2).alias("avg_amount"),
    )
    .orderBy(F.desc("count"))
).show()
step_end("payment_analysis")

step_begin("window_ranking")
window_spec = Window.partitionBy("category").orderBy(F.desc("total_amount"))
(
    df.withColumn("rank", F.rank().over(window_spec))
    .filter(F.col("rank") <= 3)
    .select("category", "product_name", "total_amount", "rank")
    .orderBy("category", "rank")
).show(30)
step_end("window_ranking")

step_begin("save_to_hdfs")
out_path = f"hdfs://namenode:9000/results/{EXPERIMENT}"
cat_stats.coalesce(1).write.mode("overwrite").csv(out_path + "/category_stats", header=True)
logger.info(f"saved to {out_path}")
step_end("save_to_hdfs")

metrics["total_time_s"] = round(time.time() - total_start, 3)
metrics["peak_ram_mb"] = round(mem_mb(), 1)
metrics["row_count"] = row_count

logger.info(f"done, total={metrics['total_time_s']}s, peak_ram={metrics['peak_ram_mb']}mb")

with open(METRICS_FILE, "w") as fh:
    json.dump(metrics, fh, indent=2)
logger.info(f"metrics saved to {METRICS_FILE}")

spark.stop()
