from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Agg Demo") \
        .master("local[2]") \
        .getOrCreate()

    logger = Log4j(spark)

    invoice_df = spark.read \
        .format("parquet") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("data/summary.parquet")

    totol_window = Window.partitionBy("Country").orderBy("WeekNumber").rowsBetween(Window.unboundedPreceding,
                                                                                   Window.currentRow)

    invoice_df.withColumn("RunningTtoal", F.row_number().over(totol_window)).show()
