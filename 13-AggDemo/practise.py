from pyparsing import col
from pyspark.sql import functions as F
from pyspark.sql import *
from pyspark.sql.functions import weekofyear, to_date

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("demo") \
        .master("local[2]") \
        .getOrCreate()

    df1 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("data/invoices.csv")
    df1.show(3)

    # # To showcase the aggregate function and how it works
    # df1.select(F.count("*").alias("Count *"),
    #            F.sum("Quantity").alias("SumQuantity"),
    #            F.avg("UnitPrice").alias("AvgUnitPrice"),
    #            F.countDistinct("InvoiceNo").alias("countDistinct")).show(10)

    summarized_df = df1.groupBy("Country", "InvoiceNo").agg(F.sum("Quantity").alias("TotalQuantity"),
                                                            F.round(F.sum(F.expr("Quantity * UnitPrice")), 2).alias(
                                                                "InvoiceValue"))
    # summarized_df.show(10)
    converted_data = df1.withColumn("InvoiceDate", F.to_date(F.col('InvoiceDate'), 'dd-MM-yyyy H.mm'))
    df2 = converted_data.withColumn("Week", F.weekofyear(F.col("InvoiceDate")))
    # df2.show(10)

    summary = df2.groupBy("Country", "Week").agg(F.countDistinct("InvoiceNo").alias("NumInvoices"),
                                                 F.sum("Quantity").alias("TotalQuantity"),
                                                 F.round(F.sum(F.expr("Quantity * UnitPrice")), 2).alias(
                                                     "InvoiceValue")).drop("StockCode", "Description", "CustomerID")

    # summary.write.format("parquet").mode("overwrite").save("output")
    summary.sort("Country", "Week").show(50)
