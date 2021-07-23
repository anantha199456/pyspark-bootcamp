from pyspark.sql import *
from pyspark.sql.functions import monotonically_increasing_id, expr, col, when, to_date
from pyspark.sql.types import IntegerType

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("DEmo") \
        .getOrCreate()

    data_list = [("Anantha", "28", "8", "19"), ("Krishna", "28", "8", "21"), ("Kannan", "25", "7", "105")]

    # toDF will add the column name to your new Dataframe
    df = spark.createDataFrame(data_list).toDF("Name", "Day", "Month", "Year")

    # now if you want add a new column use withColumn. monotonically_increasing_id -
    # fuction is something which autogenerates the id's. never produces duplicates

    df1 = df.withColumn("id", monotonically_increasing_id())
    # df1.show()
    # df1.printSchema()
    # data_list = [("Anantha", 28, 8, 19), ("Krishna", 28, 8, 21), ("Kannan", 25, 7, 105)]

    # df2 = df1.withColumn("year", expr("""
    #             case when Year > 22 then Year + 2000
    #             when Year < 100 then Year + 2010
    #             else Year
    #             end"""))
    df2 = df1.withColumn("year",
                         when(col("year") > 22, col("year") + 2000) \
                         .when(col("year") < 100, col("year") + 2010) \
                         .otherwise(col("year")))

    df3 = df2.withColumn("Day", col("Day").cast(IntegerType())) \
        .withColumn("Month", col("Month").cast(IntegerType())) \
        .withColumn("Year", col("Year").cast(IntegerType()))
    # df3.show()
    # df3.printSchema()

    df4 = df3.withColumn("DOB", to_date(expr("concat(Day,'/',Month,'/',Year)"), 'd/M/y')) \
        .drop("Day", "Month", "Year") \
        .sort(expr("DOB desc"))
    df4.show()
