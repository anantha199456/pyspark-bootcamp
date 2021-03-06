from pyspark.sql import *

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Demo") \
        .master("local[2]") \
        .getOrCreate()

    orders_list = [("01", "02", 350, 1),
                   ("01", "04", 580, 1),
                   ("01", "07", 320, 2),
                   ("02", "03", 450, 1),
                   ("02", "06", 220, 1),
                   ("03", "01", 195, 1),
                   ("04", "09", 270, 3),
                   ("04", "08", 410, 2),
                   ("05", "02", 350, 1)]

    order_df = spark.createDataFrame(orders_list).toDF("order_id", "prod_id", "unit_price", "qty")

    product_list = [("01", "Scroll Mouse", 250, 20),
                    ("02", "Optical Mouse", 350, 20),
                    ("03", "Wireless Mouse", 450, 50),
                    ("04", "Wireless Keyboard", 580, 50),
                    ("05", "Standard Keyboard", 360, 10),
                    ("06", "16 GB Flash Storage", 240, 100),
                    ("07", "32 GB Flash Storage", 320, 50),
                    ("08", "64 GB Flash Storage", 430, 25)]

    product_df = spark.createDataFrame(product_list).toDF("prod_id", "prod_name", "list_price", "qty")
    new_prod_df = product_df.withColumnRenamed("qty", "product_qty")
    join_expr = order_df.prod_id == product_df.prod_id
    order_df.join(new_prod_df, join_expr, "inner") \
        .drop(new_prod_df.prod_id) \
        .select("order_id", "prod_id", "prod_name", "unit_price",
                "list_price",
                "qty").sort("order_id").show()

    # raw_input = input("press any key to stop .....")
    # print(int(raw_input) + 5)
