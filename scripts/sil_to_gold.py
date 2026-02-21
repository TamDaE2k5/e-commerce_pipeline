from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from urllib.parse import urlparse
def main():
    # b1 tạo session cho saprk
    spark = SparkSession.builder.appName('SilverToGold_SparkJob') \
            .config('spark.hadoop.fs.s3a.endpoint', 'http://minio:9000') \
            .config('spark.hadoop.fs.s3a.accessKey', 'minioadmin') \
            .config('spark.hadoop.fs.s3a.secretKey', 'minioadmin') \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .getOrCreate()
    
    # b2 đọc data
    now = datetime.now()
    # day = now.day
    # month = now.month
    # year = now.year
    day = 15
    month = 2
    year = 2026
    try:
        df_silver = spark.read.parquet(f"s3a://data/silver/{day}-{month}-{year}/*.parquet")
        print('read data successfully')
    except Exception as e:
        print('error when read data', e)
        raise
    # b3 phân rã data
        # dim_date
    dim_date_new = (
        df_silver
        .select(
            date_format("crawl_full_day", "yyyyMMdd").cast("int").alias("date_key"),
            col("crawl_full_day"),
            col("crawl_year").cast("int").alias("year"),
            col("crawl_month").cast("int").alias("month"),
            col("crawl_day").cast("int").alias("day"),
            col("crawl_day_of_week")
        )
        .distinct()
    )

    try:
        dim_date_old = spark.read.parquet(f"s3a://data/gold/star1/dim_date/*.parquet")
        dim_date = (
            dim_date_new.alias("n")
            .join(dim_date_old.alias("o"), "date_key", "left_anti")
            .unionByName(dim_date_old)
        )
    except:
        dim_date = dim_date_new
    
        # dim_category
    dim_category_new = (
        df_silver
        .select(col("category").alias("category_name"))
        .distinct()
    )

    try:
        dim_category_old = spark.read.parquet(f"s3a://data/gold/star1/dim_category/*.parquet")
        dim_category_inc = (
            dim_category_new.alias("n")
            .join(dim_category_old.alias("o"), "category_name", "left_anti")
            .withColumn("category_key", sha2(col("category_name"), 256))
        )
        dim_category = dim_category_old.unionByName(dim_category_inc)
    except:
        dim_category = dim_category_new.withColumn(
            "category_key", sha2(col("category_name"), 256)
        )
        
        # dim_source
    dim_source_new = (
        df_silver
        .select(col("Src").alias("source_name"))
        .distinct()
    )

    try:
        dim_source_old = spark.read.parquet(f"s3a://data/gold/star1/dim_source/*.parquet")
        dim_source_inc = (
            dim_source_new.alias("n")
            .join(dim_source_old.alias("o"), "source_name", "left_anti")
            .withColumn("source_key", sha2(col("source_name"), 256))
        )
        dim_source = dim_source_old.unionByName(dim_source_inc)
    except:
        dim_source = dim_source_new.withColumn(
            "source_key", sha2(col("source_name"), 256)
        )
        # dim_product
    df_silver = df_silver.withColumn(
        "product_hk",
        sha2(concat_ws("|", "Src", "product_name"), 256)
    )

    dim_product_new = (
        df_silver
        .select(
            "product_hk",
            "product_name",
            "origin",
            col("category").alias("category_name")
        )
        .distinct()
    )

    try:
        dim_product_old = spark.read.parquet(f"s3a://data/gold/star1/dim_product/*.parquet")
        dim_product_inc = (
            dim_product_new.alias("n")
            .join(dim_product_old.alias("o"), "product_hk", "left_anti")
            .withColumn("product_key", sha2(col("product_hk"), 256))
        )
        dim_product = dim_product_old.unionByName(dim_product_inc)
    except:
        dim_product = dim_product_new.withColumn(
            "product_key", sha2(col("product_hk"), 256)
        )
        #fact
    fact = df_silver \
        .join(dim_product.select("product_key", "product_hk"), "product_hk") \
        .join(dim_source, df_silver.Src == dim_source.source_name, 'left') \
        .join(dim_category, df_silver.category == dim_category.category_name, 'left') \
        .select(
            date_format("crawl_full_day", "yyyyMMdd").cast("int").alias("date_key"),
            "product_key",
            "source_key",
            col("crawl_timestamp"),
            "product_original_price",
            "product_price",
            "product_discount",
            "discount_rate",
            "quantity_sold",
            "income"
        )
    # fact.show()
    # b4 lưu vào tmp do spark lazy (overwrite bị xoá dữ liệu nên ko overwrite dc vào file cũ (file cũ bị xoá))
    dim_date.coalesce(1).write.mode("overwrite").parquet(f"s3a://data/gold/star1/_tmp/dim_date")
    dim_category.coalesce(1).write.mode("overwrite").parquet(f"s3a://data/gold/star1/_tmp/dim_category")
    dim_source.coalesce(1).write.mode("overwrite").parquet(f"s3a://data/gold/star1/_tmp/dim_source")
    dim_product.coalesce(1).write.mode("overwrite").parquet(f"s3a://data/gold/star1/_tmp/dim_product")
    fact.coalesce(1).write.mode("append") \
        .parquet("s3a://data/gold/star1/fact_product_sale")

    # b5 lưu schema vào gold
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jsc.hadoopConfiguration()
    )

    def promote(tmp_path: str, final_path: str):
        spark = SparkSession.builder.getOrCreate()
        sc = spark.sparkContext
        jvm = sc._jvm

        hadoop_conf = sc._jsc.hadoopConfiguration()

        # Parse URI (s3a://...)
        tmp_uri = jvm.java.net.URI(tmp_path)
        final_uri = jvm.java.net.URI(final_path)

        # 🔥 LẤY FILESYSTEM ĐÚNG THEO URI
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(tmp_uri, hadoop_conf)

        tmp_p = jvm.org.apache.hadoop.fs.Path(tmp_path)
        final_p = jvm.org.apache.hadoop.fs.Path(final_path)

        # Xóa gold cũ nếu tồn tại
        if fs.exists(final_p):
            fs.delete(final_p, True)

        # Rename _tmp -> gold
        if not fs.rename(tmp_p, final_p):
            raise RuntimeError(f"Rename failed from {tmp_path} to {final_path}")

    promote(f"s3a://data/gold/star1/_tmp/dim_date", f"s3a://data/gold/star1/dim_date")
    promote(f"s3a://data/gold/star1/_tmp/dim_category", f"s3a://data/gold/star1/dim_category")
    promote(f"s3a://data/gold/star1/_tmp/dim_source", f"s3a://data/gold/star1/dim_source")
    promote(f"s3a://data/gold/star1/_tmp/dim_product", f"s3a://data/gold/star1/dim_product")

    spark.stop()
if __name__ == '__main__':
    main()