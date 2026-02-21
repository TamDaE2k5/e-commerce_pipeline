from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime

def main():
    print('Bronze to silver spark job')
    spark = SparkSession.builder.appName('BronzeToSilverSparkJob') \
            .config('spark.hadoop.fs.s3a.endpoint', 'http://minio:9000') \
            .config('spark.hadoop.fs.s3a.accessKey', 'minioadmin') \
            .config('spark.hadoop.fs.s3a.secretKey', 'minioadmin') \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.hadoop.fs.s3a.connection.timeout", "10000") \
            .config("spark.sql.debug.maxToStringFields", "100") \
            .getOrCreate()
    
# read data
    day_process = datetime.now()
    day = day_process.day
    month = day_process.month
    year = day_process.year
    # day = 15
    # month = 2
    # year = 2026
    try:
        df_tiki = spark.read.option('multiline', 'true') \
                .json(f's3a://data/bronze/{day}-{month}-{year}/tiki_*.json').repartition(10)
        print('read data tiki successfully')
    except Exception as e: 
        print(e)
        df_tiki = None
    
    try:
        df_lazada = spark.read.option('multiline', 'true') \
                    .json(f's3a://data/bronze/{day}-{month}-{year}/lazada_*.json')
        print('read data lazada successfully')
    except Exception as e:
        print(e)
        df_lazada = None

# processing
    # tiki
        # filter
    if df_tiki is not None:
        df_tiki = df_tiki.select(
            col('Src'),
            col('category'),
            col('crawl-timestamp'),
            col('crawl-day'),
            explode(col('data')).alias('product')
        )
        df_tiki = df_tiki.select(
            col('Src').alias('Src'),
            col('category').alias('category'),
            col('crawl-timestamp').alias('crawl_timestamp'),
            col('crawl-day').alias('crawl_full_day'),
            col('product.name').alias('product_name'),
            col('product.original_price').alias('product_original_price'),
            col('product.price').alias('product_price'),
            col('product.discount').alias('product_discount'),
            col('product.discount_rate').alias('discount_rate'),
            col('product.quantity_sold.value').alias('quantity_sold')
        )
            #fill nan for quantity_sold (string '' is not null)
        df_tiki = df_tiki.fillna({'quantity_sold':0})  
        # fix data type
        # normalize time
        df_tiki = df_tiki.withColumn(
            "crawl_timestamp",
            date_format(to_timestamp(col("crawl_timestamp"), "H:m:s"), "HH:mm:ss")
        )
        df_tiki = df_tiki.withColumn(
            'crawl_full_day',
            to_date(col('crawl_full_day'), 'd-M-yyyy')
        )
            #add col origin
        df_tiki = df_tiki.withColumn(
            'origin', lit('Vietnam')
        )
        cols = ['product_original_price', 'product_price', 'product_discount', 'discount_rate', 'quantity_sold']
        for c in cols:
            df_tiki = df_tiki.withColumn(c, col(c).cast(LongType()))
        # duplicate
        window_spec  = Window.partitionBy('product_name', 'category', 'crawl_full_day') \
                    .orderBy(col('crawl_full_day').desc())
        df_tiki = df_tiki.withColumn("rn", row_number().over(window_spec)) \
                .filter(col("rn") == 1) \
                .drop("rn")
            # phân rã day
        df_tiki=df_tiki.withColumn(
           'crawl_day_of_week',
            date_format(col('crawl_full_day'), 'EEEE')
        ).withColumn(
            'crawl_month',
            date_format(col('crawl_full_day'), 'MM')
        ).withColumn(
            'crawl_year',
            date_format(col('crawl_full_day'), 'yyyy')
        ).withColumn(
            'crawl_day',
            date_format(col('crawl_full_day'), 'dd')
        )
    else:
        print('Tiki data is None')
    # lazada
        # filter
    if df_lazada is not None:
        df_lazada = df_lazada \
                .withColumnRenamed('crawl-day', 'crawl_full_day') \
                .withColumnRenamed('crawl-timestamp', 'crawl_timestamp') \
                .withColumnRenamed('discount', 'discount_rate') \
                .withColumnRenamed('price', 'product_price') \
                .withColumnRenamed('name', 'product_name') \
                .withColumnRenamed('sold', 'quantity_sold') \
                .withColumn(
                    'crawl_full_day',
                    to_date(col('crawl_full_day'), 'd-M-y')
                ).withColumn(
                    'crawl_timestamp',
                    date_format(to_timestamp(col('crawl_timestamp'), 'H:m:s'), 'HH:mm:ss')
                )
        cols = ['discount_rate', 'product_name', 'origin', 'product_price', 'quantity_sold']
        # '' processing
        for c in cols:
            df_lazada = df_lazada.withColumn(
                c,
                when(col(c)=='', None).otherwise(col(c))
            )
        # fix data type
        df_lazada = df_lazada.withColumn(
            'discount_rate',
            regexp_replace(col('discount_rate'), '% Off', '')
        ).fillna({'discount_rate': 0}).withColumn(
            'discount_rate',
            col('discount_rate').cast(IntegerType())
        )

        df_lazada = df_lazada.withColumn(
            'product_price',
            regexp_replace(col('product_price'), '[₫,]', '').cast(IntegerType())
        )
        df_lazada = df_lazada.fillna({'quantity_sold': '0'})
        temp_col = regexp_extract(col('quantity_sold'), r'([0-9]+(\.[0-9]+)?)', 1).cast("float")
        df_lazada = df_lazada.withColumn(
            'quantity_sold',
            when(col('quantity_sold').rlike('K'), temp_col*1000) \
                .otherwise(temp_col).cast(IntegerType())
        )
            # add column
        df_lazada = df_lazada.withColumn(
            'product_original_price',
            col('product_price') + (col('product_price') * col('discount_rate') / 100).cast(IntegerType())
        ).withColumn(
            'product_discount',
            col('product_original_price') - col('product_price')
        )
        # duplicate
        window_spec = Window.partitionBy("product_name", "crawl_full_day") \
           .orderBy(col("crawl_full_day").desc())
        df_lazada = df_lazada.withColumn("rn", row_number().over(window_spec)) \
                .filter(col("rn") == 1) \
                .drop("rn")
            
        df_lazada=df_lazada.withColumn(
                'crawl_day_of_week',
                date_format(col('crawl_full_day'), 'EEEE')
            ).withColumn(
                'crawl_month',
                date_format(col('crawl_full_day'), 'MM')
            ).withColumn(
                'crawl_year',
                date_format(col('crawl_full_day'), 'yyyy')
            ).withColumn(
                'crawl_day',
                date_format(col('crawl_full_day'), 'dd')
            )
    else:
        print('Lazada data is None')
# merge dataframe
    print('merge data')
    if df_tiki is not None and df_lazada is not None:
        df_merged = df_tiki.unionByName(df_lazada)
    elif df_tiki is not None:
        df_merged = df_tiki
        print('Only Tiki data available')
    elif df_lazada is not None:
        df_merged = df_lazada
        print('Only Lazada data available')
    else:
        print('No data from either source. Exiting.')
        spark.stop()
        return
    df_merged = df_merged.withColumn(
        'income',
        col('product_price').cast('bigint')*col('quantity_sold').cast('bigint')
    )
    print(f'total data row today is {df_merged.count}')
# load to silver
    print(f'load to silver day: {day}-{month}-{year}')
    try:
        output_path = f"s3a://data/silver/{day}-{month}-{year}"
        df_merged \
            .write \
            .mode("overwrite") \
            .format("parquet") \
            .save(output_path)
                    # .coalesce(1) \
        print(f"Successfully saved to S3: {output_path}")
        print('Bronze To Silver Spark Job: DONE!')
    except Exception as e:
        print(f'error {e}')
    
    spark.stop()
if __name__ == "__main__":
    main()
