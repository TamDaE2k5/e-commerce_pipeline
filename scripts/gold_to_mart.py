import pandas as pd
from minio import Minio
from clickhouse_driver import Client
from io import BytesIO
# from dotenv import load_dotenv
# import os
# load_dotenv(dotenv_path='.env')
# PASS_CLICK_HOUSE=os.getenv('PASS_CLICK_HOUSE')
minio_conn = Minio(
    'minio:9000',
    access_key='minioadmin',
    secret_key='minioadmin',
    secure=False
)

client = Client(
    host='clickhouse-server',
    port=9000,
    user='default',
    password='your_password'
)

def read_data(bucketname, prefix):
    try:
        objs = minio_conn.list_objects(bucket_name=bucketname, prefix=prefix, recursive=True)
        dfs = []
        for obj in objs:
            if obj.object_name.endswith('.parquet'):
                print(f'read file {obj.object_name}')
                res = minio_conn.get_object(bucketname, obj.object_name)
                data = res.read()
                res.close()
                res.release_conn()

                df = pd.read_parquet(BytesIO(data))
                dfs.append(df)

        if dfs:
            combined_df = pd.concat(dfs, ignore_index=True)
            print('read data ok')
            return combined_df
        return None
    except Exception as e:
        print('err', e)

def insert_data(df, table):
    try:
        if isinstance(df.index, pd.MultiIndex):
            df = df.reset_index()
        data = df.to_records(index=False).tolist()
        columns = ', '.join(df.columns)
        query = f"INSERT INTO data_mart.{table} ({columns}) VALUES"
            
            # Thực hiện insert
        client.execute(query, data)
        print(f"Đã chèn {len(data)} rows vào bảng {table}")
    except Exception as e:
        print('err', e)

def main():
    dim_category = read_data('data','gold/star1/dim_category/')
    dim_date = read_data('data','gold/star1/dim_date/')
    dim_product = read_data('data','gold/star1/dim_product/')
    dim_source = read_data('data','gold/star1/dim_source/')
    fact = read_data('data','gold/star1/fact_product_sale/')


    obt = dim_date.merge(fact, on='date_key', how='inner') \
                .merge(dim_product, on='product_key', how='inner') \
                .merge(dim_source, on='source_key', how='inner')
    obt.head()

    df_trend_day_of_week_pandas = (
        obt
        .groupby('crawl_day_of_week', as_index=False)
        .agg(
            total_quantity_sold=('quantity_sold', 'sum'),
            total_income=('income', 'sum'),
            avg_quantity_sold=('quantity_sold', 'mean')
        )
    )

    df_trend_category_pandas = (
        obt
        .groupby('category_name', as_index=False)
        .agg(
            total_quantity_sold=('quantity_sold', 'sum'),
            total_income=('income', 'sum'),
            avg_quantity_sold=('quantity_sold', 'mean')
        )
    )

    df_trend_category_dayofweek_pandas = (
        obt
        .groupby(['category_name','crawl_day_of_week'], as_index=False)
        .agg(
            total_quantity_sold=('quantity_sold', 'sum'),
            total_income=('income', 'sum'),
            avg_quantity_sold=('quantity_sold', 'mean')
        )
    )

    df_marketplace_pandas = (
        obt
        .groupby(['source_name'], as_index=False)
        .agg(
            total_quantity_sold=('quantity_sold', 'sum'),
            total_income=('income', 'sum'),
            avg_quantity_sold=('quantity_sold', 'mean')
        )
    )

    df_marketplace_category_pandas = (
        obt
        .groupby(['source_name', 'category_name'], as_index=False)
        .agg(
            total_quantity_sold=('quantity_sold', 'sum'),
            total_income=('income', 'sum'),
            avg_quantity_sold=('quantity_sold', 'mean'),
            avg_price=('product_price', 'mean'),
            total_price=('product_price', 'sum')
        )
    )

    df_marketplace_category_dayofweek_pandas = (
        obt
        .groupby(['source_name', 'category_name', 'crawl_day_of_week'], as_index=False)
        .agg(
            total_quantity_sold=('quantity_sold', 'sum'),
            total_income=('income', 'sum'),
            avg_quantity_sold=('quantity_sold', 'mean')
        )
    )

    df_marketplace_category_pandas = (
        obt
        .groupby(['source_name', 'category_name'], as_index=False)
        .agg(
            total_quantity_sold=('quantity_sold', 'sum'),
            total_income=('income', 'sum'),
            avg_quantity_sold=('quantity_sold', 'mean')
        )
    )

    client.execute('USE data_mart')

    insert_data(df_trend_day_of_week_pandas, 'df_trend_day_of_week')
    insert_data(df_trend_category_pandas, 'df_trend_category')
    insert_data(df_trend_category_dayofweek_pandas, 'df_trend_category_dayofweek')
    insert_data(df_marketplace_pandas, 'df_marketplace')
    insert_data(df_marketplace_category_pandas, 'df_marketplace_category')
    insert_data(df_marketplace_category_dayofweek_pandas, 'df_marketplace_category_dayofweek')
    
    print('df_trend_day_of_week', df_trend_day_of_week_pandas.shape)
    print('df_trend_category', df_trend_category_pandas.shape)
    print('df_trend_category_dayofweek', df_trend_category_dayofweek_pandas.shape)
    print('df_marketplace', df_marketplace_pandas.shape)
    print('df_marketplace_category', df_marketplace_category_pandas.shape)
    print('df_marketplace_category_dayofweek', df_marketplace_category_dayofweek_pandas.shape)

    client.disconnect()

if __name__ == '__main__':
    main()