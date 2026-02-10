from clickhouse_driver import Client
# from dotenv import load_dotenv
# import os

# load_dotenv(dotenv_path='.env')
# PASS_CLICK_HOUSE = os.getenv('PASS_CLICK_HOUSE')

def main():
    client = Client(
        host='clickhouse-server',
        port=9000,
        user='default',
        password='your_password',
    )

    # 1. Kiểm tra và xóa database nếu tồn tại
    result = client.execute('EXISTS DATABASE data_mart')
    # Kết quả trả về: [(1,)] nếu tồn tại, [(0,)] nếu không
    if result and result[0][0] == 1:
        client.execute('DROP DATABASE data_mart')
        print("Database data_mart dropped.")

    # 2. Tạo database mới
    client.execute('CREATE DATABASE data_mart')
    print("Database data_mart created.")
    client.execute('USE data_mart')
    # 3. Tạo các bảng
    client.execute('''
        CREATE TABLE data_mart.df_trend_day_of_week (
                crawl_day_of_week String,
                total_quantity_sold  UInt64,
                total_income UInt128,
                avg_quantity_sold Float64 )
        ENGINE = MergeTree
        ORDER BY crawl_day_of_week
    ''')
    print("Table df_trend_day_of_week created.")

    client.execute('''
        CREATE TABLE data_mart.df_trend_category (
                category_name String,
                total_quantity_sold  UInt64,
                total_income UInt128,
                avg_quantity_sold Float64 )
        ENGINE = MergeTree
        ORDER BY category_name
    ''')
    print("Table df_trend_category created.")

    client.execute('''
        CREATE TABLE data_mart.df_trend_category_dayofweek(
                category_name String,
                crawl_day_of_week String,
                total_quantity_sold UInt64,
                total_income UInt128,
                avg_quantity_sold Float64)
        ENGINE = MergeTree
        ORDER BY category_name
    ''')
    print("Table df_trend_category_dayofweek created.")
    
    client.execute('''
        CREATE TABLE data_mart.df_marketplace (
                source_name String,
                total_quantity_sold UInt64,
                total_income UInt128,
                avg_quantity_sold Float64)
        ENGINE = MergeTree
        ORDER BY source_name
    ''')
    print("Table df_marketplace created.")

    client.execute('''
        CREATE TABLE data_mart.df_marketplace_category_dayofweek (
                source_name String,
                category_name String,
                crawl_day_of_week String,
                total_quantity_sold UInt64,
                total_income UInt128,
                avg_quantity_sold Float64)
        ENGINE = MergeTree
        ORDER BY (category_name, source_name)
    ''')
    print("Table df_marketplace_category_dayofweek created.")
    
    client.execute('''
        CREATE TABLE data_mart.df_marketplace_category (
                source_name String,
                category_name String,
                total_quantity_sold UInt64,
                total_income UInt128,
                avg_quantity_sold Float64)
        ENGINE = MergeTree
        ORDER BY (category_name, source_name)
    ''')
    print("Table df_marketplace_category created.")

if __name__ == '__main__':
    main()