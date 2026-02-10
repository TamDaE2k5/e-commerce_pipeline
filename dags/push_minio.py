from minio import Minio
from datetime import datetime
from pathlib import Path

def push():
    now = datetime.now()
    date_str = f"{now.day}-{now.month}-{now.year}"
    client = Minio(
        "minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

    bucket_name = "data"
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        
    base_path = Path("/opt/airflow/bronze")
    path = base_path / date_str

    if not path.exists():
        raise FileNotFoundError(f"Directory not found: {path}")

    for file in path.iterdir():
        if file.is_file():
            object_name = f"bronze/{path.name}/{file.name}"

            client.put_object(
                bucket_name=bucket_name,
                object_name=object_name,
                data=file.open("rb"),
                length=file.stat().st_size,
                content_type="application/json"
            )

            print(f"Uploaded: {object_name}")
