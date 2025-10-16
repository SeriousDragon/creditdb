# /home/project/dags/dagenerator.py
from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo
import io
import numpy as np
import pandas as pd
import boto3
from faker import Faker
from airflow.decorators import dag, task

# ---- S3 конфиг ----
S3_ACCESS_KEY = "2f8081d9d4e74fdaaa1b42a049ddd6fe"
S3_SECRET_KEY = "a4bd0ca5dc544d71bc2a182d692cfb13"
S3_ENDPOINT  = "https://s3.ru-1.storage.selcloud.ru"
S3_BUCKET    = "thursday-project-bucket"
S3_FILE_KEY  = "credit_clients.csv"

BASE_CUSTOMER_ID = 15815691
TZ = ZoneInfo("Europe/Amsterdam")

default_args = {
    "owner": "team3",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0
}

@dag(
    dag_id="generate_credit_clients_to_s3",
    default_args = default_args,
    start_date=datetime(2025, 10, 16),
    schedule_interval="@hourly",
    catchup=False,
    params={"rows": 2222},
    tags=["s3", "data-gen", "selectel"]
)
def generate_credit_clients_to_s3():

    @task
    def make_csv(rows: int | str) -> str:
        # rows приходит шаблоном из Jinja — может быть строкой
        rows = int(rows)

        faker = Faker()
        Faker.seed(42)
        np.random.seed(4242)

        today = datetime.now(TZ).date().isoformat()

        ids = np.arange(BASE_CUSTOMER_ID + 1, BASE_CUSTOMER_ID + 1 + rows, dtype=np.int64)
        surnames = [faker.last_name() for _ in range(rows)]
        credit = np.random.randint(300, 851, size=rows, dtype=np.int32)
        geos = np.random.choice(["France", "Spain", "Germany"], size=rows)
        genders = np.random.choice(["Male", "Female"], size=rows)
        ages = np.random.randint(18, 93, size=rows, dtype=np.int32)
        tenure = np.random.randint(0, 11, size=rows, dtype=np.int32)

        mask_zero = np.random.rand(rows) < 0.70
        balance = np.where(mask_zero, 0.0, np.round(np.random.uniform(100.0, 250000.0, size=rows), 2))

        num_products = np.random.choice([1, 2, 3, 4], size=rows, p=[0.60, 0.30, 0.08, 0.02])
        has_card = np.random.choice([0, 1], size=rows, p=[0.30, 0.70])
        is_active = np.random.choice([0, 1], size=rows, p=[0.50, 0.50])
        salary = np.round(np.random.uniform(10_000.0, 200_000.0, size=rows), 2)
        exited = np.random.choice([0, 1], size=rows, p=[0.80, 0.20])

        df = pd.DataFrame({
            "Date": today,
            "CustomerId": ids,
            "Surname": surnames,
            "CreditScore": credit,
            "Geography": geos,
            "Gender": genders,
            "Age": ages,
            "Tenure": tenure,
            "Balance": balance,
            "NumOfProducts": num_products,
            "HasCrCard": has_card,
            "IsActiveMember": is_active,
            "EstimatedSalary": salary,
            "Exited": exited,
        })

        buf = io.StringIO()
        df.to_csv(buf, index=False)
        return buf.getvalue()

    @task
    def upload_to_s3(csv_text: str) -> dict:
        client = boto3.client(
            "s3",
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
        )
        client.put_object(
            Bucket=S3_BUCKET,
            Key=S3_FILE_KEY,
            Body=csv_text.encode("utf-8"),
            ContentType="text/csv",
        )
        lines = csv_text.strip().splitlines()
        return {
            "bucket": S3_BUCKET,
            "key": S3_FILE_KEY,
            "rows": max(0, len(lines) - 1),
            "date": lines[1].split(",")[0] if len(lines) > 1 else None,
        }

    # Берём rows из dag_run.conf, иначе params.rows — без get_current_context
    rows_tmpl = "{{ (dag_run.conf['rows'] if dag_run and dag_run.conf and 'rows' in dag_run.conf else params.rows) }}"
    csv_text = make_csv(rows=rows_tmpl)
    upload_to_s3(csv_text)

dag = generate_credit_clients_to_s3()