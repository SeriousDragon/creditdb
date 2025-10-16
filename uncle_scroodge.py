# dags/hourly_new_clients_etl_s3_checked.py
from datetime import datetime, timedelta
from airflow.decorators import dag, task
import pandas as pd
from pandas.errors import EmptyDataError
import boto3, logging, os, tempfile

import psycopg2
from psycopg2.extras import execute_values

# =========================
# Конфигурация
# =========================
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "123"
POSTGRES_HOST = "127.0.0.1"
POSTGRES_PORT = "5432"
POSTGRES_DB = "creditdb"

# Selectel S3
S3_ACCESS_KEY = "2f8081d9d4e74fdaaa1b42a049ddd6fe"
S3_SECRET_KEY = "a4bd0ca5dc544d71bc2a182d692cfb13"
S3_ENDPOINT  = "https://s3.ru-1.storage.selcloud.ru"
S3_BUCKET    = "thursday-project-bucket"
S3_FILE_KEY  = "credit_clients.csv"

# Ожидаемые CSV-колонки (из файла)
CSV_COLS_CAMEL = [
    "CustomerId","Surname","CreditScore","Geography","Gender","Age","Tenure",
    "Balance","NumOfProducts","HasCrCard","IsActiveMember","EstimatedSalary","Exited"
]
# Целевая snake_case-схема таблицы
DB_COLS_SNAKE = [
    "customer_id","surname","credit_score","geography","gender","age","tenure",
    "balance","num_products","has_cr_card","is_active_member","estimated_salary","exited"
]
# Альтернатива: CamelCase-колонки в уже созданной таблице
DB_COLS_CAMEL = CSV_COLS_CAMEL[:]  # идентичны CSV

default_args = {
    "owner": "team3",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# =========================
# Вспомогательные функции
# =========================
def _pg_conn():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=POSTGRES_DB,
    )

def _ensure_table_snake():
    """Создаёт snake_case-таблицу, если её нет (не ломает существующую CamelCase)."""
    ddl = """
    CREATE TABLE IF NOT EXISTS clients (
        customer_id       BIGINT PRIMARY KEY,
        surname           TEXT,
        credit_score      INT,
        geography         TEXT,
        gender            TEXT,
        age               INT,
        tenure            INT,
        balance           DOUBLE PRECISION,
        num_products      INT,
        has_cr_card       INT,
        is_active_member  INT,
        estimated_salary  DOUBLE PRECISION,
        exited            INT,
        created_at        TIMESTAMPTZ DEFAULT NOW()
    );
    """
    with _pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()

def _get_existing_columns():
    """Возвращает сет имён колонок существующей таблицы clients (точное имя, с регистром)."""
    sql = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'clients'
    ORDER BY ordinal_position;
    """
    with _pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    return {r[0] for r in rows}

def _detect_mode(existing_cols: set[str]) -> str:
    """Возвращает 'snake' | 'camel' в зависимости от колонок таблицы."""
    if not existing_cols:
        return "snake"  # таблицы нет — создадим snake_case
    if set(DB_COLS_SNAKE).issubset(existing_cols):
        return "snake"
    if set(DB_COLS_CAMEL).issubset(existing_cols):
        return "camel"
    # частично несовпадающая схема
    raise RuntimeError(
        f"Схема таблицы 'clients' не совпадает с ожидаемой. Имеется: {sorted(existing_cols)}"
    )

def _infer_compression(filepath: str) -> str | None:
    with open(filepath, "rb") as f:
        sig = f.read(4)
    if sig[:2] == b"\x1f\x8b":
        return "gzip"
    if sig[:4] == b"PK\x03\x04":
        return "zip"
    return None

def _read_csv_robust(filepath: str) -> pd.DataFrame:
    if os.path.getsize(filepath) == 0:
        logging.info("CSV пустой (0 байт) — возвращаю пустой DataFrame.")
        return pd.DataFrame(columns=CSV_COLS_CAMEL)

    compression = _infer_compression(filepath)
    last_err = None
    for enc in ("utf-8-sig", "utf-8", "cp1251"):
        for sep in (None, ";", ","):
            try:
                df = pd.read_csv(
                    filepath,
                    encoding=enc,
                    sep=sep,            # sep=None => autodetect (engine='python')
                    engine="python",
                    on_bad_lines="skip",
                    compression=compression,
                )
                return df
            except EmptyDataError:
                logging.info("EmptyDataError — возвращаю пустой DataFrame.")
                return pd.DataFrame(columns=CSV_COLS_CAMEL)
            except Exception as e:
                last_err = e
                continue
    raise last_err if last_err else RuntimeError("Не удалось прочитать CSV.")

def _coerce_schema_to_camel(df: pd.DataFrame) -> pd.DataFrame:
    # Добавим недостающие CSV-колонки как NA, лишние отбросим
    for c in CSV_COLS_CAMEL:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[CSV_COLS_CAMEL]

    # Мягкое приведение типов
    def to_int(col):   return pd.to_numeric(df[col], errors="coerce").astype("Int64")
    def to_float(col): return pd.to_numeric(df[col], errors="coerce").astype("float64")
    def to_str(col):   return df[col].astype("string")

    df["CustomerId"]     = to_int("CustomerId")
    df["Surname"]        = to_str("Surname")
    df["CreditScore"]    = to_int("CreditScore")
    df["Geography"]      = to_str("Geography")
    df["Gender"]         = to_str("Gender")
    df["Age"]            = to_int("Age")
    df["Tenure"]         = to_int("Tenure")
    df["Balance"]        = to_float("Balance")
    df["NumOfProducts"]  = to_int("NumOfProducts")
    df["HasCrCard"]      = to_int("HasCrCard")
    df["IsActiveMember"] = to_int("IsActiveMember")
    df["EstimatedSalary"]= to_float("EstimatedSalary")
    df["Exited"]         = to_int("Exited")

    df = df.dropna(subset=["CustomerId"])
    return df

def _camel_to_snake_df(df_camel: pd.DataFrame) -> pd.DataFrame:
    rename_map = {
        "CustomerId":"customer_id",
        "Surname":"surname",
        "CreditScore":"credit_score",
        "Geography":"geography",
        "Gender":"gender",
        "Age":"age",
        "Tenure":"tenure",
        "Balance":"balance",
        "NumOfProducts":"num_products",
        "HasCrCard":"has_cr_card",
        "IsActiveMember":"is_active_member",
        "EstimatedSalary":"estimated_salary",
        "Exited":"exited",
    }
    out = df_camel.rename(columns=rename_map)
    # упорядочим
    for c in DB_COLS_SNAKE:
        if c not in out.columns:
            out[c] = pd.NA
    return out[DB_COLS_SNAKE]

def _df_to_rows(df: pd.DataFrame, cols: list[str]):
    rows = []
    it = df[cols].itertuples(index=False, name=None)
    for tup in it:
        # Конвертация pandas NA -> None
        norm = []
        for v in tup:
            if pd.isna(v):
                norm.append(None)
            else:
                norm.append(v.item() if hasattr(v, "item") else v)
        rows.append(tuple(norm))
    return rows

# =========================
# DAG
# =========================
@dag(
    dag_id="hourly_new_clients_etl_s3_checked_get_rekt",
    description="ETL: Selectel S3 → PostgreSQL (устойчиво, идемпотентно, авто-детект схемы)",
    schedule_interval="@hourly",
    start_date=datetime(2025, 10, 16),
    catchup=False,
    tags=["bank","clients","s3"],
    max_active_runs=1,
    default_args=default_args,
)
def etl_selectel_s3_checked():

    @task()
    def ensure_table():
        _ensure_table_snake()
        logging.info("Таблица clients (snake_case) создана при отсутствии.")

    @task()
    def fetch_new_clients() -> list:
        """Скачиваем CSV из S3 во временный файл, читаем устойчиво, приводим к CSV-схеме (CamelCase)."""
        s3 = boto3.client(
            "s3",
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
            endpoint_url=S3_ENDPOINT,
        )
        head = s3.head_object(Bucket=S3_BUCKET, Key=S3_FILE_KEY)
        size = head.get("ContentLength", 0) or 0
        logging.info("S3 object size: %d bytes", size)
        if size == 0:
            logging.info("Файл пуст — данных нет.")
            return []

        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
            tmp_path = tmp.name
        s3.download_file(S3_BUCKET, S3_FILE_KEY, tmp_path)

        try:
            df_raw = _read_csv_robust(tmp_path)
            df = _coerce_schema_to_camel(df_raw)
            logging.info("Прочитано строк: raw=%d, normalized=%d", len(df_raw), len(df))
            return df.to_dict(orient="records")
        finally:
            try:
                os.remove(tmp_path)
            except Exception:
                pass

    @task()
    def insert_new_clients(new_clients_list: list):
        """Вставка пачками, поддерживаются обе схемы таблицы: snake_case и CamelCase."""
        if not new_clients_list:
            logging.info("Нет данных для вставки.")
            return

        df_camel = pd.DataFrame(new_clients_list)
        if df_camel.empty:
            logging.info("Пустой DataFrame — вставка не требуется.")
            return

        # Определяем схему таблицы
        existing_cols = _get_existing_columns()
        mode = _detect_mode(existing_cols)
        logging.info("Режим вставки: %s", mode)

        with _pg_conn() as conn:
            with conn.cursor() as cur:
                if mode == "snake":
                    df_snake = _camel_to_snake_df(df_camel)

                    cols = DB_COLS_SNAKE
                    rows = _df_to_rows(df_snake, cols)
                    if not rows:
                        logging.info("После подготовки строк нет.")
                        return

                    sql = f"""
                    INSERT INTO clients ({", ".join(cols)})
                    VALUES %s
                    ON CONFLICT (customer_id) DO NOTHING;
                    """
                    execute_values(
                        cur,
                        sql,
                        rows,
                        template="(" + ",".join(["%s"]*len(cols)) + ")",
                        page_size=10_000,
                    )

                else:  # mode == "camel"
                    # столбцы уже CamelCase в таблице
                    cols = DB_COLS_CAMEL
                    # гарантируем порядок
                    df_camel = df_camel[cols]
                    rows = _df_to_rows(df_camel, cols)
                    if not rows:
                        logging.info("После подготовки строк нет.")
                        return

                    sql = """
                    INSERT INTO clients (
                        "CustomerId","Surname","CreditScore","Geography","Gender","Age","Tenure",
                        "Balance","NumOfProducts","HasCrCard","IsActiveMember","EstimatedSalary","Exited"
                    ) VALUES %s
                    ON CONFLICT ("CustomerId") DO NOTHING;
                    """
                    execute_values(
                        cur,
                        sql,
                        rows,
                        template="(" + ",".join(["%s"]*len(cols)) + ")",
                        page_size=10_000,
                    )

            conn.commit()

        logging.info("Вставка завершена (режим: %s).", mode)

    # Оркестрация
    t0 = ensure_table()
    recs = fetch_new_clients()
    ins  = insert_new_clients(recs)

    t0 >> recs >> ins

etl_s3_dag = etl_selectel_s3_checked()