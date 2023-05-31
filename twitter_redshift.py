import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import psycopg2
from airflow.hooks.base_hook import BaseHook

# Redshift credentials
host = 'your-hostname'
port = 'your-port'
database = 'your-database'
user = 'your-username'
password = 'your-password'

s3_bucket = 'bancobari-raw-381733490365'
s3_file = 'twitter/redecoxa.parquet'

# Loading parquet file from S3 to a DF
s3_path = f's3://{s3_bucket}/{s3_file}'
df = pd.read_parquet(s3_path)

# Redshift Connection
conn = psycopg2.connect(
    host=host,
    port=port,
    database=database,
    user=user,
    password=password
)

# Creating tables
cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS redecoxa (
        user VARCHAR(255),
        text VARCHAR(255),
        favorite_count INT,
        retweet_count INT,
        created_at TIMESTAMP
    )
""")

conn.commit()

# Carregamento do DataFrame para o Redshift
cur = conn.cursor()

# Drop temporário para permitir carregamento de Parquet
cur.execute("DROP TABLE IF EXISTS redecoxa_temp")

# Carregamento do Parquet para uma tabela temporária
table_name = 'redecoxa_temp'
pq.write_table(pa.Table.from_pandas(df), f's3://{s3_bucket}/{table_name}.parquet')
copy_query = f"""
    COPY {table_name}
    FROM 's3://{s3_bucket}/{table_name}.parquet'
    IAM_ROLE 'arn:aws:iam::381733490365:role/spectrumRoles'
    FORMAT AS PARQUET
"""
cur.execute(copy_query)

# Inserção dos dados da tabela temporária para a tabela final
insert_query = f"""
    INSERT INTO redecoxa
    SELECT * FROM {table_name}
"""
cur.execute(insert_query)

conn.commit()