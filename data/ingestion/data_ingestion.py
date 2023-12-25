import pandas as pd
from sqlalchemy import create_engine

db_connection = 'postgresql://username:password@localhost:5432/db'

# Create database engine
engine = create_engine(db_connection)

# Function to read data from a table
def read_table(table_name):
    query = f"SELECT * FROM {table_name};"
    df = pd.read_sql(query, engine)
    return df

# read data from each table
df1 = read_table("table1")
df2 = read_table("table2")
df3 = read_table("table3")
df4 = read_table("table4")

# saving each df to a Parquet file
df1.to_parquet('/path/to/data/df1.parquet')
df2.to_parquet('/path/to/data/df2.parquet')
df3.to_parquet('/path/to/data/df3.parquet')
df4.to_parquet('/path/to/data/df4.parquet')
