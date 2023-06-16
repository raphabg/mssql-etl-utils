import pandas as pd
from sqlalchemy import create_engine, URL
from tqdm import tqdm

# SQL Server connection details
server = ''
database = ''
username = ''
password = ''
driver = ''  # Adjust the driver based on your SQL Server version

# CSV file details
csv_file = r''
encoding = 'utf-8'
schema = 'dbo'
table_name = ''
chunksize = 80000

# Create SQL Server connection
if username != '':
    connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'
else:
    connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=Yes'

connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
engine = create_engine(connection_url, echo=False, fast_executemany=True)

df = pd.read_csv(csv_file, dtype=str, encoding=encoding, chunksize=chunksize)

counter = 0
with tqdm(unit='rows') as progress_bar:
    for i, chunk in enumerate(df):
        if_exists = 'append'

        if (i == 0):
            if_exists = 'replace'

        chunk.to_sql(table_name, engine, schema, if_exists, index=False)

        rowCount = chunk.shape[0]
        counter += rowCount

        progress_bar.update(len(counter))
        progress_bar.set_description(f'Progress: {counter})')


print("CSV data inserted into the SQL Server table.")
