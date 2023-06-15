import csv
import pyodbc
from tqdm import tqdm

# Database connection details
server = ''
database = ''
table_name = ''
username = ''
password = ''

output_file = '.csv'

# Establishing a connection to the SQL Server database with the fast_executemany option
if username != '':  
    connection_string = f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={server};DATABASE={database};Trusted_Connection=Yes;fast_executemany=on'
else:
    connection_string = f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password};fast_executemany=on'

connection = pyodbc.connect(connection_string)

# Creating a cursor to execute SQL queries
cursor = connection.cursor()

# SQL query to retrieve data from the table
sql_query = f'SELECT * FROM {table_name}'

# Executing the SQL query
cursor.execute(sql_query)

# Fetching the first row to get column names
column_names = [column[0] for column in cursor.description]

# Closing the cursor
cursor.close()

# Reconnecting with fast_executemany option for actual data retrieval
connection = pyodbc.connect(connection_string + ";fast_executemany=on")
cursor = connection.cursor()

# Executing the SQL query
cursor.execute(f"SELECT COUNT(*) from {table_name}")
rowCount=cursor.fetchone()[0]
cursor.close()

cursor = connection.cursor()
cursor.execute(sql_query)

# Fetching all the rows from the cursor
rows = []
counter = 0
with tqdm() as progress_bar:
    while True:
        # Fetching a batch of rows
        batch = cursor.fetchmany(80000)  # Adjust the batch size as needed
        
        if not batch:
            # No more rows to fetch
            break
        
        # Appending the batch of rows to the main list
        rows.extend(batch)
        
        # Updating the counter
        counter += len(batch)
        
        # Updating the progress bar
        percentage = round((counter/rowCount) * 100, 2)
        progress_bar.update(len(batch))
        progress_bar.set_description(f'Progress: {counter}/{rowCount} ({percentage}%)')


# Closing the cursor and the database connection
cursor.close()
connection.close()

# Writing the data to a CSV file
with open(output_file, 'w', newline='') as csv_file:
    writer = csv.writer(csv_file)
    
    # Writing the column names as the header
    writer.writerow(column_names)
    
    # Writing the data rows
    writer.writerows(rows)

print(f'Data extracted and saved to {output_file} successfully.')
