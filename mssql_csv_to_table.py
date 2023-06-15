import csv
import pyodbc

# Database connection details

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

# Create the SQL INSERT query
insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['?'] * len(column_names))})"

# Open the CSV file
with open(csv_file_path, 'r', newline='') as csv_file:
    reader = csv.reader(csv_file)
    
    # Read the column names from the first row
    column_names = next(reader)
    
    # Prepare the SQL INSERT statement
    insert_statement = pyodbc.prepare(cursor, insert_query)
    
    # Read and insert the data rows
    rows = []
    batch_size = 1000  # Adjust the batch size as needed
    for row in reader:
        rows.append(row)
        
        # Insert a batch of rows
        if len(rows) >= batch_size:
            cursor.executemany(insert_statement, rows)
            connection.commit()
            rows = []
    
    # Insert any remaining rows
    if rows:
        cursor.executemany(insert_statement, rows)
        connection.commit()

# Closing the cursor and the database connection
cursor.close()
connection.close()

print('Data imported successfully.')
