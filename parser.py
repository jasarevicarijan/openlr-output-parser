import json
import psycopg2
from datetime import datetime

# Define the PostgreSQL connection parameters
db_params = {
    'host': 'your_host_name',
    'database': 'your_database_name',
    'user': 'your_user',
    'password': 'your_password'
}

# Define the SQL queries templates
create_query = "INSERT INTO {table} ({columns}) VALUES ({values});"
update_query = "UPDATE {table} SET {set_clause} WHERE {where_clause};"
delete_query = "DELETE FROM {table} WHERE {where_clause};"

# Establish a database connection
conn = psycopg2.connect(**db_params)
cur = conn.cursor()

# Define the SQL query for creating table
create_table_sql = """
CREATE TABLE ADAM1 (
    ID serial PRIMARY KEY,
    NAME VARCHAR(255),
    COUNT INT,
    START_TIME TIMESTAMP
);
"""
cur.execute(create_table_sql)

# Open and read the 'results.txt' file
with open('results.txt', 'r') as file:
    lines = file.readlines()

# Iterate through each line (JSON record) in the file
for line in lines:
    record = json.loads(line)

    if 'payload' in record:
        payload = record['payload'][0]

        if 'schema' in payload and 'table' in payload['schema']:
            table_name = payload['schema']['table']
            if 'op' in payload:
                if payload['op'] == 'c':
                    # Create operation
                    columns = ', '.join(payload['after'].keys())
                    values = []
                    for key, value in payload['after'].items():
                        if key == 'START_TIME':
                            try:
                                timestamp = datetime.utcfromtimestamp(value / 1000000000)  # Assuming value is in nanoseconds
                                values.append(f"'{timestamp}'")
                            except Exception as e:
                                print(f"Error converting timestamp: {e}")
                                values.append("NULL")  # Handle invalid timestamps by inserting NULL
                        else:
                            values.append(f"'{value}'" if isinstance(value, str) else str(value))

                    create_sql = create_query.format(table=table_name, columns=columns, values=', '.join(values))
                    try:
                        cur.execute(create_sql)
                    except psycopg2.Error as e:
                        print(f"Error executing SQL query: {e}")
                elif payload['op'] == 'u':
                    # Update operation
                    set_clause = ', '.join([f"{key} = '{value}'" if isinstance(value, str) else f"{key} = {value}" for key, value in payload['after'].items()])
                    where_clause = f"ID = {payload['before']['ID']}"
                    try:
                        if 'START_TIME' in payload['after']:
                            timestamp = payload['after']['START_TIME']
                            timestamp = datetime.utcfromtimestamp(timestamp / 1000000000)
                            set_clause = set_clause.replace("'START_TIME': {timestamp}'", "'START_TIME': '{timestamp}'")
                        update_sql = update_query.format(table=table_name, set_clause=set_clause, where_clause=where_clause)
                        cur.execute(update_sql)
                    except psycopg2.Error as e:
                        print(f"Error executing SQL query: {e}")
                elif payload['op'] == 'd':
                    # Delete operation
                    where_clause = f"ID = {payload['before']['ID']}"
                    delete_sql = delete_query.format(table=table_name, where_clause=where_clause)
                    try:
                        cur.execute(delete_sql)
                    except psycopg2.Error as e:
                        print(f"Error executing SQL query: {e}")


# Commit the changes and close the database connection
conn.commit()
cur.close()
conn.close()
