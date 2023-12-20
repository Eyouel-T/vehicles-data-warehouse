import pandas as pd
import csv
import psycopg2


def load_data(filename):
    # Initialize the output lists
    vehicle_rows = []
    timed_vehicle_rows = []

    with open(filename, 'r') as file:
        # Create a CSV reader object
        reader = csv.reader(file, delimiter=';')

        # Process each row in the CSV file
        
        for row in reader:
            
            # Split the row into items based on the delimiter
            items = [item.strip() for item in row]

            # Append the first four items to vehicle_rows
            vehicle_rows.append(items[:4])

            # Process the remaining items in chunks of six
            remaining_items = items[4:]
            for i in range(0, len(remaining_items), 6):
                chunk = remaining_items[i:i+6]
                timed_vehicle_rows.append([items[0]] + chunk)

    # Create a dataframe for vehicle_rows
    vehicle_df = pd.DataFrame(vehicle_rows, columns=['track_id', 'type', 'traveled_d', 'avg_speed'])
    vehicle_df = vehicle_df.iloc[1:] 
    print(len(vehicle_df))
    # Create a dataframe for timed_vehicle_rows
    timed_vehicle_df = pd.DataFrame(timed_vehicle_rows, columns=['track_id', 'lat', 'lon', 'speed', 'lon_acc', 'lat_acc', 'time'])
    timed_vehicle_df = timed_vehicle_df.iloc[1:] 
    print(len(timed_vehicle_df))
    return vehicle_df, timed_vehicle_df












'''def get_csv_columns(file_path):
    with open(file_path, 'r') as f:
        reader = csv.reader(f, delimiter=';')
        header = next(reader)
    return header

def guess_data_type(value):
    try:
        int(value)
        return 'integer'
    except ValueError:
        try:
            float(value)
            return 'numeric'
        except ValueError:
            return 'text'


def create_table_from_csv(file_path, table_name, connection):
    columns = get_csv_columns(file_path)
    data_types = [guess_data_type(value) for value in columns]

    create_table_query = f"CREATE TABLE {table_name} ("
    for column, data_type in zip(columns, data_types):
        create_table_query += f"{column} {data_type}, "
    create_table_query = create_table_query.rstrip(', ') + ")"

    with connection.cursor() as cursor:
        cursor.execute(create_table_query)

    return columns, data_types

def load_data_into_table(file_path, table_name, connection):
    with open(file_path, 'r') as f:
        with connection.cursor() as cursor:
            #cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER", f)
            cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH (FORMAT CSV, DELIMITER ';', HEADER)", f)

# PostgreSQL connection details
conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database="week2",
    user="postgres",
    password="root"
)

# Specify the CSV file path and table name
csv_file_path = 'data.csv'
table_name = 'traffic'

# Create the table dynamically based on the CSV file
#columns, data_types = create_table_from_csv(csv_file_path, table_name, conn)

# Load the data from the CSV file into the table
#load_data_into_table(csv_file_path, table_name, conn)

# Commit the changes and close the connection
conn.commit()
conn.close()
'''