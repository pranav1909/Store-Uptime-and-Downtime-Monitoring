from flask import Flask, jsonify, request
import csv
import pandas as pd
from pytz import timezone, utc
from datetime import datetime, timedelta
from collections import defaultdict
import pytz
import sqlite3

app = Flask(__name__)

# Global variable to store the report data
report_data = None

# Create a dictionary to store the store business hours
store_business_hours = defaultdict(list)

# Function to ingest data from CSVs and store in the database (assuming SQLite for simplicity)
def ingest_data_from_csv(csv_path, table_name):
    # Read the CSV file
    data = pd.read_csv(csv_path)

    # Connect to the database (assuming SQLite)
    import sqlite3
    conn = sqlite3.connect('store_data.db')

    # Save the data to the database table
    data.to_sql(table_name, conn, if_exists='replace', index=False)

    # Close the database connection
    conn.close()

# Function to get store business hours
def get_store_business_hours(csv_path):
    with open(csv_path, 'r') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Skip header row
        for row in reader:
            store_id, day_of_week, start_time_local, end_time_local = row
            store_business_hours[int(store_id)].append((int(day_of_week), start_time_local, end_time_local))

# Function to calculate uptime and downtime for each store
def calculate_uptime_downtime():
    # Connect to the database (assuming SQLite)
    conn = sqlite3.connect('store_data.db')

    # Get the max timestamp from the first CSV as the current timestamp
    cur = conn.cursor()
    cur.execute("SELECT MAX(timestamp_utc) FROM store_status")
    current_timestamp = cur.fetchone()[0]
    current_timestamp = datetime.strptime(current_timestamp, '%Y-%m-%d %H:%M:%S.%f')

    # Convert current_timestamp to UTC
    current_timestamp = current_timestamp.replace(tzinfo=timezone('UTC'))

    # Calculate uptime and downtime for each store
    store_uptime_downtime = defaultdict(lambda: defaultdict(int))

    # Helper function to convert local time to UTC time
    def local_to_utc(store_id, local_time_str):
        timezone_str = store_timezones.get(store_id, 'America/Chicago')

        local = pytz.timezone(timezone_str)
        naive = datetime.strptime(local_time_str, "%H:%M:%S")
        local_dt = local.localize(naive, is_dst=None)
        utc_dt = local_dt.astimezone(pytz.utc)

        return utc_dt

    # Get store status data
    cur.execute("SELECT * FROM store_status")
    store_status_data = cur.fetchall()

    # Get store business hours data
    cur.execute("SELECT * FROM store_business_hours")
    store_business_hours_data = cur.fetchall()
    store_business_hours = {int(store_id): (day, start_time_local, end_time_local) for store_id, day, start_time_local, end_time_local in store_business_hours_data}

    # Get store timezones data
    cur.execute("SELECT * FROM store_timezones")
    store_timezones_data = cur.fetchall()

    # Create a dictionary to store the timezone for each store
    store_timezones = {int(store_id): timezone_str for store_id, timezone_str in store_timezones_data}

    # Initialize variables before the loop
    prev_end_time_utc = None
    prev_status = None

    for store_id, status, timestamp_utc in store_status_data:
        timestamp_utc = datetime.strptime(timestamp_utc, '%Y-%m-%d %H:%M:%S.%f')

        # Convert timestamp_utc to an offset-aware datetime with UTC timezone
        timestamp_utc = timestamp_utc.replace(tzinfo=pytz.UTC)
    
        # Convert timestamp_utc to the local timezone for the store
        store_timezone = store_timezones.get(store_id, 'America/Chicago')
        local_time = timestamp_utc.astimezone(pytz.timezone(store_timezone))

        # Find the current day of the week (0=Monday, 6=Sunday)
        day_of_week = local_time.weekday()

        # Find the business hours for the store on the current day
        business_hours = store_business_hours.get(store_id, None)

        if business_hours is not None:
            day, start_time_local, end_time_local = business_hours

            if day == day_of_week:
                start_time_utc = local_to_utc(store_id, start_time_local)
                end_time_utc = local_to_utc(store_id, end_time_local)

                # Calculate the time difference between consecutive status observations
                if prev_end_time_utc is not None:
                    time_diff = (start_time_utc - prev_end_time_utc).total_seconds() / 60.0
                    store_uptime_downtime[store_id]['uptime'] += time_diff if prev_status == 'active' else 0
                    store_uptime_downtime[store_id]['downtime'] += time_diff if prev_status == 'inactive' else 0

                # Calculate the time difference from the last status observation to the current observation
                if prev_end_time_utc is not None:
                    time_diff = (timestamp_utc - end_time_utc).total_seconds() / 60.0
                    store_uptime_downtime[store_id]['uptime'] += time_diff if status == 'active' else 0
                    store_uptime_downtime[store_id]['downtime'] += time_diff if status == 'inactive' else 0

                # Save the current end time and status as the previous end time and status for the next iteration
                prev_end_time_utc = end_time_utc
                prev_status = status



    # Close the database connection
    conn.close()

    # Convert uptime and downtime to hours
    for store_id in store_uptime_downtime:
        store_uptime_downtime[store_id]['uptime'] /= 60
        store_uptime_downtime[store_id]['downtime'] /= 60

    return store_uptime_downtime


# Function to generate the report
def generate_report():
    global report_data
    # Calculate uptime and downtime for each store
    store_uptime_downtime = calculate_uptime_downtime()

    # Generate the report with the required metrics
    report_data = []
    for store_id in store_uptime_downtime:
        uptime_last_hour = store_uptime_downtime[store_id]['uptime']
        downtime_last_hour = store_uptime_downtime[store_id]['downtime']
        uptime_last_day = uptime_last_hour * 24
        downtime_last_day = downtime_last_hour * 24
        uptime_last_week = uptime_last_day * 7
        downtime_last_week = downtime_last_day * 7

        report_data.append({
            'store_id': store_id,
            'uptime_last_hour': uptime_last_hour,
            'downtime_last_hour': downtime_last_hour,
            'uptime_last_day': uptime_last_day,
            'downtime_last_day': downtime_last_day,
            'uptime_last_week': uptime_last_week,
            'downtime_last_week': downtime_last_week,
        })

# Define API endpoints

@app.route('/trigger_report', methods=['GET'])
def trigger_report():
    if request.method == 'GET':
        # Ingest data from CSVs into the database
        ingest_data_from_csv('D:\LoopAI assignment\data\store_status.csv', 'store_status')
        ingest_data_from_csv('D:\LoopAI assignment\data\store_business_hours.csv', 'store_business_hours')
        ingest_data_from_csv('D:\LoopAI assignment\data\store_timezones.csv', 'store_timezones')

        # Get store business hours
        get_store_business_hours('D:\LoopAI assignment\data\store_business_hours.csv')

        # Generate the report
        generate_report()

        # Return the report_id as the response
        return jsonify({"report_id": "some_random_string"})
    else:
        return jsonify({"message": "Method not allowed."}), 405

@app.route('/get_report', methods=['GET'])
def get_report():
    if request.method == 'GET':
        report_id = request.args.get('report_id')
        global report_data

        if report_data is None:
            return jsonify({"status": "Running"})
        else:
            return jsonify({"status": "Complete", "report": report_data})
    else:
        return jsonify({"message": "Method not allowed."}), 405

if __name__ == '__main__':
    app.run(debug=True)