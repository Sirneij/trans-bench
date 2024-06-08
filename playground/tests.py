import csv
import os
import re
import subprocess
from pathlib import Path

import duckdb
import pymysql


def estimate_time_duration(t1: tuple, t2: tuple) -> tuple[float, float]:
    u1, s1, cu1, cs1, elapsed1 = t1
    u2, s2, cu2, cs2, elapsed2 = t2
    return elapsed2 - elapsed1, u2 - u1 + s2 - s1 + cu2 - cu1 + cs2 - cs1


def measure_duckdb_performance():
    # Create or connect to DuckDB database (in-memory or file-based)
    conn = duckdb.connect(database=':memory:')  # Use ':memory:' for in-memory database

    data_file = 'edge.facts'
    output_file = Path(__file__).parent / 'test_duckdb.csv'

    # Ensure output directory exists
    output_dir = output_file.parent
    output_dir.mkdir(parents=True, exist_ok=True)

    # Create the SQL commands
    sql_commands = [
        "CREATE TABLE tc_path (x INTEGER, y INTEGER);",
        f"COPY tc_path FROM '{data_file}' (DELIMITER '\t');",
        "CREATE INDEX tc_path_yx ON tc_path (y, x);",
        """
        CREATE TABLE tc_result AS
        WITH RECURSIVE tc AS (
            SELECT x, y
            FROM tc_path
            UNION
            SELECT tc_path.x, tc.y
            FROM tc_path, tc
            WHERE tc_path.y = tc.x
        )
        SELECT * FROM tc;
        """,
        f"COPY (SELECT * FROM tc_result) TO '{output_file}' WITH (HEADER, DELIMITER ',');",
    ]

    # Execute each command and measure the time taken
    for command in sql_commands:
        start_time = os.times()
        try:
            conn.execute(command)
        except Exception as e:
            print(f"Error executing command: {command}")
            print(e)
        end_time = os.times()
        real_time, cpu_time = estimate_time_duration(start_time, end_time)
        print(
            f"Executed: {command.split()[0].capitalize()}{command.split()[1].capitalize()} in {real_time}, {cpu_time} seconds"
        )

    # Close the connection
    conn.close()


def measure_mariadb_performance():
    conn = pymysql.connect(
        host='localhost',
        user='root',
        password='sirneij',
        database='sirneij',
        local_infile=True,  # Enable local infile
    )
    cursor = conn.cursor()

    data_file = 'edge.facts'
    output_file = Path(__file__).parent / 'test_mariadb.csv'
    timing_file = Path(__file__).parent / 'test_timing_mariadb.csv'

    # Ensure output directory exists
    output_dir = output_file.parent
    output_dir.mkdir(parents=True, exist_ok=True)

    # Read the SQL script and substitute the placeholder with the actual file path
    with open('test_mariadb.sql', 'r') as f:
        sql_script = f.read()

    sql_script = sql_script.replace('{data_file}', data_file)

    # Split the script into individual commands
    sql_commands = [
        f'{command.strip()};' for command in sql_script.split(';') if command.strip()
    ]

    # Headers for the CSV
    headers = [
        'CreateTableRealTime',
        'CreateTableCPUTime',
        'LoadDataRealTime',
        'LoadDataCPUTime',
        'CreateIndexRealTime',
        'CreateIndexCPUTime',
        'AnalyzeRealTime',
        'AnalyzeCPUTime',
        'ExecuteQueryRealTime',
        'ExecuteQueryCPUTime',
        'WriteResultRealTime',
        'WriteResultCPUTime',
    ]

    # Dictionary to hold the timing results
    timing_results = {header: 0 for header in headers}

    # Execute each command and measure the time taken
    for i, command in enumerate(sql_commands):
        start_time = os.times()
        try:
            cursor.execute(command)
            conn.commit()
        except pymysql.MySQLError as e:
            print(f"Error executing command: {command}")
            print(e)
        end_time = os.times()
        real_time, cpu_time = estimate_time_duration(start_time, end_time)
        timing_results[headers[2 * i]] = real_time
        timing_results[headers[2 * i + 1]] = cpu_time

    # Close the cursor and the connection
    cursor.close()
    conn.close()

    # Write timing results to CSV file
    with open(timing_file, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(headers)
        csv_writer.writerow([timing_results[header] for header in headers])

    password = 'sirneij'
    command = ['sudo', '-S', 'mv', '/tmp/test_mariadb.csv', output_file]
    # Run the command and pass the password
    subprocess.run(
        command, input=f'{password}\n', text=True, capture_output=True
    )


def measure_postresql_performance():
    # Paths to the data file and output file
    data_file = 'edge.facts'
    output_file = 'test_pg.csv'

    # Read the SQL script and substitute the placeholders
    with open('test_pg.sql', 'r') as file:
        sql_script = file.read()

    # Substitute the placeholders with actual file paths
    sql_script = sql_script.replace('{data_file}', data_file)
    sql_script = sql_script.replace('{output_file}', output_file)

    # Write the modified script to a temporary file
    with open('test_pg_temp.sql', 'w') as file:
        file.write(sql_script)

    # Execute the script using psql
    command = ['psql', '-f', 'test_pg_temp.sql']

    result = subprocess.run(command, capture_output=True, text=True)

    # Print the result to the console (optional)
    print(result.stdout)
    print(result.stderr)

    # Parse the timing information
    timings = parse_timings(result.stdout)

    print(timings)

    # Print the parsed timings in seconds
    # for step, time in timings.items():
    #     print(f'{step}: {time} seconds')


def parse_timings(output):
    # Regular expression to match timing lines
    timing_regex = re.compile(r'Time:\s+([\d.]+)\s+ms')

    # Define the steps in the order they appear
    steps = [
        'CreateTable',
        'LoadData',
        'CreateIndex',
        'Analyze',
        'ExecuteQuery',
        'WriteResult',
    ]

    # Find all timing values in the output
    times = timing_regex.findall(output)

    # Convert times from milliseconds to seconds
    times_in_seconds = [float(time) / 1000 for time in times]

    # Map steps to their corresponding timing values
    timings = dict(zip(steps, times_in_seconds))

    return timings


if __name__ == '__main__':
    # measure_postresql_performance()
    measure_mariadb_performance()
    # measure_duckdb_performance()
