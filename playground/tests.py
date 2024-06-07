import subprocess
import re
import pymysql

def measure_mariadb_performance():
    conn = pymysql.connect(
        host='your_host',
        user='your_user',
        password='your_password',
        database='your_database'
    )
    cursor = conn.cursor()



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
    measure_postresql_performance()
