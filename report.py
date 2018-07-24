import json
import os
import psycopg2

from datetime import datetime, timedelta

import config

new_tables = [
    'coverage_journey_anticipations',
    'coverage_journeys',
    'coverage_journeys_departments',
    'coverage_journeys_duration',
    'coverage_journeys_networks_transfers',
    'coverage_journeys_requests_params',
    'coverage_journeys_transfers',
    'coverage_journeys_types',
    'coverage_lines',
    'coverage_modes',
    'coverage_networks',
    'coverage_start_end_networks',
    'coverage_stop_areas',
    'error_stats',
    'requests_calls',
    'token_stats',
]
connection_string = "host='{host}' port='{port}' dbname='{dbname}' user='{user}' password='{password}'". \
    format(dbname=config.db["dbname"], user=os.getenv('STAT_DATABASE_USER', config.db["user"]),
           password=os.getenv('STAT_DATABASE_PASSWORD', config.db["password"]), schema=config.db["schema"],
           host=os.getenv('STAT_DATABASE_HOST', config.db["host"]), port=config.db['port'])
connection = psycopg2.connect(connection_string)
cursor = connection.cursor()

all_tables_dates_not_done = {}
for table in new_tables:
    all_tables_dates_not_done[table] = []
    query = """
    select distinct(request_date ::date) as request_date
    from stat_compiled.{}
    where request_date >= '{}'
    order by request_date
    """.format(table, os.getenv('START_DATE_REPORT', '2018-01-01'))
    cursor.execute(query)
    all_dates = [value[0] for value in cursor.fetchall()]
    cursor_date = min(all_dates)
    today = datetime.now().date()
    while cursor_date < today:
        cursor_date = cursor_date + timedelta(days=1)
        if cursor_date not in all_dates:
            all_tables_dates_not_done[table].append(cursor_date.isoformat())
if not all_tables_dates_not_done:
    exit(0)
else:
    print(json.dumps(all_tables_dates_not_done))
    exit(1)
