import psycopg2
import psycopg2.extras
from psycopg2 import sql
import logging
from datetime import datetime
import re

DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/ciscoise"

def handle_cisco_ise_failed_attempts(ip, message):
    log_data = parse_syslog_message(message)

    # Determine the table name based on conditions
    if 'Failed-Attempt: Authentication failed' in message and 'Protocol=Tacacs' in message:
        table_name = 'fta'
        fields = ('timestamp', 'ipaddress', 'username', 'nasipaddress', 'remoteaddress', 'failurereason', 'networkdevicename', 'requestlatency')
        row_data = [datetime.now().strftime('%Y-%m-%d %H:%M:%S'), ip, log_data.get('UserName'), log_data.get('Device IP Address'),
                    log_data.get('RemoteAddress'), log_data.get('FailureReason'), log_data.get('NetworkDeviceName'), log_data.get('RequestLatency')]
    elif 'WLC' in log_data.get('NetworkDeviceName', '') and 'HO' in log_data.get('CalledStationID', ''):
        table_name = 'fwa'
        fields = ('timestamp', 'ipaddress', 'username', 'nasipaddress', 'calledstationid', 'failurereason', 'networkdevicename')
        row_data = [datetime.now().strftime('%Y-%m-%d %H:%M:%S'), ip, log_data.get('UserName'), log_data.get('NASIPAddress'),
                    log_data.get('CalledStationID'), log_data.get('FailureReason'), log_data.get('NetworkDeviceName')]
    elif '-' in log_data.get('NetworkDeviceName', ''):
        table_name = 'fla'
        fields = ('timestamp', 'ipaddress', 'username', 'nasipaddress', 'nasportid', 'failurereason', 'networkdevicename')
        row_data = [datetime.now().strftime('%Y-%m-%d %H:%M:%S'), ip, log_data.get('UserName'), log_data.get('NASIPAddress'),
                    log_data.get('NasPortID'), log_data.get('FailureReason'), log_data.get('NetworkDeviceName')]
    else:
        return  # Exit if neither condition is met

    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()

    # Create the SQL insert statement dynamically
    insert_stmt = sql.SQL('INSERT INTO {} ({}) VALUES %s').format(
        sql.Identifier(table_name),
        sql.SQL(', ').join(map(sql.Identifier, fields))
    )

    try:
        psycopg2.extras.execute_values(cursor, insert_stmt, [tuple(row_data)])
        conn.commit()
    except Exception as error:
        logging.error(f"Error inserting data into PostgreSQL: {error}")
    finally:
        cursor.close()
        conn.close()

def parse_syslog_message(message):
    field_patterns = {
        'UserName': r'UserName=([^,]+)',
        'NASIPAddress': r'NAS-IP-Address=([^,]+)',
        'CalledStationID': r'Called-Station-ID=([^,:]+)',
        'NasPortID': r'NAS-Port-Id=([^\s,]+)',
        'FailureReason': r'FailureReason=([^,]+)',
        'NetworkDeviceName': r'NetworkDeviceName=([^,]+)',
        'RemoteAddress': r'Remote-Address=([^,]+)',
        'RequestLatency': r'RequestLatency=([^,]+)',
        'Device IP Address': r'Device IP Address=([^,]+)',
    }

    data = {}
    for key, pattern in field_patterns.items():
        match = re.search(pattern, message)
        if match:
            data[key] = match.group(1)

    return data
