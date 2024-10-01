import psycopg2
import psycopg2.extras
from psycopg2 import sql
import logging
from datetime import datetime
import re
import os

DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/ciscoise"

def parse_syslog_message(message):
    data = {}
    user_name_search = re.search(r'User=([^,]+)', message)
    if user_name_search:
        data['Username'] = user_name_search.group(1)

    device_search = re.search(r'NetworkDeviceName=([^,]+)', message)
    if device_search:
        data['NetworkDeviceName'] = device_search.group(1)

    device_ip_search = re.search(r'Device IP Address=([^,]+)', message)
    if device_ip_search:
        data['NetworkDeviceIP'] = device_ip_search.group(1)


    RemoteAddr_search = re.search(r'Remote-Address=([^,]+)', message)
    if RemoteAddr_search:
        data['RemoteDevice'] = RemoteAddr_search.group(1)

    commands = re.search(r'CmdSet=\[ CmdAV=([^,]+) ]', message)
    if commands:
        data['CmdSet'] = commands.group(1).replace("CmdArgAV=", "",)
    
    timestamp = re.search(r'\d* \d* (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d* \+\d{2}:\d{2})', message)
    if timestamp:
        data['timestamp'] = timestamp.group(1)
    else:
        data['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    return data

def handle_cisco_ise_tacacs_accounting(ip, message):
    log_data = parse_syslog_message(message)
    log_directory = r'C:\\Syslog\\syslog2.0'
    log_filename = 'tacacs_accounting.txt'
    with open(log_directory + '\\' + log_filename, 'a') as file:
        file.write(str(log_data) + '\n')
    logging.info("Logged TACACS accounting message")

    if 'terminal pager 0' not in log_data.get('CmdSet', ''):
        table_name = 'tca'

        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()

        insert_stmt = sql.SQL('INSERT INTO {} (timestamp, username, networkdevicename, networkdeviceip, remotedevice, cmdset, ipaddress) VALUES %s').format(sql.Identifier(table_name)) #add ipaddress

        # timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        row_data = [log_data.get('timestamp'), log_data.get('Username'), 
                    log_data.get('NetworkDeviceName'), log_data.get('NetworkDeviceIP'), 
                    log_data.get('RemoteDevice'), log_data.get('CmdSet'), ip]

        try:
            psycopg2.extras.execute_values(cursor, insert_stmt, [tuple(row_data)])
            conn.commit()
        except Exception as error:
            logging.error(f"Error inserting data into PostgreSQL: {error}")
        finally:
            cursor.close()
            conn.close()
