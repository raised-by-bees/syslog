import logging
from datetime import datetime
import re
from database_utils import fta_inserter, fwa_inserter, fla_inserter

# Setup logging
logging.basicConfig(filename=r'C:\Syslog\cisco_ise_failed_attempts_handler.log', level=logging.DEBUG,
                    format='%(asctime)s - %(processName)s - %(threadName)s - %(levelname)s - %(message)s', filemode='a')

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

def handle_cisco_ise_failed_attempts(ip, message):
    log_data = parse_syslog_message(message)

    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    if 'Failed-Attempt: Authentication failed' in message and 'Protocol=Tacacs' in message:
        fta_inserter.add_to_batch([
            timestamp, ip, log_data.get('UserName'), log_data.get('Device IP Address'),
            log_data.get('RemoteAddress'), log_data.get('FailureReason'),
            log_data.get('NetworkDeviceName'), log_data.get('RequestLatency')
        ])
        logging.info(f"Added FTA record for {ip}")
    elif 'WLC' in log_data.get('NetworkDeviceName', '') and 'HO' in log_data.get('CalledStationID', ''):
        fwa_inserter.add_to_batch([
            timestamp, ip, log_data.get('UserName'), log_data.get('NASIPAddress'),
            log_data.get('CalledStationID'), log_data.get('FailureReason'),
            log_data.get('NetworkDeviceName')
        ])
        logging.info(f"Added FWA record for {ip}")
    elif '-' in log_data.get('NetworkDeviceName', ''):
        fla_inserter.add_to_batch([
            timestamp, ip, log_data.get('UserName'), log_data.get('NASIPAddress'),
            log_data.get('NasPortID'), log_data.get('FailureReason'),
            log_data.get('NetworkDeviceName')
        ])
        logging.info(f"Added FLA record for {ip}")
    else:
        logging.warning(f"Unhandled failed attempt message from {ip}")
