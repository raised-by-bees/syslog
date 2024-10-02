import logging
from datetime import datetime
import re
import os
from database_utils import tca_inserter

# Setup logging
log_directory = r'C:\Syslog'
os.makedirs(log_directory, exist_ok=True)
logging.basicConfig(filename=os.path.join(log_directory, 'cisco_ise_tacacs_accounting_handler.log'), level=logging.DEBUG,
                    format='%(asctime)s - %(processName)s - %(threadName)s - %(levelname)s - %(message)s', filemode='a')

def parse_syslog_message(message):
    data = {}
    patterns = {
        'Username': r'User=([^,]+)',
        'NetworkDeviceName': r'NetworkDeviceName=([^,]+)',
        'NetworkDeviceIP': r'Device IP Address=([^,]+)',
        'RemoteDevice': r'Remote-Address=([^,]+)',
        'CmdSet': r'CmdSet=\[ CmdAV=([^,]+) ]',
        'timestamp': r'\d* \d* (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d* \+\d{2}:\d{2})'
    }

    for key, pattern in patterns.items():
        match = re.search(pattern, message)
        if match:
            data[key] = match.group(1)
    
    if 'CmdSet' in data:
        data['CmdSet'] = data['CmdSet'].replace("CmdArgAV=", "")
    
    if 'timestamp' not in data:
        data['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    return data

def handle_cisco_ise_tacacs_accounting(ip, message):
    log_data = parse_syslog_message(message)
    
    # Log the parsed data for debugging
    logging.debug(f"Parsed TACACS accounting message: {log_data}")

    if 'terminal pager 0' not in log_data.get('CmdSet', ''):
        tca_inserter.add_to_batch([
            log_data.get('timestamp'),
            log_data.get('Username'),
            log_data.get('NetworkDeviceName'),
            log_data.get('NetworkDeviceIP'),
            log_data.get('RemoteDevice'),
            log_data.get('CmdSet'),
            ip
        ])
        logging.info(f"Added TACACS accounting record for {ip}")
    else:
        logging.debug(f"Ignored 'terminal pager 0' command from {ip}")

    logging.info("Processed TACACS accounting message")
