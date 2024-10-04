import logging
import re
import os
from handlers.cisco_ise_handler import handle_cisco_ise_syslog
from handlers.wlc_handler import handle_wlc_syslog
from message_combiner import chunKing

# Setup logging
log_directory = r'C:\Syslog'
os.makedirs(log_directory, exist_ok=True)
logging.basicConfig(filename=os.path.join(log_directory, 'handler_dispatcher.log'), level=logging.DEBUG,
                    format='%(asctime)s - %(processName)s - %(threadName)s - %(levelname)s - %(message)s', filemode='a')

SYSLOG_HANDLERS = {
    'cisco_ise': ['10.23.18.218', '10.23.18.219', '10.23.18.220', '10.23.18.221', '10.23.18.222', '10.23.18.223', 
                  '10.24.18.218', '10.24.18.219', '10.24.18.220', '10.24.18.221', '10.24.18.222', '10.24.18.223', 
                  '10.23.252.3'],
    'wlc': ['10.23.16.25', '10.23.20.130']
}

# Use a thread-local storage for message fragments
import threading
message_fragments = threading.local()

def handle_syslog(ip, message):
    logging.debug(f"Received message from {ip}: {message[:100]}...")  # Log first 100 chars of message

    for handler, ips in SYSLOG_HANDLERS.items():
        if ip in ips:
            if handler == 'cisco_ise':
                # Ensure message_fragments is initialized for this thread
                if not hasattr(message_fragments, 'fragments'):
                    message_fragments.fragments = {}
                
                complete_message = chunKing(ip, message_fragments.fragments, message)
                if complete_message:
                    handle_cisco_ise_syslog(ip, complete_message)
                else:
                    logging.debug(f"Incomplete message from {ip}, waiting for more fragments")
            elif handler == 'wlc':
                handle_wlc_syslog(ip, message)
            return

    logging.warning(f"Unhandled syslog source: {ip}")

def flush_message_fragments():
    """
    Flush any incomplete message fragments. This should be called periodically
    to prevent memory buildup from incomplete messages.
    """
    if hasattr(message_fragments, 'fragments'):
        current_time = time.time()
        for uid, data in list(message_fragments.fragments.items()):
            if current_time - data['timestamp'] > 60:  # 60 seconds timeout
                logging.warning(f"Flushing incomplete message {uid} from {data['ip']}")
                del message_fragments.fragments[uid]

# This function should be called periodically in your main loop
def maintenance():
    flush_message_fragments()
    # Add any other maintenance tasks here









import logging
import re
import os
from handlers.cisco_ise_handler import handle_cisco_ise_syslog
from handlers.wlc_handler import handle_wlc_syslog
from message_combiner import chunKing
from syslog_onboarding import onboard_new_message_type_existing_table, onboard_new_message_type_new_table, onboard_existing_message_new_table

# Setup logging
log_directory = r'C:\Syslog'
os.makedirs(log_directory, exist_ok=True)
logging.basicConfig(filename=os.path.join(log_directory, 'handler_dispatcher.log'), level=logging.DEBUG,
                    format='%(asctime)s - %(processName)s - %(threadName)s - %(levelname)s - %(message)s', filemode='a')

SYSLOG_HANDLERS = {
    'cisco_ise': ['10.23.18.218', '10.23.18.219', '10.23.18.220', '10.23.18.221', '10.23.18.222', '10.23.18.223', 
                  '10.24.18.218', '10.24.18.219', '10.24.18.220', '10.24.18.221', '10.24.18.222', '10.24.18.223', 
                  '10.23.252.3'],
    'wlc': ['10.23.16.25', '10.23.20.130']
}

# Use a thread-local storage for message fragments
import threading
message_fragments = threading.local()

# Dictionary to store onboarded message handlers
ONBOARDED_HANDLERS = {}

def handle_syslog(ip, message):
    logging.debug(f"Received message from {ip}: {message[:100]}...")  # Log first 100 chars of message

    for handler, ips in SYSLOG_HANDLERS.items():
        if ip in ips:
            if handler == 'cisco_ise':
                # Ensure message_fragments is initialized for this thread
                if not hasattr(message_fragments, 'fragments'):
                    message_fragments.fragments = {}
                
                complete_message = chunKing(ip, message_fragments.fragments, message)
                if complete_message:
                    handle_cisco_ise_syslog(ip, complete_message)
                else:
                    logging.debug(f"Incomplete message from {ip}, waiting for more fragments")
            elif handler == 'wlc':
                handle_wlc_handler(ip, message)
            return

    # Check for onboarded message types
    message_type = extract_message_type(message)
    if message_type in ONBOARDED_HANDLERS:
        ONBOARDED_HANDLERS[message_type](message)
        return

    logging.warning(f"Unhandled syslog source: {ip}")

def extract_message_type(message):
    # Implement logic to extract message type from the syslog message
    # This is a placeholder and should be adjusted based on your syslog message format
    match = re.search(r'TYPE=(\w+)', message)
    return match.group(1) if match else None

def onboard_new_message_type(message_type, table_name, field_mapping, new_table=False):
    if new_table:
        handler = onboard_new_message_type_new_table(message_type, table_name, field_mapping)
    else:
        handler = onboard_new_message_type_existing_table(message_type, table_name, field_mapping)
    ONBOARDED_HANDLERS[message_type] = handler
    logging.info(f"Onboarded new message type: {message_type} to table: {table_name}")

def onboard_existing_message_to_new_table(message_type, old_table, new_table, field_mapping):
    handler = onboard_existing_message_new_table(message_type, old_table, new_table, field_mapping)
    ONBOARDED_HANDLERS[message_type] = handler
    logging.info(f"Onboarded existing message type: {message_type} from {old_table} to new table: {new_table}")

# Example usage:
# onboard_new_message_type('NEW_TYPE', 'existing_table', {'Field1': 'column1', 'Field2': 'column2'})
# onboard_new_message_type('ANOTHER_NEW_TYPE', 'new_table', {'Field1': 'column1', 'Field2': 'column2'}, new_table=True)
# onboard_existing_message_to_new_table('EXISTING_TYPE', 'old_table', 'new_table', {'ExistingField1': 'new_column1', 'ExistingField2': 'new_column2'})

def flush_message_fragments():
    """
    Flush any incomplete message fragments. This should be called periodically
    to prevent memory buildup from incomplete messages.
    """
    if hasattr(message_fragments, 'fragments'):
        current_time = time.time()
        for uid, data in list(message_fragments.fragments.items()):
            if current_time - data['timestamp'] > 60:  # 60 seconds timeout
                logging.warning(f"Flushing incomplete message {uid} from {data['ip']}")
                del message_fragments.fragments[uid]

# This function should be called periodically in your main loop
def maintenance():
    flush_message_fragments()
    # Add any other maintenance tasks here
