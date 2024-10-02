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
