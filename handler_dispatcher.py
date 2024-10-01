import logging
import re
import os
import logging
from handlers.cisco_ise_handler import handle_cisco_ise_syslog
from handlers.wlc_handler import handle_wlc_syslog
from message_combiner import chunKing

SYSLOG_HANDLERS = {
    'cisco_ise': ['10.23.18.218', '10.23.18.219', '10.23.18.220', '10.23.18.221', '10.23.18.222', '10.23.18.223', '10.24.18.218', '10.24.18.219', '10.24.18.220', '10.24.18.221', '10.24.18.222', '10.24.18.223', '10.23.252.3'],
    'wlc': ['10.23.16.25', '10.23.20.130']
}

message_fragments = {}

def handle_syslog(ip, message, enable_file_logging=True):
    def write_syslog_to_file(directory, filename, message):
        if enable_file_logging:
            os.makedirs(directory, exist_ok=True)
            file_path = os.path.join(directory, filename)
            with open(file_path, 'a') as file:
                file.write(message + '\n')
    for handler, ips in SYSLOG_HANDLERS.items():
        if ip in ips:
            if handler == 'cisco_ise':
                if 'langles3' in message:
                    write_syslog_to_file(r"C:\Syslog\Syslog Examples\ISE\TACACS Accounting", "ISE_accounting.log", message)
                message = chunKing(ip, message_fragments, message)
                if message:
                    handle_cisco_ise_syslog(ip, message)
            elif handler == 'wlc':
                handle_wlc_syslog(ip, message)
            return
    logging.warning(f"Unhandled syslog source: {ip}")