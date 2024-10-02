import logging
import re
import os
from handlers.cisco_ise_failed_attempts_handler import handle_cisco_ise_failed_attempts
from handlers.cisco_ise_tacacs_accounting_handler import handle_cisco_ise_tacacs_accounting
from handlers.cisco_ise_passed_attempts_handler import handle_cisco_ise_passed_attempts

# Setup logging
log_directory = r'C:\Syslog'
os.makedirs(log_directory, exist_ok=True)
logging.basicConfig(filename=os.path.join(log_directory, 'cisco_ise_handler.log'), level=logging.DEBUG,
                    format='%(asctime)s - %(processName)s - %(threadName)s - %(levelname)s - %(message)s', filemode='a')

def handle_cisco_ise_syslog(ip, message):
    logging.debug(f"Processing Cisco ISE message from {ip}: {message[:100]}...")  # Log first 100 chars of message

    if 'TACACS+ Accounting request rejected' in message:
        logging.info(f"Ignoring rejected message from {ip}")
        return

    try:
        if 'CISE_Failed_Attempts' in message:
            handle_cisco_ise_failed_attempts(ip, message)
            logging.info(f"Processed Failed Attempt message from {ip}")

        elif 'CISE_TACACS_Accounting' in message:
            if 'TACACS+ Accounting with Command' in message and 'EEM:' not in message:
                handle_cisco_ise_tacacs_accounting(ip, message)
                logging.info(f"Processed TACACS Accounting message from {ip}")

        elif 'CISE_Passed_Authentications' in message and 'Command Auth' not in message and 'Protocol=Tacacs' not in message:
            handle_cisco_ise_passed_attempts(ip, message)
            logging.info(f"Processed Passed Authentication message from {ip}")

        else:
            out = re.search(r'(CISE[^\s]+)\s', message)
            logging.info(f"Unhandled message type from {ip}: {out.group(0) if out else 'Unknown'}")

    except Exception as error:
        logging.error(f"Error processing Cisco ISE syslog message from {ip}: {error}")
        logging.error(f"Message: {message[:500]}...")  # Log first 500 chars of the problematic message

def write_syslog_to_file(directory, filename, message):
    try:
        os.makedirs(directory, exist_ok=True)
        file_path = os.path.join(directory, filename)
        with open(file_path, 'a') as file:
            file.write(message + '\n')
    except Exception as e:
        logging.error(f"Error writing to file {file_path}: {e}")

# Remove the enable_file_logging parameter from the main function
# If file logging is needed, it should be handled separately in a thread-safe manner
