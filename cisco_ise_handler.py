import logging
import re
import os

from handlers.cisco_ise_failed_attempts_handler import handle_cisco_ise_failed_attempts
from handlers.cisco_ise_tacacs_accounting_handler import handle_cisco_ise_tacacs_accounting
from handlers.cisco_ise_passed_attempts_handler import handle_cisco_ise_passed_attempts

def handle_cisco_ise_syslog(ip, message, enable_file_logging=True):
    def write_syslog_to_file(directory, filename, message):
        if enable_file_logging:
            os.makedirs(directory, exist_ok=True)
            file_path = os.path.join(directory, filename)
            with open(file_path, 'a') as file:
                file.write(message + '\n')

    if 'TACACS+ Accounting request rejected' in message:
        logging.info(f"Ignoring rejected message from {ip}")
        return

    if 'CISE_Failed_Attempts' in message:
        handle_cisco_ise_failed_attempts(ip, message)
        # if 'langles3' in message:
        #     write_syslog_to_file(r"C:\Syslog\Syslog Examples\ISE\TACACS Accounting", "failed_attempts.log", message)

    elif ('CISE_TACACS_Accounting' in message):# or ('CISE_Passed_Authentications' in message and 'CmdArgAV' in message):
        if 'TACACS+ Accounting with Command' in message and 'EEM:' not in message:
            handle_cisco_ise_tacacs_accounting(ip, message)
            # if 'langles3' in message:
            #     write_syslog_to_file(r"C:\Syslog\Syslog Examples\ISE\TACACS Accounting", "with_command_accounting.log", message)

        if 'CISE_Passed_Authentications' in message:
            #handle_cisco_ise_tacacs_accounting(ip, message)
            if 'langles3' in message:
                write_syslog_to_file(r"C:\Syslog\Syslog Examples\ISE\TACACS Accounting", "new_passed_accounting.log", message)
        #logging.info(f"TACACS Accounting message from {ip}")
        #write_syslog_to_file(r"C:\Syslog\Syslog Examples\ISE\TACACS Accounting", "new_tacacs_accounting.log", message)

    elif 'CISE_Passed_Authentications' in message and message.find("Command Auth") == -1 and message.find("Protocol=Tacacs") == -1:
        handle_cisco_ise_passed_attempts(ip, message)
        # if 'langles3' in message:
        #     write_syslog_to_file(r"C:\Syslog\Syslog Examples\ISE\TACACS Accounting", "passed_auth.log", message)
    else:
        try:
            out = re.search(r'(CISE[^\s]+)\s', message)
            logging.info(f"Unhandled message type from {ip}: {out.group(0) if out else 'Unknown'}")
            # if 'langles3' in message:
            #     write_syslog_to_file(r"C:\Syslog\Syslog Examples\ISE\Unhandled", "unhandled_messages.log", message)
        except Exception as error:
            logging.error(f"Error processing syslog message: {error}")
        # logging.warning(f"Unhandled Cisco ISE syslog message type from {ip} {out}")
