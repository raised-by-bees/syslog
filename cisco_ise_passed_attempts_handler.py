import os
import re
import logging
from datetime import datetime
from database_utils import pwa_inserter, pla_inserter

# Setup logging
log_directory = r'C:\Syslog'
os.makedirs(log_directory, exist_ok=True)
logging.basicConfig(filename=os.path.join(log_directory, 'cisco_ise_passed_attempts_handler.log'), level=logging.DEBUG,
                    format='%(asctime)s - %(processName)s - %(threadName)s - %(levelname)s - %(message)s', filemode='a')

def parse_syslog_message(message):
    common_field_patterns = {
        'NAS-IP-Address': r'NAS-IP-Address=(.*?),[\s<]',
        'NAS-Port-Id': r'NAS-Port-Id=(.*?),[\s<]',
        'NetworkDeviceName': r'NetworkDeviceName=(.*?),[\s<]',
        'DeviceIP': r'Device IP Address=(.*?),[\s<]',
        'RequestLatency': r'RequestLatency=(.*?),[\s<]',
        'cisco-av-pair=method': r'cisco-av-pair=method=(.*?),[\s<]',
        'UserName': r'UserName=(.*?),[\s<]',
        'AuthenticationMethod': r'AuthenticationMethod=(.*?),[\s<]',
        'AuthenticationIdentityStore': r'[^=]AuthenticationIdentityStore=(.*?),[\s<]',
        'SelectedAccessService': r'SelectedAccessService=(.*?),[\s<]',
        'SelectedAuthorizationProfiles': r'SelectedAuthorizationProfiles=(.*?),[\s<]',
        'IdentityGroup': r'[^(Host)]IdentityGroup=Endpoint Identity Groups:(.*?),[\s<]',
        'SelectedAuthenticationIdentityStores': r'SelectedAuthenticationIdentityStores=(.*?),[\s<]',
        'AuthenticationStatus': r'AuthenticationStatus=(.*?),[\s<]',
        'NetworkDeviceGroups=Location#': r'NetworkDeviceGroups=Location#(.*?),[\s<]',
        'NetworkDeviceGroups=Device=': r'NetworkDeviceGroups=Device Type#(.*?),[\s<]',
        'NetworkDeviceGroups=Rollout=': r'NetworkDeviceGroups=Rollout Stage#(.*?),[\s<]',
        'NetworkDeviceGroups=Reauth=': r'NetworkDeviceGroups=Reauth Controller#(.*?),[\s<]',
        'NetworkDeviceGroups=Closed=': r'NetworkDeviceGroups=Closed Mode#(.*?),[\s<]',
        'IdentityPolicyMatchedRule': r'IdentityPolicyMatchedRule=(.*?),[\s<]',
        'AuthorizationPolicyMatchedRule': r'AuthorizationPolicyMatchedRule=(.*?),[\s<]',
        'Subject - Common Name': r'Subject - Common Name=(.*?),[\s<]',
        'EndPointMACAddress': r'EndPointMACAddress=(.*?),[\s<]',
        'ISEPolicySetName': r'ISEPolicySetName=(.*?),[\s<]',
        'AD-Host-Resolved-DNs': r'AD-Host-Resolved-DNs=(.*?),[\s<]',
        'Days to Expiry': r'Days to Expiry=(.*?),[\s<]',
        'Session-Timeout': r'Session-Timeout=(.*?);[\s<]',
        'cisco-av-pair=ACS': r'cisco-av-pair=ACS:(.*?);[\s<]'
    }

    wlc_field_patterns = {
        'Called-Station-ID': r'Called-Station-ID=([^,:]+)',
        'RadiusFlowType': r'RadiusFlowType=(.*?),[\s<]'
    }

    extracted_fields = {}
    for key, pattern in common_field_patterns.items():
        matches = re.findall(pattern, message)
        if matches:
            if len(matches) > 1:
                try:
                    if key == 'UserName':
                        matches = list(map(lambda x: x.replace("-", "").lower(), matches))
                    uniqueMatches = list(set(matches))
                    if len(uniqueMatches) > 1:
                        extracted_fields[key] = ", ".join(uniqueMatches)
                    else:
                        extracted_fields[key] = uniqueMatches[0]
                except Exception as error:
                    logging.error(f"Error processing matches for {key}: {error}")
            else:
                extracted_fields[key] = matches[0]

    if 'NetworkDeviceName' in extracted_fields and 'WLC' in extracted_fields['NetworkDeviceName']:
        for key, pattern in wlc_field_patterns.items():
            matches = re.findall(pattern, message)
            if matches:
                extracted_fields[key] = matches[0]

    timestamp = re.search(r'\d* \d* (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d* \+\d{2}:\d{2})', message)
    extracted_fields['timestamp'] = timestamp.group(1) if timestamp else datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    return extracted_fields

def handle_cisco_ise_passed_attempts(ip, message):
    extracted_fields = parse_syslog_message(message)
    extracted_fields['source_ip'] = ip

    if 'NetworkDeviceName' in extracted_fields and 'WLC' in extracted_fields['NetworkDeviceName']:
        pwa_inserter.add_to_batch([
            extracted_fields.get('timestamp'),
            extracted_fields.get('source_ip'),
            extracted_fields.get('NAS-IP-Address'),
            extracted_fields.get('NetworkDeviceName'),
            extracted_fields.get('RequestLatency'),
            extracted_fields.get('cisco-av-pair=method'),
            extracted_fields.get('UserName'),
            extracted_fields.get('AuthenticationMethod'),
            extracted_fields.get('AuthenticationIdentityStore'),
            extracted_fields.get('SelectedAccessService'),
            extracted_fields.get('SelectedAuthorizationProfiles'),
            extracted_fields.get('IdentityGroup'),
            extracted_fields.get('SelectedAuthenticationIdentityStores'),
            extracted_fields.get('AuthenticationStatus'),
            extracted_fields.get('NetworkDeviceGroups=Location#'),
            extracted_fields.get('NetworkDeviceGroups=Device='),
            extracted_fields.get('NetworkDeviceGroups=Rollout='),
            extracted_fields.get('NetworkDeviceGroups=Reauth='),
            extracted_fields.get('NetworkDeviceGroups=Closed='),
            extracted_fields.get('IdentityPolicyMatchedRule'),
            extracted_fields.get('AuthorizationPolicyMatchedRule'),
            extracted_fields.get('Subject - Common Name'),
            extracted_fields.get('EndPointMACAddress'),
            extracted_fields.get('ISEPolicySetName'),
            extracted_fields.get('AD-Host-Resolved-DNs'),
            extracted_fields.get('Days to Expiry'),
            extracted_fields.get('Session-Timeout'),
            extracted_fields.get('cisco-av-pair=ACS'),
            extracted_fields.get('DeviceIP'),
            extracted_fields.get('Called-Station-ID'),
            extracted_fields.get('RadiusFlowType')
        ])
        logging.info(f"Added PWA record for {ip}")
    elif 'NetworkDeviceGroups=Device=' in extracted_fields and 'switch' in extracted_fields['NetworkDeviceGroups=Device=']:
        pla_inserter.add_to_batch([
            extracted_fields.get('timestamp'),
            extracted_fields.get('source_ip'),
            extracted_fields.get('NAS-IP-Address'),
            extracted_fields.get('NAS-Port-Id'),
            extracted_fields.get('NetworkDeviceName'),
            extracted_fields.get('RequestLatency'),
            extracted_fields.get('cisco-av-pair=method'),
            extracted_fields.get('UserName'),
            extracted_fields.get('AuthenticationMethod'),
            extracted_fields.get('AuthenticationIdentityStore'),
            extracted_fields.get('SelectedAccessService'),
            extracted_fields.get('SelectedAuthorizationProfiles'),
            extracted_fields.get('IdentityGroup'),
            extracted_fields.get('SelectedAuthenticationIdentityStores'),
            extracted_fields.get('AuthenticationStatus'),
            extracted_fields.get('NetworkDeviceGroups=Location#'),
            extracted_fields.get('NetworkDeviceGroups=Device='),
            extracted_fields.get('NetworkDeviceGroups=Rollout='),
            extracted_fields.get('NetworkDeviceGroups=Reauth='),
            extracted_fields.get('NetworkDeviceGroups=Closed='),
            extracted_fields.get('IdentityPolicyMatchedRule'),
            extracted_fields.get('AuthorizationPolicyMatchedRule'),
            extracted_fields.get('Subject - Common Name'),
            extracted_fields.get('EndPointMACAddress'),
            extracted_fields.get('ISEPolicySetName'),
            extracted_fields.get('AD-Host-Resolved-DNs'),
            extracted_fields.get('Days to Expiry'),
            extracted_fields.get('Session-Timeout'),
            extracted_fields.get('cisco-av-pair=ACS'),
            extracted_fields.get('DeviceIP')
        ])
        logging.info(f"Added PLA record for {ip}")
    else:
        logging.warning(f"Unhandled passed attempt message from {ip}")

    logging.debug(f"Processed passed attempt message from {ip}")
