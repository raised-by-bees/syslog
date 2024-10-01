import os
import re
import logging
from datetime import datetime
from database_utils import pwa_inserter, pla_inserter

def handle_cisco_ise_passed_attempts(ip, message):
    output_dir = 'c:/syslog'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
   
    file_path = os.path.join(output_dir, 'passed_not_WLC_not_switch.txt')
   
    # Define the fields and their regex patterns
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

    # Additional fields for specific conditions
    wlc_field_patterns = {
        'Called-Station-ID': r'Called-Station-ID=([^,:]+)',
        'RadiusFlowType': r'RadiusFlowType=(.*?),[\s<]'
    }

    switch_field_patterns = {}

    #first things first
    extracted_fields = {}
    extracted_fields['source_ip'] = ip #add source ip
    timestamp = re.search(r'\d* \d* (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d* \+\d{2}:\d{2})', message) #pull timestamp from message, if not present in this format create timestamp with current time
    if timestamp:
        extracted_fields['timestamp'] = timestamp.group(1)
    else:
        extracted_fields['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Extract the fields using regex
    for key, pattern in common_field_patterns.items():
        matches = re.findall(pattern, message)
        if matches:
            # Handle the case where there might be multiple values
            if len(matches) > 1:
                try:
                    if key=='UserName': #username field
                        matches= list(map(lambda x: x.replace("-","").lower(),matches)) #replace - and set to lower case
                    uniqueMatches=list(set(matches)) #change list to set to remove duplicates

                    if len(uniqueMatches) > 1:
                        extracted_fields[key] = ", ".join(uniqueMatches)
                    else:
                        extracted_fields[key] = uniqueMatches[0]
                except Exception as error:
                    logging.error(f"change matches to a set: {error}")
            else:
                extracted_fields[key] = matches[0]

    # Check for WLC specific fields
    if 'NetworkDeviceName' in extracted_fields and 'WLC' in extracted_fields['NetworkDeviceName']:
        for key, pattern in wlc_field_patterns.items():
            matches = re.findall(pattern, message)
            if matches:
                extracted_fields[key] = matches[0]

        row_data = [
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
        ]
        pwa_inserter.add_to_batch(tuple(row_data))

    # Check for switch specific fields
    elif 'NetworkDeviceGroups=Device=' in extracted_fields and 'switch' in extracted_fields['NetworkDeviceGroups=Device=']:
        for key, pattern in switch_field_patterns.items():
            matches = re.findall(pattern, message)
            if matches:
                extracted_fields[key] = matches[0]

        row_data = [
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
        ]
        pla_inserter.add_to_batch(tuple(row_data))

    else:
        # Write the extracted fields to the file
        try:
            with open(file_path, 'a') as file:
                file.write(message)  # Separate entries by a new line
        except Exception as error:
            logging.error(f"Error writing syslog to file: {error}")
