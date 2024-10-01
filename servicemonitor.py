import psutil
import time
import os
import json
import subprocess

# Service details
SERVICE_NAME = "pythonservice.exe"
DESIRED_SERVICE_NAME = "PythonSyslogService"
SERVICE_PID = None

# File to store memory usage data
filename = "memory_usage_data.txt"
file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), filename)

# Function to find the correct PID based on service name using PowerShell
def find_pid_by_service_name_powershell(service_name):
    command = f"Get-WmiObject Win32_Service | Where-Object {{ $_.Name -eq '{service_name}' }} | Select-Object -ExpandProperty ProcessId"
    result = subprocess.run(["powershell", "-Command", command], capture_output=True, text=True)
    pid = result.stdout.strip()
    if pid.isdigit():
        return int(pid)
    return None

while True:
    if SERVICE_PID is None:
        SERVICE_PID = find_pid_by_service_name_powershell(DESIRED_SERVICE_NAME)
   
    if SERVICE_PID:
        try:
            # Get the process by PID
            process = psutil.Process(SERVICE_PID)
           
            # Check if the process name matches
            if process.name() == SERVICE_NAME:
                # Get the memory usage in bytes
                memory_usage = process.memory_info().rss
                # Get the current timestamp
                timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
               
                # Prepare the data entry
                data_entry = {
                    "timestamp": timestamp,
                    "memory_usage_bytes": memory_usage
                }
               
                # Write the data to the file
                with open(file_path, 'a') as file:
                    file.write(json.dumps(data_entry) + "\n")
                print(f"Logged memory usage: {memory_usage} bytes at {timestamp}")
            else:
                print(f"Process with PID {SERVICE_PID} is not {SERVICE_NAME}")
       
        except psutil.NoSuchProcess:
            print(f"No process found with PID {SERVICE_PID}")
            SERVICE_PID = None
    else:
        print("Desired process not found, retrying...")

    # Wait for a specified interval before the next check (e.g., 60 seconds)
    time.sleep(5)
