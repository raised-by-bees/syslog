import socket
import logging
import os
import win32serviceutil
import win32service
import win32event
from handler_dispatcher import handle_syslog
import multiprocessing
import queue
import time
from database_utils import flush_all_batches, log_batch_status, cleanup_connections
from message_combiner import chunKing

def setup_logging(process_name):
    log_directory = r'C:\Syslog'
    log_filename = f'syslogService_{process_name}.txt'
    os.makedirs(log_directory, exist_ok=True)
    logging.basicConfig(filename=os.path.join(log_directory, log_filename), level=logging.INFO,
                        format='%(asctime)s - %(processName)s - %(levelname)s - %(message)s', filemode='a')
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logging.getLogger('').addHandler(console)
    logging.info(f"Logging started for {process_name}")

def process_syslog_queue(message_queue, fragment_queue, processed_queue, is_running, flush_interval):
    setup_logging("Worker")
    last_flush_time = time.time()
    
    while is_running.value:
        try:
            addr, message = message_queue.get(timeout=1)
            if "CISE_" in message and any(x in message for x in ["1 1", "1 2", "2 2"]):
                fragment_queue.put((addr, message))
            else:
                handle_syslog(addr, message)
            
            # Check if it's time for a batch insert
            current_time = time.time()
            if current_time - last_flush_time >= flush_interval:
                last_flush_time = current_time
                try:
                    flush_all_batches()
                    log_batch_status()
                    logging.info(f"Batch insert completed at {time.strftime('%Y-%m-%d %H:%M:%S')}")
                except Exception as e:
                    logging.error(f"Error during batch insert: {e}")
                finally:
                    cleanup_connections()
        except queue.Empty:
            # No message in the queue, check if it's time for a flush
            current_time = time.time()
            if current_time - last_flush_time >= flush_interval:
                last_flush_time = current_time
                try:
                    flush_all_batches()
                    log_batch_status()
                    logging.info(f"Batch insert completed at {time.strftime('%Y-%m-%d %H:%M:%S')}")
                except Exception as e:
                    logging.error(f"Error during batch insert: {e}")
                finally:
                    cleanup_connections()
        except Exception as e:
            logging.error(f"Unexpected error in process_syslog_queue: {e}")
            time.sleep(1)

        # Process any stitched messages
        try:
            while True:
                addr, message = processed_queue.get_nowait()
                handle_syslog(addr, message)
        except queue.Empty:
            pass

def process_fragment_queue(fragment_queue, processed_queue, is_running):
    setup_logging("FragmentStitcher")
    message_fragments = {}
    
    while is_running.value:
        try:
            addr, message = fragment_queue.get(timeout=1)
            complete_message = chunKing(addr, message_fragments, message)
            if complete_message:
                processed_queue.put((addr, complete_message))
        except queue.Empty:
            pass
        except Exception as e:
            logging.error(f"Unexpected error in process_fragment_queue: {e}")
            time.sleep(1)

def monitor_queue_size(message_queue, fragment_queue, is_running, queue_monitoring_file):
    setup_logging("Monitor")
    while is_running.value:
        main_queue_size = message_queue.qsize()
        fragment_queue_size = fragment_queue.qsize()
        logging.info(f"Main Queue size: {main_queue_size}, Fragment Queue size: {fragment_queue_size}")
        try:
            with open(queue_monitoring_file, 'a') as f:
                f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Main Queue Size: {main_queue_size}, Fragment Queue Size: {fragment_queue_size}\n")
        except Exception as e:
            logging.error(f"Error writing to queue monitoring file: {e}")
        time.sleep(5)

class SyslogService(win32serviceutil.ServiceFramework):
    _svc_name_ = "PythonSyslogService"
    _svc_display_name_ = "Python Syslog Service for Windows"

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        self.is_running = multiprocessing.Value('b', True)
        self.message_queue = multiprocessing.Queue()
        self.fragment_queue = multiprocessing.Queue()
        self.processed_queue = multiprocessing.Queue()
        self.max_queue_size = 100000
        self.queue_monitoring_file = r"C:\Syslog\queue_size.txt"
        self.num_processes = 1  # Set to 1 as per your requirement
        self.flush_interval = 60
        self.processes = []

    def SvcDoRun(self):
        self.ReportServiceStatus(win32service.SERVICE_RUNNING)
        self.start_syslog_server()

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        self.is_running.value = False
        for process in self.processes:
            process.join(timeout=5)
            if process.is_alive():
                process.terminate()
        cleanup_connections()  # Ensure database connections are closed
        win32event.SetEvent(self.hWaitStop)

    def start_syslog_server(self):
        setup_logging("Main")

        # Start worker processes
        for _ in range(self.num_processes):
            p = multiprocessing.Process(target=process_syslog_queue, 
                                        args=(self.message_queue, self.fragment_queue, self.processed_queue, self.is_running, self.flush_interval))
            p.start()
            self.processes.append(p)

        # Start fragment stitching process
        fragment_process = multiprocessing.Process(target=process_fragment_queue,
                                                   args=(self.fragment_queue, self.processed_queue, self.is_running))
        fragment_process.start()
        self.processes.append(fragment_process)

        # Start monitoring process
        monitor_process = multiprocessing.Process(target=monitor_queue_size, 
                                                  args=(self.message_queue, self.fragment_queue, self.is_running, self.queue_monitoring_file))
        monitor_process.start()
        self.processes.append(monitor_process)

        IP = "10.23.252.4"
        PORT = 514

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind((IP, PORT))

        logging.info(f"Syslog server started on {IP}:{PORT}")

        while self.is_running.value:
            try:
                data, addr = sock.recvfrom(8192)
                if not data:
                    break
                message = data.decode()
                if self.message_queue.qsize() < self.max_queue_size:
                    self.message_queue.put((addr[0], message))
                else:
                    logging.warning(f"Message queue is full. Dropping message from {addr[0]}.")
            except Exception as e:
                logging.error(f"Error receiving syslog message: {e}")

        sock.close()

if __name__ == '__main__':
    multiprocessing.freeze_support()  # Necessary for PyInstaller
    win32serviceutil.HandleCommandLine(SyslogService)
