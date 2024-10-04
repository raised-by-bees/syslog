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
from database_utils import flush_all_batches, log_batch_status, cleanup_connections, get_total_batch_size
import signal

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

def process_syslog_queue(message_queue, is_running, flush_interval, counters):
    setup_logging("Worker")
    last_flush_time = time.time()
    
    while is_running.value:
        try:
            addr, message = message_queue.get(timeout=1)
            with counters['handled'].get_lock():
                counters['handled'].value += 1
            handle_syslog(addr, message)
            with counters['ready_for_insertion'].get_lock():
                counters['ready_for_insertion'].value += 1
            
            current_time = time.time()
            if current_time - last_flush_time >= flush_interval:
                last_flush_time = current_time
                try:
                    flush_all_batches()
                    log_batch_status()
                    log_counter_status(counters)
                    logging.info(f"Batch insert completed at {time.strftime('%Y-%m-%d %H:%M:%S')}")
                except Exception as e:
                    logging.error(f"Error during batch insert: {e}")
                finally:
                    cleanup_connections()
        except queue.Empty:
            current_time = time.time()
            if current_time - last_flush_time >= flush_interval:
                last_flush_time = current_time
                try:
                    flush_all_batches()
                    log_batch_status()
                    log_counter_status(counters)
                    logging.info(f"Batch insert completed at {time.strftime('%Y-%m-%d %H:%M:%S')}")
                except Exception as e:
                    logging.error(f"Error during batch insert: {e}")
                finally:
                    cleanup_connections()
        except Exception as e:
            logging.error(f"Unexpected error in process_syslog_queue: {e}")
            time.sleep(1)

def monitor_queue_size(message_queue, is_running, queue_monitoring_file, counters):
    setup_logging("Monitor")
    while is_running.value:
        queue_size = message_queue.qsize()
        total_batch_size = get_total_batch_size()
        logging.info(f"Queue size: {queue_size}, Total batch size: {total_batch_size}")
        try:
            with open(queue_monitoring_file, 'a') as f:
                f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Queue Size: {queue_size}, Total Batch Size: {total_batch_size}\n")
            log_counter_status(counters)
        except Exception as e:
            logging.error(f"Error writing to queue monitoring file: {e}")
        time.sleep(5)

def log_counter_status(counters):
    logging.info(f"Messages received: {counters['received'].value}")
    logging.info(f"Messages handled: {counters['handled'].value}")
    logging.info(f"Messages ready for insertion: {counters['ready_for_insertion'].value}")

class SyslogService(win32serviceutil.ServiceFramework):
    _svc_name_ = "PythonSyslogService"
    _svc_display_name_ = "Python Syslog Service for Windows"

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        self.is_running = multiprocessing.Value('b', True)
        self.message_queue = multiprocessing.Queue()
        self.max_queue_size = 100000
        self.queue_monitoring_file = r"C:\Syslog\queue_size.txt"
        self.num_processes = 1  # Set to 1 as per your requirement
        self.flush_interval = 60
        self.processes = []
        
        # Initialize shared counters
        self.manager = multiprocessing.Manager()
        self.counters = {
            'received': multiprocessing.Value('i', 0),
            'handled': multiprocessing.Value('i', 0),
            'ready_for_insertion': multiprocessing.Value('i', 0)
        }

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
                                        args=(self.message_queue, self.is_running, self.flush_interval, self.counters))
            p.start()
            self.processes.append(p)

        # Start monitoring process
        monitor_process = multiprocessing.Process(target=monitor_queue_size, 
                                                  args=(self.message_queue, self.is_running, self.queue_monitoring_file, self.counters))
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
                with self.counters['received'].get_lock():
                    self.counters['received'].value += 1
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




def get_total_batch_size():
    return sum(inserter.get_batch_size() for inserter in [fta_inserter, fwa_inserter, fla_inserter, 
                                                          pwa_inserter, pla_inserter, tca_inserter])
