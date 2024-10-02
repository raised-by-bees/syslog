import socket
import logging
import os
import win32serviceutil
import win32service
import win32event
from handler_dispatcher import handle_syslog
import threading
import queue
import time
import concurrent.futures
from database_utils import flush_all_batches, log_batch_status

class SyslogService(win32serviceutil.ServiceFramework):
    _svc_name_ = "PythonSyslogService"
    _svc_display_name_ = "Python Syslog Service for Windows"

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        self.sock = None
        self.is_running = True
        self.setup_logging()
        self.message_queue = queue.Queue()
        self.max_queue_size = 50000  # Increased max queue size
        self.queue_monitoring_file = r"C:\Syslog\queue_size.txt"
        self.thread_monitoring_file = r"C:\Syslog\thread_count.txt"
        self.min_thread_count = 200  # Increased minimum thread count
        self.max_thread_count = 500  # Increased maximum thread count
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.max_thread_count)
        self.flush_interval = 15  # Reduced flush interval to 15 seconds
        self.last_logged_size = 0
        self.last_flush_time = time.time()

    def setup_logging(self):
        log_directory = r'C:\Syslog'
        log_filename = 'syslogService.txt'
        os.makedirs(log_directory, exist_ok=True)
        logging.basicConfig(filename=os.path.join(log_directory, log_filename), level=logging.DEBUG,
                            format='%(asctime)s - %(levelname)s - %(message)s', filemode='a')
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        logging.getLogger('').addHandler(console)
        logging.info("Logging started")

    def SvcDoRun(self):
        self.ReportServiceStatus(win32service.SERVICE_RUNNING)
        self.start_syslog_server()

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        self.is_running = False
        if self.sock:
            self.sock.close()
        self.executor.shutdown(wait=True)
        win32event.SetEvent(self.hWaitStop)

    def start_syslog_server(self):
        def process_syslog_queue():
            while self.is_running:
                try:
                    addr, message = self.message_queue.get(timeout=1)
                    logging.debug(f"Processing message from {addr}")
                    try:
                        handle_syslog(addr, message)
                    except Exception as e:
                        logging.error(f"Error processing syslog message: {e}")
                    finally:
                        self.message_queue.task_done()
                        
                    # Check if it's time for a batch insert
                    current_time = time.time()
                    if current_time - self.last_flush_time >= self.flush_interval:
                        self.last_flush_time = current_time
                        self.executor.submit(self.perform_batch_insert)
                except queue.Empty:
                    continue
                except Exception as e:
                    logging.error(f"Unexpected error in process_syslog_queue: {e}")
                    time.sleep(1)

        def monitor_queue_size():
            while self.is_running:
                active_threads = len([t for t in threading.enumerate() if t.is_alive() and t != threading.current_thread()])
                queue_size = self.message_queue.qsize()
                
                logging.info(f"Active threads: {active_threads}, Queue size: {queue_size}")
                
                self.write_to_file(self.thread_monitoring_file, f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Thread Count: {active_threads}")
                self.write_to_file(self.queue_monitoring_file, f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Queue Size: {queue_size}")
                
                if active_threads < self.min_thread_count:
                    threads_to_start = min(self.min_thread_count - active_threads, self.max_thread_count - active_threads)
                    logging.warning(f"Starting {threads_to_start} new threads")
                    for _ in range(threads_to_start):
                        self.executor.submit(process_syslog_queue)
                
                time.sleep(10)

        def perform_batch_insert():
            try:
                flush_all_batches()
                log_batch_status()
                logging.info(f"Batch insert completed at {time.strftime('%Y-%m-%d %H:%M:%S')}")
            except Exception as e:
                logging.error(f"Error during batch insert: {e}")

        IP = "10.23.252.4"
        PORT = 514

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((IP, PORT))

        logging.info(f"Syslog server started on {IP}:{PORT}")

        for _ in range(self.min_thread_count):
            self.executor.submit(process_syslog_queue)
        
        threading.Thread(target=monitor_queue_size, daemon=True).start()

        while self.is_running:
            try:
                data, addr = self.sock.recvfrom(8192)
                if not data:
                    break
                message = data.decode()
                if self.message_queue.qsize() < self.max_queue_size:
                    self.message_queue.put((addr[0], message))
                else:
                    logging.warning(f"Message queue is full. Dropping message from {addr[0]}.")
            except Exception as e:
                logging.error(f"Error receiving syslog message: {e}")

    def write_to_file(self, file_path, content):
        try:
            with open(file_path, 'a') as f:
                f.write(content + '\n')
        except Exception as e:
            logging.error(f"Error writing to file {file_path}: {e}")

if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(SyslogService)
