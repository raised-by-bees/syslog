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
from database_utils import flush_all_batches

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
        self.max_queue_size = 10000
        self.queue_monitoring_file = r"C:\Syslog\queue_size.txt"
        self.thread_monitoring_file = r"C:\Syslog\thread_count.txt"
        self.min_thread_count = 152
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.min_thread_count)
        self.flush_interval = 300  # 5 minutes

    def setup_logging(self):
        log_directory = r'C:\\Syslog'
        log_filename = 'syslogService.txt'
        logging.basicConfig(filename=os.path.join(log_directory, log_filename), level=logging.WARN,
                            format='%(asctime)s - %(levelname)s - %(message)s', filemode='a')
        logging.info(f"Logging started")

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

    def start_syslog_server(self, enable_file_logging=True):
        def write_syslog_to_file(directory, filename, message):
            if enable_file_logging:
                os.makedirs(directory, exist_ok=True)
                file_path = os.path.join(directory, filename)
                with open(file_path, 'a') as file:
                    file.write(message + '\n')

        def process_syslog_queue():
            while self.is_running:
                try:
                    addr, message = self.message_queue.get(timeout=1)
                    try:
                        handle_syslog(addr, message)
                    except Exception as e:
                        logging.error(f"Error processing syslog message: {e}")
                        logging.error(f"Message: {message}")
                    finally:
                        self.message_queue.task_done()
                except queue.Empty:
                    continue
                except Exception as e:
                    logging.error(f"Unexpected error in process_syslog_queue: {e}")
                    time.sleep(1)

        def monitor_queue_size():
            while self.is_running:
                tcnt = threading.active_count()
                if tcnt < self.min_thread_count:
                    threads_to_start = self.min_thread_count - tcnt
                    logging.warning(f"Thread count dropped to {tcnt}. Starting {threads_to_start} new threads.")
                    for _ in range(threads_to_start):
                        self.executor.submit(process_syslog_queue)
                    with open(self.thread_monitoring_file, 'a') as f:
                        f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Thread Count Before Replenishment: {tcnt}\n")
               
                queue_size = self.message_queue.qsize()
                if queue_size > 0 and queue_size != self.last_logged_size:
                    with open(self.queue_monitoring_file, 'a') as f:
                        f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Queue Size: {queue_size}\n")
                    self.last_logged_size = queue_size
                time.sleep(10)

        def periodic_flush():
            while self.is_running:
                time.sleep(self.flush_interval)
                try:
                    flush_all_batches()
                    logging.info(f"Periodic flush completed at {time.strftime('%Y-%m-%d %H:%M:%S')}")
                except Exception as e:
                    logging.error(f"Error during periodic flush: {e}")

        IP = "10.23.252.3"
        PORT = 514

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((IP, PORT))

        logging.info(f"Syslog server started on {IP}:{PORT}")

        for _ in range(self.min_thread_count):
            self.executor.submit(process_syslog_queue)
        
        threading.Thread(target=monitor_queue_size, daemon=True).start()
        threading.Thread(target=periodic_flush, daemon=True).start()

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

if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(SyslogService)
