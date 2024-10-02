import socket
import logging
import os
import multiprocessing
import queue
import time
import signal
from database_utils import flush_all_batches, log_batch_status
from handler_dispatcher import handle_syslog, maintenance

class SyslogService:
    def __init__(self):
        self.is_running = multiprocessing.Value('b', True)
        self.message_queue = multiprocessing.Queue()
        self.max_queue_size = 100000
        self.queue_monitoring_file = r"C:\Syslog\queue_size.txt"
        self.thread_monitoring_file = r"C:\Syslog\thread_count.txt"
        self.num_processes = multiprocessing.cpu_count()
        self.flush_interval = 5
        self.processes = []

    def setup_logging(self):
        log_directory = r'C:\Syslog'
        os.makedirs(log_directory, exist_ok=True)
        logging.basicConfig(filename=os.path.join(log_directory, 'syslogService.log'), level=logging.DEBUG,
                            format='%(asctime)s - %(processName)s - %(levelname)s - %(message)s', filemode='a')
        logging.info("Logging started")

    def process_syslog_queue(self):
        while self.is_running.value:
            try:
                ip, message = self.message_queue.get(timeout=1)
                handle_syslog(ip, message)
            except queue.Empty:
                continue
            except Exception as e:
                logging.error(f"Error processing syslog message: {e}")

    def monitor_queue_size(self):
        while self.is_running.value:
            queue_size = self.message_queue.qsize()
            logging.info(f"Current queue size: {queue_size}")
            
            try:
                with open(self.queue_monitoring_file, 'a') as f:
                    f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Queue Size: {queue_size}\n")
            except Exception as e:
                logging.error(f"Error writing to queue monitoring file: {e}")
            
            # Perform maintenance tasks
            maintenance()
            
            # Force a batch insert if queue size is large
            if queue_size > self.max_queue_size // 2:
                flush_all_batches()
                log_batch_status()
            
            time.sleep(10)  # Check every 10 seconds

    def start_syslog_server(self):
        self.setup_logging()

        # Start worker processes
        for _ in range(self.num_processes):
            p = multiprocessing.Process(target=self.process_syslog_queue)
            p.start()
            self.processes.append(p)

        # Start monitoring process
        monitor_process = multiprocessing.Process(target=self.monitor_queue_size)
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

    def stop(self):
        self.is_running.value = False
        for process in self.processes:
            process.join(timeout=5)
            if process.is_alive():
                process.terminate()
        logging.info("Syslog server stopped")

def signal_handler(signum, frame):
    logging.info(f"Received signal {signum}")
    syslog_service.stop()

if __name__ == '__main__':
    multiprocessing.freeze_support()  # Necessary for PyInstaller
    syslog_service = SyslogService()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        syslog_service.start_syslog_server()
    except Exception as e:
        logging.error(f"Error in syslog server: {e}")
    finally:
        syslog_service.stop()
