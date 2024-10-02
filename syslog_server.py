import socket
import logging
import os
import multiprocessing
import queue
import time
import signal
import sys
from database_utils import flush_all_batches, log_batch_status
from handler_dispatcher import handle_syslog, maintenance

# Explicitly set the start method to 'spawn'
multiprocessing.set_start_method('spawn', force=True)

def setup_logging():
    log_directory = r'C:\Syslog'
    os.makedirs(log_directory, exist_ok=True)
    logging.basicConfig(filename=os.path.join(log_directory, 'syslogService.log'), level=logging.DEBUG,
                        format='%(asctime)s - %(processName)s - %(levelname)s - %(message)s', filemode='a')
    logging.info("Logging started")

def process_syslog_messages(message_queue, is_running):
    setup_logging()  # Each process needs its own logging setup
    while is_running.value:
        try:
            ip, message = message_queue.get(timeout=1)
            handle_syslog(ip, message)
        except queue.Empty:
            continue
        except Exception as e:
            logging.error(f"Error processing syslog message: {e}")

def monitor_queue_size(message_queue, is_running):
    setup_logging()  # Each process needs its own logging setup
    queue_monitoring_file = r"C:\Syslog\queue_size.txt"
    last_log_time = 0
    while is_running.value:
        current_time = time.time()
        queue_size = message_queue.qsize()
        
        if current_time - last_log_time >= 10:  # Log every 10 seconds
            logging.info(f"Current queue size: {queue_size}")
            try:
                with open(queue_monitoring_file, 'a') as f:
                    f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Queue Size: {queue_size}\n")
                last_log_time = current_time
            except Exception as e:
                logging.error(f"Error writing to queue monitoring file: {e}")
        
        maintenance()
        
        if queue_size > 50000:  # Half of max_queue_size
            flush_all_batches()
            log_batch_status()
        
        time.sleep(1)

def start_syslog_server():
    setup_logging()
    is_running = multiprocessing.Value('b', True)
    message_queue = multiprocessing.Queue()
    max_queue_size = 100000
    num_processes = max(1, multiprocessing.cpu_count() - 1)

    processes = []
    for _ in range(num_processes):
        p = multiprocessing.Process(target=process_syslog_messages, args=(message_queue, is_running))
        p.start()
        processes.append(p)

    monitor_process = multiprocessing.Process(target=monitor_queue_size, args=(message_queue, is_running))
    monitor_process.start()
    processes.append(monitor_process)

    IP = "10.23.252.4"
    PORT = 514

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((IP, PORT))

    logging.info(f"Syslog server started on {IP}:{PORT}")

    try:
        while is_running.value:
            try:
                data, addr = sock.recvfrom(8192)
                if not data:
                    break
                message = data.decode()
                if message_queue.qsize() < max_queue_size:
                    message_queue.put((addr[0], message))
                else:
                    logging.warning(f"Message queue is full. Dropping message from {addr[0]}.")
            except Exception as e:
                logging.error(f"Error receiving syslog message: {e}")
    finally:
        is_running.value = False
        for process in processes:
            process.join(timeout=5)
            if process.is_alive():
                process.terminate()
        sock.close()
        logging.info("Syslog server stopped")

if __name__ == '__main__':
    multiprocessing.freeze_support()  # Necessary for PyInstaller
    start_syslog_server()
