import psycopg2
import psycopg2.extras
from psycopg2 import sql
from psycopg2.pool import ThreadedConnectionPool
import threading
import time
import logging
import os
import ipaddress
import multiprocessing

# Setup logging
log_directory = r'C:\Syslog'
os.makedirs(log_directory, exist_ok=True)
logging.basicConfig(filename=os.path.join(log_directory, 'database_utils.log'), level=logging.WARNING,
                    format='%(asctime)s - %(processName)s - %(threadName)s - %(levelname)s - %(message)s', filemode='a')

DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/ciscoise"

# Create a connection pool
min_conn = 1
max_conn = 30
connection_pool = None
pool_lock = threading.Lock()

def get_connection_pool():
    global connection_pool
    if connection_pool is None:
        with pool_lock:
            if connection_pool is None:
                connection_pool = ThreadedConnectionPool(min_conn, max_conn, DATABASE_URL)
    return connection_pool

class BatchedDatabaseInserter:
    def __init__(self, table_name, fields, field_types, not_null_fields, max_batch_size=200, max_wait_time=60):
        self.table_name = table_name
        self.fields = fields
        self.field_types = field_types
        self.not_null_fields = not_null_fields
        self.max_batch_size = max_batch_size
        self.max_wait_time = max_wait_time
        self.batch = []
        self.last_insert_time = time.time()
        self.lock = threading.Lock()
        self.timer = None
        self.rejected_count = multiprocessing.Value('i', 0)

    def validate_data(self, row_data):
        if len(row_data) != len(self.fields):
            return False, f"Mismatch in number of fields. Expected {len(self.fields)}, got {len(row_data)}"
        for i, (value, field_type, field_name) in enumerate(zip(row_data, self.field_types, self.fields)):
            if value is None:
                if field_name in self.not_null_fields:
                    return False, f"NULL value not allowed for field {field_name}"
                continue
            if field_type == 'inet':
                try:
                    ipaddress.ip_address(value)
                except ValueError:
                    return False, f"Invalid inet address for field {field_name}: {value}"
            elif field_type == 'int':
                if not isinstance(value, int) and not (isinstance(value, str) and value.isdigit()):
                    return False, f"Invalid integer for field {field_name}: {value}"
            elif field_type == 'text':
                if not isinstance(value, str):
                    return False, f"Invalid text for field {field_name}: {value}"
        return True, ""

    def add_to_batch(self, row_data):
        with self.lock:
            is_valid, error_message = self.validate_data(row_data)
            if is_valid:
                self.batch.append(row_data)
                if len(self.batch) >= self.max_batch_size:
                    logging.warning(f"Max Batch Size Hit for {self.table_name}. Inserting batch.")
                    threading.Thread(target=self._insert_batch).start()
                elif self.timer is None:
                    self.timer = threading.Timer(60.0, self._insert_batch)
                    self.timer.start()
            else:
                with self.rejected_count.get_lock():
                    self.rejected_count.value += 1
                logging.warning(f"Rejected log for {self.table_name} due to data structure mismatch: {error_message}")

    def _insert_batch(self):
        with self.lock:
            if not self.batch:
                return

            if self.timer:
                self.timer.cancel()
                self.timer = None

            batch_to_insert = self.batch
            self.batch = []

        conn = None
        try:
            pool = get_connection_pool()
            if pool is None:
                logging.error("Connection pool is not available")
                return

            conn = pool.getconn()
            cursor = conn.cursor()

            insert_stmt = sql.SQL('INSERT INTO {} ({}) VALUES %s').format(
                sql.Identifier(self.table_name),
                sql.SQL(', ').join(map(sql.Identifier, self.fields))
            )

            psycopg2.extras.execute_values(cursor, insert_stmt, batch_to_insert)
            conn.commit()
            logging.warning(f"Inserted {len(batch_to_insert)} rows into {self.table_name}")
        except psycopg2.pool.PoolError:
            logging.error("Connection pool is closed")
        except Exception as error:
            logging.error(f"Error inserting batch data into {self.table_name}: {error}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                cursor.close()
                try:
                    pool = get_connection_pool()
                    if pool:
                        pool.putconn(conn)
                except psycopg2.pool.PoolError:
                    logging.error("Failed to return connection to pool")

        self.last_insert_time = time.time()

    def flush(self):
        self._insert_batch()

    def get_batch_size(self):
        return len(self.batch)

    def get_rejected_count(self):
        return self.rejected_count.value

# Create instances for each table with field types and NOT NULL constraints
fta_inserter = BatchedDatabaseInserter('fta', 
    ('timestamp', 'ipaddress', 'username', 'nasipaddress', 'remoteaddress', 'failurereason', 'networkdevicename', 'requestlatency'),
    ('text', 'text', 'text', 'text', 'text', 'text', 'text', 'int'),
    not_null_fields=['timestamp'])

fwa_inserter = BatchedDatabaseInserter('fwa', 
    ('timestamp', 'ipaddress', 'username', 'nasipaddress', 'calledstationid', 'failurereason', 'networkdevicename'),
    ('text', 'text', 'text', 'text', 'text', 'text', 'text'),
    not_null_fields=['timestamp'])

fla_inserter = BatchedDatabaseInserter('fla', 
    ('timestamp', 'ipaddress', 'username', 'nasipaddress', 'nasportid', 'failurereason', 'networkdevicename'),
    ('text', 'text', 'text', 'text', 'text', 'text', 'text'),
    not_null_fields=['timestamp'])

pwa_inserter = BatchedDatabaseInserter('pwa', 
    ('timestamp', 'sourceip', 'nasipaddress', 'networkdevicename', 'requestlatency', 'ciscoavpairmethod', 'username', 'authenticationmethod', 'authenticationidentitystore', 'selectedaccessservice', 'selectedauthorizationprofiles', 'identitygroup', 'selectedauthenticationidentitystores', 'authenticationstatus', 'ndlocation', 'nddevice', 'ndrollout', 'ndreauth', 'ndclosed', 'identitypolicymatchedrule', 'authorizationpolicymatchedrule', 'subjectcommonname', 'endpointmacaddress', 'isepolicysetname', 'adhostresolveddns', 'daystoexpiry', 'sessiontimeout', 'ciscoavpairacs', 'deviceip', 'calledstationid', 'radiusflowtype'),
    ('text', 'inet', 'inet', 'text', 'int', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'int', 'int', 'text', 'inet', 'text', 'text'),
    not_null_fields=['timestamp'])

pla_inserter = BatchedDatabaseInserter('pla', 
    ('timestamp', 'sourceip', 'nasipaddress', 'nasportid', 'networkdevicename', 'requestlatency', 'ciscoavpairmethod', 'username', 'authenticationmethod', 'authenticationidentitystore', 'selectedaccessservice', 'selectedauthorizationprofiles', 'identitygroup', 'selectedauthenticationidentitystores', 'authenticationstatus', 'ndlocation', 'nddevice', 'ndrollout', 'ndreauth', 'ndclosed', 'identitypolicymatchedrule', 'authorizationpolicymatchedrule', 'subjectcommonname', 'endpointmacaddress', 'isepolicysetname', 'adhostresolveddns', 'daystoexpiry', 'sessiontimeout', 'ciscoavpairacs', 'deviceip'),
    ('text', 'inet', 'inet', 'text', 'text', 'int', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'int', 'int', 'text', 'inet'),
    not_null_fields=['timestamp'])

tca_inserter = BatchedDatabaseInserter('tca', 
    ('timestamp', 'username', 'networkdevicename', 'networkdeviceip', 'remotedevice', 'cmdset', 'ipaddress'),
    ('text', 'text', 'text', 'inet', 'inet', 'text', 'inet'),
    not_null_fields=['timestamp', 'username', 'networkdevicename', 'networkdeviceip', 'cmdset'])

def flush_all_batches():
    for inserter in [fta_inserter, fwa_inserter, fla_inserter, pwa_inserter, pla_inserter, tca_inserter]:
        inserter.flush()

def log_batch_status():
    for name, inserter in [('fta', fta_inserter), ('fwa', fwa_inserter), ('fla', fla_inserter), 
                           ('pwa', pwa_inserter), ('pla', pla_inserter), ('tca', tca_inserter)]:
        logging.info(f"Batch size for {name}: {inserter.get_batch_size()}")
        logging.info(f"Rejected count for {name}: {inserter.get_rejected_count()}")

def get_total_batch_size():
    return sum(inserter.get_batch_size() for inserter in [fta_inserter, fwa_inserter, fla_inserter, 
                                                          pwa_inserter, pla_inserter, tca_inserter])

def get_total_rejected_count():
    return sum(inserter.get_rejected_count() for inserter in [fta_inserter, fwa_inserter, fla_inserter, 
                                                              pwa_inserter, pla_inserter, tca_inserter])

def cleanup_connections():
    global connection_pool
    with pool_lock:
        if connection_pool:
            connection_pool.closeall()
            connection_pool = None
            logging.info("All database connections closed.")
