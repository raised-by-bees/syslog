import psycopg2
import psycopg2.extras
from psycopg2 import sql
from psycopg2.pool import ThreadedConnectionPool
import threading
import time
import logging
import os

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
    def __init__(self, table_name, fields, max_batch_size=200, max_wait_time=60):
        self.table_name = table_name
        self.fields = fields
        self.max_batch_size = max_batch_size
        self.max_wait_time = max_wait_time
        self.batch = []
        self.last_insert_time = time.time()
        self.lock = threading.Lock()
        self.timer = None

    def add_to_batch(self, row_data):
        with self.lock:
            self.batch.append(row_data)
            if len(self.batch) >= self.max_batch_size:
                logging.warning(f"Max Batch Size Hit for {self.table_name}. Inserting batch.")
                threading.Thread(target=self._insert_batch).start()
            elif self.timer is None:
                self.timer = threading.Timer(60.0, self._insert_batch)
                self.timer.start()

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

# Create instances for each table
fta_inserter = BatchedDatabaseInserter('fta', ('timestamp', 'ipaddress', 'username', 'nasipaddress', 'remoteaddress', 'failurereason', 'networkdevicename', 'requestlatency'))
fwa_inserter = BatchedDatabaseInserter('fwa', ('timestamp', 'ipaddress', 'username', 'nasipaddress', 'calledstationid', 'failurereason', 'networkdevicename'))
fla_inserter = BatchedDatabaseInserter('fla', ('timestamp', 'ipaddress', 'username', 'nasipaddress', 'nasportid', 'failurereason', 'networkdevicename'))
pwa_inserter = BatchedDatabaseInserter('pwa', ('timestamp', 'sourceip', 'nasipaddress', 'networkdevicename', 'requestlatency', 'ciscoavpairmethod', 'username', 'authenticationmethod', 'authenticationidentitystore', 'selectedaccessservice', 'selectedauthorizationprofiles', 'identitygroup', 'selectedauthenticationidentitystores', 'authenticationstatus', 'ndlocation', 'nddevice', 'ndrollout', 'ndreauth', 'ndclosed', 'identitypolicymatchedrule', 'authorizationpolicymatchedrule', 'subjectcommonname', 'endpointmacaddress', 'isepolicysetname', 'adhostresolveddns', 'daystoexpiry', 'sessiontimeout', 'ciscoavpairacs', 'deviceip', 'calledstationid', 'radiusflowtype'))
pla_inserter = BatchedDatabaseInserter('pla', ('timestamp', 'sourceip', 'nasipaddress', 'nasportid', 'networkdevicename', 'requestlatency', 'ciscoavpairmethod', 'username', 'authenticationmethod', 'authenticationidentitystore', 'selectedaccessservice', 'selectedauthorizationprofiles', 'identitygroup', 'selectedauthenticationidentitystores', 'authenticationstatus', 'ndlocation', 'nddevice', 'ndrollout', 'ndreauth', 'ndclosed', 'identitypolicymatchedrule', 'authorizationpolicymatchedrule', 'subjectcommonname', 'endpointmacaddress', 'isepolicysetname', 'adhostresolveddns', 'daystoexpiry', 'sessiontimeout', 'ciscoavpairacs', 'deviceip'))
tca_inserter = BatchedDatabaseInserter('tca', ('timestamp', 'username', 'networkdevicename', 'networkdeviceip', 'remotedevice', 'cmdset', 'ipaddress'))

def flush_all_batches():
    for inserter in [fta_inserter, fwa_inserter, fla_inserter, pwa_inserter, pla_inserter, tca_inserter]:
        inserter.flush()

def log_batch_status():
    for name, inserter in [('fta', fta_inserter), ('fwa', fwa_inserter), ('fla', fla_inserter), 
                           ('pwa', pwa_inserter), ('pla', pla_inserter), ('tca', tca_inserter)]:
        logging.info(f"Batch size for {name}: {inserter.get_batch_size()}")

        
def get_total_batch_size():
    return sum(inserter.get_batch_size() for inserter in [fta_inserter, fwa_inserter, fla_inserter, 
                                                          pwa_inserter, pla_inserter, tca_inserter])


def cleanup_connections():
    global connection_pool
    with pool_lock:
        if connection_pool:
            connection_pool.closeall()
            connection_pool = None
            logging.info("All database connections closed.")
