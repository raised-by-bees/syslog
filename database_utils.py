import psycopg2
import psycopg2.extras
from psycopg2 import sql
import threading
import time
import logging

DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/ciscoise"

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
                self._insert_batch()
            elif self.timer is None:
                self.timer = threading.Timer(self.max_wait_time, self._insert_batch)
                self.timer.start()

    def _insert_batch(self):
        with self.lock:
            if not self.batch:
                return

            if self.timer:
                self.timer.cancel()
                self.timer = None

            conn = None
            cursor = None
            try:
                conn = psycopg2.connect(DATABASE_URL)
                cursor = conn.cursor()

                insert_stmt = sql.SQL('INSERT INTO {} ({}) VALUES %s').format(
                    sql.Identifier(self.table_name),
                    sql.SQL(', ').join(map(sql.Identifier, self.fields))
                )

                psycopg2.extras.execute_values(cursor, insert_stmt, self.batch)
                conn.commit()
                logging.info(f"Inserted {len(self.batch)} rows into {self.table_name}")
            except Exception as error:
                logging.error(f"Error inserting batch data into {self.table_name}: {error}")
                if cursor:
                    cursor.execute("ROLLBACK")
                    conn.commit()
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    conn.close()

            self.batch = []
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
