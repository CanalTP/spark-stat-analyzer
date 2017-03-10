import pytest
import subprocess
import os
from includes.logger import get_logger
from models import Base
import config
from includes.database import Database

current_path = os.getcwd()
database = Database(dbname=config.db["dbname"], user=config.db["user"],
                    password=config.db["password"], schema=config.db["schema"],
                    host=config.db['host'], port=config.db['port'],
                    insert_count=config.db['insert_count'])

class Mechanism(object):
    @classmethod
    def call(cls, cmd):
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (stdout, stderr) = p.communicate(timeout=15*60)
        if stderr:
            get_logger().debug("Message : ", str(stderr))

    @classmethod
    @pytest.yield_fixture(scope='function', autouse=True)
    def clean_database(cls):
        yield
        tables = [str(table) for table in Base.metadata.sorted_tables]

        query = 'TRUNCATE {} CASCADE;'.format(', '.join(tables))
        database.cursor.execute(query)
        database.connection.commit()

    def partitionned_table_exists(self, table):
        database.cursor.execute("select * from information_schema.tables where table_name=%s", (table,))
        return bool(database.cursor.rowcount)

    def get_data(self, table_name, columns):
        return database.select_from_table(table_name=table_name, columns=columns)

    @staticmethod
    def _format_input(analyzer):
        return os.path.join(current_path, 'tests/fixtures/{analyzer}').format(analyzer=analyzer)

    def launch(self, analyzer, start_date, end_date):
        os.environ["TABLE"] = analyzer
        os.environ["CONTAINER_STORE_PATH"] = self._format_input(analyzer)
        os.environ["MASTER_OPTION"] = 'local[1]'
        os.environ["START_DATE"] = start_date
        os.environ["END_DATE"] = end_date
        cmd = "./run_analyzer.sh"
        self.call(cmd=cmd)
