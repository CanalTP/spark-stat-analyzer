import pytest
import subprocess
import os
from tests.integrations.utils import database, current_path, schema
from includes.logger import get_logger
from models import Base


class Mechanism(object):
    @classmethod
    def call(cls, cmd):
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (stdout, stderr) = p.communicate(timeout=15*60)
        if stderr:
            get_logger().debug("Message : ", stderr)

    @classmethod
    @pytest.fixture(scope='function', autouse=True)
    def clean_database(cls):
        tables = [str(table) for table in Base.metadata.sorted_tables]
        query = 'TRUNCATE {} CASCADE;'.format(', '.join(tables))
        database.cursor.execute(query)
        database.connection.commit()

    @classmethod
    @pytest.fixture(scope="session", autouse=True)
    def drop_database(cls, request):
        msg = '--- Skip tables dropping ---'
        if request.config.getvalue("create_database"):
            msg = '--- drop all tables ---'
            tables = [str(table) for table in Base.metadata.sorted_tables]
            tables.append(schema+'.alembic_version')
            query = 'DROP TABLE {} CASCADE;'.format(', '.join(tables))
            database.cursor.execute(query)
            database.connection.commit()

        get_logger().debug(msg)

    def get_data(self, table_name, columns):
        return database.select_from_table(table_name=table_name, columns=columns)

    @staticmethod
    def _format_input(analyzer):
        return os.path.join(current_path, 'tests/fixtures/{analyzer}').format(analyzer=analyzer)

    def launch(self, analyzer, start_date, end_date):
        cmd = "/usr/local/spark/bin/spark-submit  --conf spark.ui.showConsoleProgress=true --master='local[8]'" \
              " manage.py -i {input} -a {analyzer} -s {start_date} -e {end_date}".format(
            input=self._format_input(analyzer),
            start_date=start_date,
            end_date=end_date,
            analyzer=analyzer
        )
        self.call(cmd=cmd)