import logging

from airflow.configuration import conf
from airflow.hooks import PrestoHook
from airflow.models import BaseOperator
from airflow.utils import apply_defaults


class PrestoCheckOperator(BaseOperator):
    """
    Performs a simple check using sql code in a specific Presto database.

    :param sql: the sql to be executed
    :type sql: string
    :param presto_dbid: reference to the Presto database
    :type presto_dbid: string
    """

    __mapper_args__ = {
        'polymorphic_identity': 'PrestoCheckOperator'
    }
    template_fields = ('sql',)
    template_ext = ('.hql', '.sql',)

    @apply_defaults
    def __init__(
            self, sql,
            presto_dbid=conf.get('hooks', 'PRESTO_DEFAULT_DBID'),
            *args, **kwargs):
        super(PrestoCheckOperator, self).__init__(*args, **kwargs)

        self.presto_dbid = presto_dbid
        self.hook = PrestoHook(presto_dbid=presto_dbid)
        self.sql = sql

    def execute(self, execution_date=None):
        logging.info('Executing SQL check: ' + self.sql)
        records = self.hook.get_records(hql=self.sql)
        if not records:
            return False
        else:
            if str(records[0][0]) in ('0', '',):
                return False
            else:
                return True
