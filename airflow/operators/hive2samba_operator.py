import logging
import tempfile

from airflow.configuration import conf
from airflow.hooks import HiveHook, SambaHook
from airflow.models import BaseOperator
from airflow.utils import apply_defaults


class Hive2SambaOperator(BaseOperator):
    """
    Executes hql code in a specific Hive database.

    :param hql: the hql to be exported
    :type hql: string
    :param hive_conn_id: reference to the Hive database
    :type hive_conn_id: string
    :param samba_conn_id: reference to the samba destination
    :type samba_conn_id: string
    """

    __mapper_args__ = {
        'polymorphic_identity': 'Hive2SambaOperator'
    }
    template_fields = ('hql',)
    template_ext = ('.hql', '.sql',)

    @apply_defaults
    def __init__(
            self, hql,
            samba_conn_id,
            destination_filepath,
            hive_conn_id=conf.get('hooks', 'HIVE_DEFAULT_CONN_ID'),
            *args, **kwargs):
        super(Hive2SambaOperator, self).__init__(*args, **kwargs)

        self.hive_conn_id = hive_conn_id
        self.samba_conn_id = samba_conn_id
        self.destination_filepath = destination_filepath
        self.samba = SambaHook(samba_conn_id=samba_conn_id)
        self.hook = HiveHook(hive_conn_id=hive_conn_id)
        self.hql = hql.strip().rstrip(';')

    def execute(self, execution_date):
        tmpfile = tempfile.NamedTemporaryFile()
        tmpfilename = tmpfile.name
        tmpfile.close()
        logging.info('Exporting file from Hive to local')
        self.hook.export_file(self.hql, tmpfile.name)
        logging.info('Pushing file to remote Samba drive')
        self.samba.push_from_local(self.destination_filepath, tmpfilename)
