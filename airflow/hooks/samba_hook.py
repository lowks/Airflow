import logging
from smbclient import SambaClient

from airflow import settings
from airflow.models import Connection


class SambaHook(object):
    '''
    Allows for interaction with an samba server.
    '''

    def __init__(self, samba_conn_id=None):
        session = settings.Session()
        samba_conn = session.query(
            Connection).filter(
                Connection.conn_id == samba_conn_id).first()
        if not samba_conn:
            raise Exception("The samba id you provided isn't defined")
        self.host = samba_conn.host
        self.login = samba_conn.login
        self.psw = samba_conn.password
        self.schema = samba_conn.schema
        session.commit()
        session.close()

    def get_conn(self):
        samba = SambaClient(
            server=self.host, share=self.schema,
            username=self.login, password=self.psw)
        return samba

    def push_from_local(
            self, destination_filepath, local_filepath, overwrite=True):
        samba = self.get_conn()
        logging.info(
            "Moving local file [{local_filepath}] to remote "
            "[{destination_filepath}]".format(**locals()))
        if overwrite and samba.exists(destination_filepath):
            logging.info("Overwritting existing file")
            samba.unlink(destination_filepath)
        samba.upload(local_filepath, destination_filepath)
