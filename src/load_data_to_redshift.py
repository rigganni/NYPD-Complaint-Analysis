import configparser
import psycopg2
from redshift_queries import copy_table_queries, create_table_queries, drop_table_queries
from sshtunnel import SSHTunnelForwarder

def run_queries(run_local = True, type = "create",  **kwargs):
    """
    Insert data into dimensional model from EMR created CSV files
 
    Parameters:
    kwargs: Keyword arguments
 
    Returns:
    None
    """

    if run_local:
        env = 'local'
    else:
        env = kwargs['dag_run'].conf['env']

    if type == "create":
        query_list = create_table_queries
    elif type == "copy":
        query_list = copy_table_queries
    else:
        query_list = drop_table_queries

    # Obtain RedShift cluster & db details
    # Obtain parameters from redshift.cfg
    config = configparser.ConfigParser()
    config.read_file(open('/tmp/redshift.cfg'))

    REMOTE_SSH_PORT = 22
    REMOTE_USERNAME = "ec2-user"
    DWH_HOST = config.get("DWH", "DWH_HOST")
    DWH_DB= config.get("DWH","DWH_DB")
    DWH_DB_USER= config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD= config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT = int(config.get("DWH","DWH_PORT"))

    # Obtain parameters from bastion.cfg
    config = configparser.ConfigParser()
    config.read_file(open('/tmp/bastion.cfg'))

    # Obtain SSH private key based on environment
    if env == "local":
        REMOTE_PKEY = config.get("BASTION", "PRIVATE_KEY_FILE")
    else:
        REMOTE_PKEY = "/tmp/pkey.pem"

    REMOTE_HOST = config.get("BASTION", "PUBLIC_DNS")

    # Create SSH tunnel & port forward to RedShift 
    with SSHTunnelForwarder((REMOTE_HOST, REMOTE_SSH_PORT),
             ssh_username=REMOTE_USERNAME,
             ssh_pkey=REMOTE_PKEY,
             remote_bind_address=(DWH_HOST, DWH_PORT),
             local_bind_address=('127.0.0.1', 5440)):


        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format("127.0.0.1", DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, 5440))
        cur = conn.cursor()
        
        # Loop through all queries in redshift_queries.py
        for query in query_list:
            print(query)
            cur.execute(query)
            conn.commit()

        conn.close()


def main(**kwargs):
    """
    Load and insert data into nypc_complaint RedShift database
 
    Parameters:
    None
 
    Returns:
    None
    """

    run_queries(type = "drop")
    run_queries(type = "create")
    run_queries(type = "copy")


if __name__ == "__main__":
    main()
