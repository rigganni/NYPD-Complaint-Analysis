import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from sshtunnel import SSHTunnelForwarder

def insert_tables(cur, conn):
    """
    Insert data into dimensional model from EMR created CSV files
 
    Parameters:
    conn(psycopg2.connect): Postgres connection to RedShift sparkify db
    cur(psycopg2.cursor): Postgres cursor to RedShift sparkify db
 
    Returns:
    None
    """

    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    """
    Load and insert data into nypc_complaint RedShift database
 
    Parameters:
    None
 
    Returns:
    None
    """

    # Obtain RedShift cluster & db details
    config = configparser.ConfigParser()
    config.read('redshift.cfg')

    REMOTE_SSH_PORT = 22
    REMOTE_USERNAME = "ec2-user"
    DWH_HOST = config.get("DWH", "DWH_HOST")
    DWH_DB= config.get("DWH","DWH_DB")
    DWH_DB_USER= config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD= config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT = int(config.get("DWH","DWH_PORT"))
    REMOTE_PKEY = config.get("BASTION", "PRIVATE_KEY_FILE")
    REMOTE_HOST = config.get("BASTION", "PUBLIC_DNS")

    with SSHTunnelForwarder((REMOTE_HOST, REMOTE_SSH_PORT),
             ssh_username=REMOTE_USERNAME,
             ssh_pkey=REMOTE_PKEY,
             remote_bind_address=(DWH_HOST, DWH_PORT),
             local_bind_address=('localhost', 5440)):


        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format("localhost", DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, 5440))
        cur = conn.cursor()
        
        insert_tables(cur, conn)

        conn.close()


if __name__ == "__main__":
    main()
