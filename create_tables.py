import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    """
    instance of the cursor, connection to database,
    function to drop all tables if they exist
    commits changes to tables in redshift
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    """
    instance of the cursor, connection to database,
    function to create staging and star tables
    commits changes to tables in redshift
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
    main function
    read config, connect to redshift cluster
    calls drop tables and create tables
    closes the connection to the database 
    """
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()
