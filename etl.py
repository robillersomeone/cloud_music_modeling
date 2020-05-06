import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    """
    instance of the cursor, connection to database,
    function to load staging tables
    COPY events s3 bucket
    COPY song metadata s3 bucket
    commits changes to tables in redshift
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    """
    instance of the cursor, connection to database,
    function to load star tables
    commits changes to tables in redshift
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
    main function
    read config, connect to redshift cluster
    COPY data from s3 to staging data
    insert data into star tables
    closes the connection to the database
    """
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()
