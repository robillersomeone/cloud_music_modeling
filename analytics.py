import configparser
import psycopg2
from sql_queries import analytics_table_queries

def artist_info(cur, conn):
    """
    instance of the cursor, connection to database,
    function to analyze artist data
    commits changes to tables in redshift
    """
    for query in analytics_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    main function
    read config, connect to redshift cluster
    query normalized tables in redshift
    closes the connection to the database
    """
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    artist_info(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()
