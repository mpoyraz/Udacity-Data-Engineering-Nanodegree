import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    """ Drops each table using the queries in 'drop_table_queries' list. """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    """ Creates each table using the queries in 'create_table_queries' list. """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """ 
    - Reads the configuration file.
    - Connects to the Redshift cluster through its endpoint address.
    - Drops the staging tables and fact/dimenion tables if exists.
    - Creates new staging tables and fact/dimenion tables.
    """

    # Read the configuration file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect to the Redshift cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # Drop the tables if exists and then create new tables
    drop_tables(cur, conn)
    create_tables(cur, conn)

    # Close the cursor and connection to the database
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()