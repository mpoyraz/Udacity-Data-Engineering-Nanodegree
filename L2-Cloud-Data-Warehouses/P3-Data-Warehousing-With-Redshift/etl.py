import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    """ Load songs and users log data from S3 into Redshift staging tables. """
    
    print('Copying songs and users log data from S3 to Redshift staging tables')
    for query in copy_table_queries:
        print('Executing the following query:\n{}'.format(query))
        cur.execute(query)
        conn.commit()
        print('The execution of the query is completed\n')

def insert_tables(cur, conn):
    """ Load songs and users log data from S3 into Redshift staging tables. """
    
    print('Creating star schema by inserting data from staging tables')
    for query in insert_table_queries:
        print('Executing the following query:\n{}'.format(query))
        cur.execute(query)
        conn.commit()
        print('The execution of the query is completed\n')

def main():
    """ 
    - Reads the configuration file.
    - Connects to the Redshift cluster through its endpoint address.
    - Loads data from S3 into staging tables.
    - Performs ETL on staging tables and inserts data into fact/dimention tables.
    """

    # Read the configuration file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect to the Redshift cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # Load data from S3 into staging tables and perform ETL in Redshift
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    # Close the cursor and connection to the database
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()