import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, test_queries


def load_staging_tables(cur, conn):
    """Loads log data and song data from S3 to redshift cluster"""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    """Inserts data from staging tables to tables in redshift cluster"""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def testing_queries(cur, conn):
    """Prints number of records in each table in redshift cluster"""
    for query in test_queries:
        print(query[1])
        cur.execute(query[0])
        for row in cur.fetchall():
            print(row)
        
        conn.commit()

def main():
    """Loads data from S3 to redshift cluster in start schema and test the database"""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} \
                            port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    testing_queries(cur, conn)
    conn.close()


if __name__ == "__main__":
    main()