import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    #drops the two staging tables as well as the tables that are part of the dimensional model.
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Runs the queries to create the staging tables as well as the fact and dimensional tables.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    #first drop the tables and their contents.
    drop_tables(cur, conn)
    #then create all the tables as per the table creating queries.
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()