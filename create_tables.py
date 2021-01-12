import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import itertools
import threading
import time
import sys

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        
def animate():
    for c in itertools.cycle(['|', '/', '-', '\\']):
        if done:
            break
        sys.stdout.write('\rcreating ' + c )
        sys.stdout.flush()
        time.sleep(0.1)
    sys.stdout.write('\rfinish!     '+'\n')

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    done = False
    t = threading.Thread(target=animate)
    t.start()
    
    main()
    
    done = True
    t.join()