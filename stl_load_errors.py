import configparser
import psycopg2


def read_errors(cur, conn):
    cur.execute("select * From stl_load_errors")
    result = cur.fetchall()
    conn.commit()
    return result

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    print(read_errors(cur, conn))

    conn.close()


if __name__ == "__main__":
    main()