import psycopg2
import csv

from config import config

with open('ord2.txt', 'r') as f:
    reader = csv.reader(f, delimiter='\t')
    next(reader, None)
    read_lines = []
    included = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
    for row in reader:
        content = list(row[i] for i in included)
        read_lines.append(tuple(content))
    print (read_lines)

def insert_customer(customer):

    sql = """INSERT INTO orders VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql, customer)

        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

if __name__=='__main__':
    for i in read_lines:    
        insert_customer(i)
