import psycopg2
import csv

from config import config

with open('ord10.txt', 'r') as f:
    reader = csv.reader(f, delimiter='\t')
    next(reader, None)
    ordine_noi = []
    ordine_modif = []
    included = [0, 1, 3, 4, 5, 6, 7, 8, 9, 10, 20, 34, 35, 36, 43, 46, 49]
    content = []

    for row in reader:
        if row[34] == '1':
            content = list(row[i] for i in included)
            ordine_noi.append(tuple(content))
        elif row[34] == '2':
            content = list(row[i] for i in included)
            ordine_modif.append(tuple(content))
##    print (ordine_modif)
        


def insert_customer(customer):

   sql = """INSERT INTO ord8(status, order_no, simbol, simbol_type, market, ef_time, side, price, volum, order_term, ticket, update_type, update_time, trader, iacc, cant_exec, order_status) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
   conn = None
   try:
       params = config()
       conn = psycopg2.connect(**params)
       cur = conn.cursor()
##       cur.execute(sql, customer)
       cur.executemany(sql, customer)

       conn.commit()
       cur.close()
   except (Exception, psycopg2.DatabaseError) as error:
       print(error)
   finally:
       if conn is not None:
           conn.close()
           
print (insert_customer(ordine_noi))
##
##if __name__=='__main__':
##   for i in ordine_noi:    
##       insert_customer(i)
