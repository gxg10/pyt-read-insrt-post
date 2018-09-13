import psycopg2
import csv
from fpdf import FPDF

from config import config

with open('ord.txt', 'r') as f:
    reader = csv.reader(f, delimiter='\t')
    next(reader, None)
    ordine_noi = []
    ordine_modif = []
    ordine_executate = []
    ordine_anulate = []
#### TABEL nou
##    included = [6, 3, 7, 27, 9, 8, 1, 43,5, 4, 40, 36]
    included = [1, 3, 4, 5, 6, 7, 8, 9, 10, 20, 34, 35, 36, 43, 46, 49]
    content = []
    for row in reader:
        if row[34] == '1':
            content = list(row[i] for i in included)
            ordine_noi.append(tuple(content))
        elif row[34] == '2':
            content = list(row[i] for i in included)
            ordine_modif.append(tuple(content))
        elif row[34] == '3':
            content = list(row[i] for i in included)
            ordine_executate.append(tuple(content))    
        elif row[34] == '12':
            content = list(row[i] for i in included)
            ordine_anulate.append(tuple(content))
##    print (ordine_noi)

##with open('IFBK_Clienti.txt', 'r') as g:
##    reader = csv.reader(g, delimiter='\t')
##    clienti = []
##    included = [0, 1, 3, 4, 6]
##    content = []
##    for row in reader:
##        content = list(row[i] for i in included)
##        clienti.append(tuple(content))
####    print (clienti)


##    sql = """INSERT INTO ord77( data, simbol, side, cont_arena, volum, pret, order_no, internal_account, piata, simbol_type, limita_pret, trader) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

def insert_customer(customer):
    
    sql = """INSERT INTO ord77s(status, order_no, simbol, simbol_type, market, ef_time, side, price, volum, order_term, ticket, update_type, update_time, trader, internal_account, cant_exec) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

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

def insert_clienti(clienti):

    sql = """ INSERT INTO clienti(iacc, nume, broker, pers, cnp) VALUES (%s, %s, %s, %s, %s)"""

    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.executemany(sql, clienti)

        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
##print (insert_clienti(clienti))

##def read(account):
##    sql = """SELECT * from clienti join ord7 on clienti.id = ord7.id where clienti.iacc ="""+ str(account)
##    conn = None
##    try:
##        params = config()
##        conn = psycopg2.connect(**params)
##        cur = conn.cursor()
##        cur.execute(sql)
##        test = cur.fetchall()
##    except (Exception, psycopg2.DatabaseError) as error:
##        print(error)
##    finally:
##        if conn is not None:
##            conn.close()
##    return test
##
##print (read(1027170))

# if UTY==1:
#     insert_to_sql
# elif UTY==2:
#     ordine deja intorduse, trebuie modificat:
#     , update volum, pret (il gasesc dupa UTY=2, la UTI am data modific)
# elif UTY==3:
#     ordine deja intrdosue care este executat: partial / total   
#     la fiecare executie partiala: OST:1 si LQY:cantitatea exec, la TRD am ticket
#     la fiecare executie completa: OST:2 si LQY: cantaitatea exec, la TRD am ticket
# elif UTY==11:
#     ordin day deja introdus, care a fost anulat de sistem:
#     la UTI am data anularii care trebuie adaugata pe ordin, STS=0
# elif UTY==12:
#     ordin day deja introdus, care a fost anulat de sistem pentru pret prea mare:
#     la UTI am data anularii care trebuie adaugata pe ordin, STS=0
# ef_time = row[6]
# simbol_type=row[4]
# piata = row[5]
# side = row[7]
# account_arena = row[27]
# volum = row[9]
# pret = row[8]
# stadiu = row[34] #UTY
# trader = row[36]
# order_no=row[1]
# limita_pret=row[40]

############if __name__=='__main__':
############   for i in ordine_noi:    
############       insert_customer(i)
