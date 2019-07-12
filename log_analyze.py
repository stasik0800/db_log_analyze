import pandas as pd
from itertools import groupby
import pandasql as psql
import log_analyze_queries as q


first_q=[]
second_q=[]
third_q =[]
fifth_q=[]
sixth_q=[]
seventh_q=[]
eighth_q=[]
ninth_q=[]


def collect_q1_2_data(info):
    txt = info.strip().lower()

    if "success" == txt:
        first_q.append(txt)

    if "failed"== txt:
        second_q.append(txt)

def collect_q3_data(stmt,info):
    info = info.strip().lower()
    if ("connection id" in info or "success" == info) and info.count("-") <= 1:
        third_q.append([stmt,info])


def collect_q6_data(usr,info):
    info = info.strip().lower()
    usr=usr.strip()
    if "success" == info or "failed" == info:
        sixth_q.append([usr,info])


def collect_q5_data(dt_stmt,dt,info):
    info = info.strip().lower()
    fifth_q.append([dt_stmt,dt,info])

def collect_q8_data(ips,info):
    info = info.strip().lower()
    ips = ips.strip()
    if "statement" in info:
        eighth_q.append([ips,info])


def collect_q7_data(dt_stmt,stmt,info):
    info = info.strip().lower()
    # if ("connection id" in info or "success" == info) and info.count("-") <= 1:
    seventh_q.append([dt_stmt,stmt, info])


def collect_q9_data(usr,stmt):
    ninth_q.append([usr,stmt])



def results():
#q.1 : result
    print "Number of Successful statements: ",len(first_q)

#q.2 : result
    print "Number of Failed statements: ",len(second_q)

#q.3 : result
    df = pd.DataFrame(third_q)
    df.columns=['stmt','info']
    q3 = q.query_q3()
    for val in (psql.sqldf(q3, locals())).values.tolist():
        print "Connection Number {1}:  {0} Successful statements".format(val[0],val[1])


#q.5 : result
    df = pd.DataFrame(fifth_q)
    df.columns=['dt_stmt','stmt','info']
    q5 = q.query_q5()
    for val in (psql.sqldf(q5, locals())).values.tolist():
        print "The last statement was sent at: {0} for Connection id {1}".format(val[0],val[1])

#q.6 : result
    for value, freq in groupby(sorted(sixth_q)):
        print "user {0} sent {1} {2} statements".format(value[0],value[1],len(list(freq)))

#q.7 : result
    df = pd.DataFrame(seventh_q)
    df.columns = ['dt_stmt', 'stmt', 'info']
    q7 = q.query_q7()
    stmt = (psql.sqldf(q7, locals())).values.tolist()[0]
    print "statement {} was the slowest".format(stmt[0])

#q.8 : result
    df = pd.DataFrame(eighth_q)
    df.columns =['ips','info']
    q8 = q.query_q8()
    val =  (psql.sqldf(q8,locals())).values.tolist()[0]
    print "{} different ips sent statements to the user".format(val[0])

#q.9 : result
    df = pd.DataFrame(ninth_q)
    df.columns = ['usr','stmt']
    q9 = q.query_q9()
    for val in  (psql.sqldf(q9, locals())).values.tolist():
        print "the user{0} sent total of {1} statements".format(val[0],val[1])



def main():
    ## Please Change location of file
    location = '~/task/stas_logfile.log'
    f  = open(location,"r")
    lines = f.readlines()

### referance row[x]
# 0) datetime of statement.
# 1) thread number.
# 2) IP of the user running the statement + port.
# 3) the database the user ran the statement to.
# 4) IP of the user running the statement.
# 5) user name of the user running the statement.
# 6) statement id of statement the user executed.
# 7) service name.
# 8) info column includes - success of statement, the statement itself, connection id, start/end time of the statement, number of rows returned. (changes dynamically)

    for line in lines:
        row = line.split('|')

        collect_q1_2_data(row[8])

        collect_q3_data(row[6],row[8])

        collect_q5_data(row[0],row[6],row[8])

        collect_q6_data(row[5],row[8])

        collect_q7_data(row[0],row[6], row[8])

        # Not clear ip is unique for all stmt?
        collect_q8_data(row[4],row[8])

        collect_q9_data(row[5],row[6])

    results()

main()
