from datetime import datetime
import requests
import psycopg2 as ps2
import psycopg2.extras as extras 
import pandas as pd

url = f"http://api.exchangerate.host/timeframe"
#Запрос с BTC/RUB
response = requests.get(url, params={'access_key': "1731467954af1a70db43b92e62e2e131",
                                'source': "BTC",
                                'currencies': "RUB",
                                'start_date':"2023-10-01",
                                'end_date':"2023-10-31",
                                'format': 1})
data = response.json()


pg_hostname = 'db'
pg_port = '5432'
pg_username = 'postgres'
pg_pass = 'password'
pg_db = 'test'

conn = ps2.connect(host=pg_hostname,
                   port=pg_port,
                   user=pg_username,
                   password=pg_pass,
                   database=pg_db)
cursor = conn.cursor()
#таблица BTCRUB
cursor.execute(f"""CREATE TABLE IF NOT EXISTS rates_month_BTC_RUB (
              id serial primary key,
              dates date,
              source varchar(255),
              currencies varchar(255),
              rate double precision);""")
conn.commit()

for date, quotes in data['quotes'].items():
    for currency, rate in quotes.items():
        insert_query = f"""
        INSERT INTO rates_month_BTC_RUB(dates, source, currencies, rate) VALUES
        ('{date}', '{data['source']}', '{currency}', {rate})
        """
        cursor.execute(insert_query)
conn.commit()

select_query = "SELECT dates, source, currencies, rate FROM rates_month_BTC_RUB"
cursor.execute(select_query)
conn.commit()
cursor.close()
conn.close()
#выполнение аналитики
conn = ps2.connect(host=pg_hostname,
                   port=pg_port,
                   user=pg_username,
                   password=pg_pass,
                   database=pg_db)

query = "SELECT dates, source, currencies, rate FROM rates_month_BTC_RUB"
df = pd.read_sql_query(query, conn)
conn.close()
#день с максимальным курсом
max_rate_day = df[df['rate'] == df['rate'].max()]['dates'].values[0]
#день с минимальным курсом
min_rate_day = df[df['rate'] == df['rate'].min()]['dates'].values[0]


# Максимальное значение курса
max_rate = df['rate'].max()


# Минимальное значение курса
min_rate = df['rate'].min()


# Среднее значение курса за весь месяц
avg_rate = df['rate'].mean()


# Значение курса на последний день месяца
last_day_rate = df.sort_values(by='dates', ascending=False)['rate'].values[0]

#месяц в котором проходит действо
month =df.sort_values(by='dates', ascending=True)['dates'].values[0].month
#замутим датасет из всего этого
one = pd.DataFrame({'MAX_rate_day': max_rate_day,'MIN_rate_day': min_rate_day,
                       'MAX_rate': max_rate,'MIN_rate': min_rate,
                       'AVG_rate': avg_rate,'LAST_DAY_rate': last_day_rate,
                       'Month': month,'Currencies': currency,},index=[0])
conn = ps2.connect(host=pg_hostname,
                   port=pg_port,
                   user=pg_username,
                   password=pg_pass,
                   database=pg_db)
cursor = conn.cursor()
cursor.execute(f"""CREATE TABLE IF NOT EXISTS one_raw_table (
              MAX_rate_day date,
              MIN_rate_day date,
              MAX_rate double precision,
              MIN_rate double precision,
              AVG_rate double precision,
              LAST_DAY_rate double precision,
              Month int8,
              currencies varchar(255));""")
conn.commit()


def execute_values(conn, df, table): 
  
    tuples = [tuple(x) for x in df.to_numpy()] 
  
    cols = ','.join(list(df.columns)) 
    # SQL query to execute 
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols) 
    cursor = conn.cursor() 
    try: 
        extras.execute_values(cursor, query, tuples) 
        conn.commit() 
    except (Exception, ps2.DatabaseError) as error: 
        print("Error: %s" % error) 
        conn.rollback() 
        cursor.close() 
        return 1
    print("the dataframe btc/rub is inserted") 
  

execute_values(conn, one, 'one_raw_table') 
conn.commit()
cursor.close()
conn.close()
#___________________________________________________________________________________________________________________________________
#Запрос с USD_JPY
response = requests.get(url, params={'access_key': "1731467954af1a70db43b92e62e2e131",
                                'source': "USD",
                                'currencies': "JPY",
                                'start_date':"2023-10-01",
                                'end_date':"2023-10-31",
                                'format': 1})
data = response.json()


conn = ps2.connect(host=pg_hostname,
                   port=pg_port,
                   user=pg_username,
                   password=pg_pass,
                   database=pg_db)
cursor = conn.cursor()
#таблица USD_JPY
cursor.execute(f"""CREATE TABLE IF NOT EXISTS rates_month_USD_JPY (
              id serial primary key,
              dates date,
              source varchar(255),
              currencies varchar(255),
              rate double precision);""")
conn.commit()

for date, quotes in data['quotes'].items():
    for currency, rate in quotes.items():
        insert_query = f"""
        INSERT INTO rates_month_USD_JPY(dates, source, currencies, rate) VALUES
        ('{date}', '{data['source']}', '{currency}', {rate})
        """
        cursor.execute(insert_query)
conn.commit()

select_query = "SELECT dates, source, currencies, rate FROM rates_month_USD_JPY"
cursor.execute(select_query)
conn.commit()
cursor.close()
conn.close()
#выполнение аналитики
conn = ps2.connect(host=pg_hostname,
                   port=pg_port,
                   user=pg_username,
                   password=pg_pass,
                   database=pg_db)

query = "SELECT dates, source, currencies, rate FROM rates_month_USD_JPY"
df = pd.read_sql_query(query, conn)
conn.close()
#день с максимальным курсом
max_rate_day = df[df['rate'] == df['rate'].max()]['dates'].values[0]
#день с минимальным курсом
min_rate_day = df[df['rate'] == df['rate'].min()]['dates'].values[0]


# Максимальное значение курса
max_rate = df['rate'].max()


# Минимальное значение курса
min_rate = df['rate'].min()


# Среднее значение курса за весь месяц
avg_rate = df['rate'].mean()


# Значение курса на последний день месяца
last_day_rate = df.sort_values(by='dates', ascending=False)['rate'].values[0]

#месяц в котором проходит действо
month =df.sort_values(by='dates', ascending=True)['dates'].values[0].month
#замутим датасет из всего этого
two = pd.DataFrame({'MAX_rate_day': max_rate_day,'MIN_rate_day': min_rate_day,
                       'MAX_rate': max_rate,'MIN_rate': min_rate,
                       'AVG_rate': avg_rate,'LAST_DAY_rate': last_day_rate,
                       'Month': month,'Currencies': currency,},index=[0])
conn = ps2.connect(host=pg_hostname,
                   port=pg_port,
                   user=pg_username,
                   password=pg_pass,
                   database=pg_db)


def execute_values(conn, df, table): 
  
    tuples = [tuple(x) for x in df.to_numpy()] 
  
    cols = ','.join(list(df.columns)) 
    
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols) 
    cursor = conn.cursor() 
    try: 
        extras.execute_values(cursor, query, tuples) 
        conn.commit() 
    except (Exception, ps2.DatabaseError) as error: 
        print("Error: %s" % error) 
        conn.rollback() 
        cursor.close() 
        return 1
    print("the dataframe usd/jpy is inserted") 
  

execute_values(conn, two, 'one_raw_table') 
conn.commit()
cursor.close()
conn.close()
#___________________________________________________________________________________________________________________________________
#Запрос с AED_GBP
response = requests.get(url, params={'access_key': "1731467954af1a70db43b92e62e2e131",
                                'source': "AED",
                                'currencies': "GBP",
                                'start_date':"2023-10-01",
                                'end_date':"2023-10-31",
                                'format': 1})
data = response.json()


conn = ps2.connect(host=pg_hostname,
                   port=pg_port,
                   user=pg_username,
                   password=pg_pass,
                   database=pg_db)
cursor = conn.cursor()
#таблица AED_GBP
cursor.execute(f"""CREATE TABLE IF NOT EXISTS rates_month_AED_GBP (
              id serial primary key,
              dates date,
              source varchar(255),
              currencies varchar(255),
              rate double precision);""")
conn.commit()

for date, quotes in data['quotes'].items():
    for currency, rate in quotes.items():
        insert_query = f"""
        INSERT INTO rates_month_AED_GBP(dates, source, currencies, rate) VALUES
        ('{date}', '{data['source']}', '{currency}', {rate})
        """
        cursor.execute(insert_query)
conn.commit()

select_query = "SELECT dates, source, currencies, rate FROM rates_month_AED_GBP"
cursor.execute(select_query)
conn.commit()
cursor.close()
conn.close()
#выполнение аналитики
conn = ps2.connect(host=pg_hostname,
                   port=pg_port,
                   user=pg_username,
                   password=pg_pass,
                   database=pg_db)

query = "SELECT dates, source, currencies, rate FROM rates_month_AED_GBP"
df = pd.read_sql_query(query, conn)
conn.close()
#день с максимальным курсом
max_rate_day = df[df['rate'] == df['rate'].max()]['dates'].values[0]
#день с минимальным курсом
min_rate_day = df[df['rate'] == df['rate'].min()]['dates'].values[0]


# Максимальное значение курса
max_rate = df['rate'].max()


# Минимальное значение курса
min_rate = df['rate'].min()


# Среднее значение курса за весь месяц
avg_rate = df['rate'].mean()


# Значение курса на последний день месяца
last_day_rate = df.sort_values(by='dates', ascending=False)['rate'].values[0]

#месяц в котором проходит действо
month =df.sort_values(by='dates', ascending=True)['dates'].values[0].month
#замутим датасет из всего этого
three = pd.DataFrame({'MAX_rate_day': max_rate_day,'MIN_rate_day': min_rate_day,
                       'MAX_rate': max_rate,'MIN_rate': min_rate,
                       'AVG_rate': avg_rate,'LAST_DAY_rate': last_day_rate,
                       'Month': month,'Currencies': currency,},index=[0])
conn = ps2.connect(host=pg_hostname,
                   port=pg_port,
                   user=pg_username,
                   password=pg_pass,
                   database=pg_db)


def execute_values(conn, df, table): 
  
    tuples = [tuple(x) for x in df.to_numpy()] 
  
    cols = ','.join(list(df.columns)) 
    
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols) 
    cursor = conn.cursor() 
    try: 
        extras.execute_values(cursor, query, tuples) 
        conn.commit() 
    except (Exception, ps2.DatabaseError) as error: 
        print("Error: %s" % error) 
        conn.rollback() 
        cursor.close() 
        return 1
    print("the dataframe aed/gbp is inserted") 
  

execute_values(conn, three, 'one_raw_table') 
conn.commit()
cursor.close()
conn.close()
#___________________________________________________________________________________________________________________________________
#Запрос с ALL_AFN
response = requests.get(url, params={'access_key': "1731467954af1a70db43b92e62e2e131",
                                'source': "ALL",
                                'currencies': "AFN",
                                'start_date':"2023-10-01",
                                'end_date':"2023-10-31",
                                'format': 1})
data = response.json()


conn = ps2.connect(host=pg_hostname,
                   port=pg_port,
                   user=pg_username,
                   password=pg_pass,
                   database=pg_db)
cursor = conn.cursor()
#таблица ALL_AFN
cursor.execute(f"""CREATE TABLE IF NOT EXISTS rates_month_ALL_AFN (
              id serial primary key,
              dates date,
              source varchar(255),
              currencies varchar(255),
              rate double precision);""")
conn.commit()

for date, quotes in data['quotes'].items():
    for currency, rate in quotes.items():
        insert_query = f"""
        INSERT INTO rates_month_ALL_AFN(dates, source, currencies, rate) VALUES
        ('{date}', '{data['source']}', '{currency}', {rate})
        """
        cursor.execute(insert_query)
conn.commit()

select_query = "SELECT dates, source, currencies, rate FROM rates_month_ALL_AFN"
cursor.execute(select_query)
conn.commit()
cursor.close()
conn.close()
#выполнение аналитики
conn = ps2.connect(host=pg_hostname,
                   port=pg_port,
                   user=pg_username,
                   password=pg_pass,
                   database=pg_db)

query = "SELECT dates, source, currencies, rate FROM rates_month_ALL_AFN"
df = pd.read_sql_query(query, conn)
conn.close()
#день с максимальным курсом
max_rate_day = df[df['rate'] == df['rate'].max()]['dates'].values[0]
#день с минимальным курсом
min_rate_day = df[df['rate'] == df['rate'].min()]['dates'].values[0]


# Максимальное значение курса
max_rate = df['rate'].max()


# Минимальное значение курса
min_rate = df['rate'].min()


# Среднее значение курса за весь месяц
avg_rate = df['rate'].mean()


# Значение курса на последний день месяца
last_day_rate = df.sort_values(by='dates', ascending=False)['rate'].values[0]

#месяц в котором проходит действо
month =df.sort_values(by='dates', ascending=True)['dates'].values[0].month
#замутим датасет из всего этого
four = pd.DataFrame({'MAX_rate_day': max_rate_day,'MIN_rate_day': min_rate_day,
                       'MAX_rate': max_rate,'MIN_rate': min_rate,
                       'AVG_rate': avg_rate,'LAST_DAY_rate': last_day_rate,
                       'Month': month,'Currencies': currency,},index=[0])
conn = ps2.connect(host=pg_hostname,
                   port=pg_port,
                   user=pg_username,
                   password=pg_pass,
                   database=pg_db)


def execute_values(conn, df, table): 
  
    tuples = [tuple(x) for x in df.to_numpy()] 
  
    cols = ','.join(list(df.columns)) 
    
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols) 
    cursor = conn.cursor() 
    try: 
        extras.execute_values(cursor, query, tuples) 
        conn.commit() 
    except (Exception, ps2.DatabaseError) as error: 
        print("Error: %s" % error) 
        conn.rollback() 
        cursor.close() 
        return 1
    print("the dataframe aell/afn is inserted") 
  

execute_values(conn, four, 'one_raw_table') 
conn.commit()
cursor.close()
conn.close()

if __name__ == '__main__':
    print('Application started')