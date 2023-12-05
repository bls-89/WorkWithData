
from datetime import datetime
def ts_to_date(x):
    return datetime.utcfromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S')
import requests
import psycopg2 as ps2

url = f"http://api.exchangerate.host/live"

response = requests.get(url, params={'access_key': "1731467954af1a70db43b92e62e2e131",
                                    'source': "BTC", 
                                    'currencies': "RUB", 
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
cursor.execute(f"""CREATE TABLE IF NOT EXISTS rates (
                id serial primary key,
                timestamp timestamp,
                source varchar(255),
                currencies varchar(255),
                rate double precision);""")
conn.commit()

insert_query = f"""INSERT INTO rates(timestamp, source, currencies, rate) VALUES{ts_to_date(data['timestamp']), data['source'], 'RUB', data['quotes']['BTCRUB']}"""

cursor.execute(insert_query)
conn.commit()

select_query = "SELECT timestamp, source, currencies, rate FROM rates"
cursor.execute(select_query)
conn.commit()
res = cursor.fetchall()

print(res)
cursor.close()
conn.close()

if __name__ == '__main__':
    print('Application started')