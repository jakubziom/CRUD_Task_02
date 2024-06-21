import csv
import sqlite3
from sqlite3 import Error

def create_connection(db_file):
    conn=None
    try:
        conn = sqlite3.connect(db_file)
        #print(f"Połączono z bazą danych {db_file}")
    except Error as e:
        print(e)   
    return conn

#tworzenie pliku z bazą danych
if __name__ == '__main__':
    create_connection(r"clean_stations.db")

#lista stacji
station=[]
#dane dot. poszczególnych stacji
latitude=[]
longitude=[]
elevation=[]
name=[]
country=[]
state=[]

#wczytuje dane z pierwszego pliku csv
with open('clean_stations.csv', newline='') as csvfile:
    reader = csv.reader(csvfile)
    for row in reader:
        station.append(row[0])
        latitude.append(row[1])
        longitude.append(row[2])
        elevation.append(row[3])
        name.append(row[4])
        country.append(row[5])
        state.append(row[6])

print(station)
print(latitude)
print(longitude)
print(elevation)
print(name)
print(country)
print(state)

#tworzenie tabeli na dane poszczególnych stacji
conn = create_connection("clean_stations.db")
cur= conn.cursor()
cur.execute(f'''CREATE TABLE IF NOT EXISTS 'clean_stations' (
    id integer PRIMARY KEY,
    '{station[0]}' text NOT NULL,
    '{latitude[0]}' text NOT NULL,
    '{longitude[0]}' text NOT NULL,
    '{elevation[0]}' text NOT NULL,
    '{name[0]}' text NOT NULL,
    '{country[0]}' text NOT NULL,
    '{state[0]}' text NOT NULL
    );
    ''')
conn.close

#dodaje to tabeli dane dot. poszczególnych stacji
def add_accountData(conn,clean_stations):
    sql= '''INSERT INTO clean_stations(station, latitude, longitude, elevation, name, country, state)
        VALUES (?,?,?,?,?,?,?);'''
    cur = conn.cursor()
    cur.execute(sql,clean_stations)
    conn.commit()
    return cur.lastrowid

for i in range(1,len(station)):
    conn = create_connection("clean_stations.db")
    #krotka z danymi do wprowadzenia do tabeli
    clean_stations=(station[i],latitude[i],longitude[i],elevation[i],name[i],country[i],state[i])
    pr_id = add_accountData(conn,clean_stations)

conn.close
#to sprawdza z której stacji są dane
station2=[]
#data pomiaru
date=[]
#dane pomiaru
precib=[]
tobs=[]

print('===============================')
#wszytuje dane z drugiego pliku csv
with open('clean_measure.csv', newline='') as csvfile:
    reader = csv.reader(csvfile)
    for row in reader:
        station2.append(row[0])
        date.append(row[1])
        precib.append(row[2])
        tobs.append(row[3])

#tworzy tabelę na dane pomiarów dla poszczególnych stacji
conn = create_connection("clean_stations.db")
cur= conn.cursor()
for i in range(0,len(station)):
    cur.execute(f'''
    CREATE TABLE IF NOT EXISTS {station[i]} (
        id integer PRIMARY KEY,
        {station[i]}_id integer NOT NULL,
        {date[0]} text NOT NULL,
        {precib[0]} text NOT NULL,
        {tobs[0]} text NOT NULL,
        FOREIGN KEY ({station[i]}_id) REFERENCES clean_stations (id)
        );
        ''')
conn.close

def add_Data(conn,clean_stations,stationNumber):
    sql= f'''INSERT INTO {stationNumber}({stationNumber}_id, date, precip, tobs)
        VALUES (?,?,?,?);'''
    cur = conn.cursor()
    cur.execute(sql,clean_stations)
    conn.commit()
    return cur.lastrowid

#dodaje dane pomiarów do poszczególnych tabel stacji
for stationName in station:
    for i in range(0,len(station2)):
        if station2[i]==stationName:
            conn = create_connection("clean_stations.db")
            #krotka z danymi do wprowadzenia do tabeli
            clean_stations=(1, date[i],precib[i],tobs[i])
            pr_id = add_Data(conn,clean_stations,stationName)

#usuwa pustą tabelę
conn = create_connection("clean_stations.db")
cur= conn.cursor()
cur.execute(f"DROP TABLE station")
conn.commit()   
conn.close 



    