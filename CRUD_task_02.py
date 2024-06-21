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
    create_connection(r"stations.db")

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


#tworzenie tabeli na dane poszczególnych stacji
conn = create_connection("stations.db")
cur= conn.cursor()
cur.execute(f'''CREATE TABLE IF NOT EXISTS 'stations' (
    id integer PRIMARY KEY,
    '{station[0]}' text NOT NULL,
    '{latitude[0]}' float NOT NULL,
    '{longitude[0]}' float NOT NULL,
    '{elevation[0]}' float NOT NULL,
    '{name[0]}' text NOT NULL,
    '{country[0]}' text NOT NULL,
    '{state[0]}' text NOT NULL
    );
    ''')
conn.close

#dodaje to tabeli dane dot. poszczególnych stacji
def add_stationData(conn,stations):
    sql= '''INSERT INTO stations(station, latitude, longitude, elevation, name, country, state)
        VALUES (?,?,?,?,?,?,?);'''
    cur = conn.cursor()
    cur.execute(sql,stations)
    conn.commit()
    return cur.lastrowid

conn = create_connection("stations.db")
for i in range(1,len(station)):
    #krotka z danymi do wprowadzenia do tabeli
    stations=(station[i],latitude[i],longitude[i],elevation[i],name[i],country[i],state[i])
    pr_id = add_stationData(conn,stations)

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
conn = create_connection("stations.db")
cur= conn.cursor()
for i in range(0,len(station)):
    cur.execute(f'''
    CREATE TABLE IF NOT EXISTS {station[i]} (
        id integer PRIMARY KEY,
        {station[i]}_id integer NOT NULL,
        {date[0]} text NOT NULL,
        {precib[0]} float NOT NULL,
        {tobs[0]} integer NOT NULL,
        FOREIGN KEY ({station[i]}_id) REFERENCES stations (id)
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
conn = create_connection("stations.db")
for stationNumber in station:
    for i in range(0,len(station2)):
        if station2[i]==stationNumber:
            #krotka z danymi do wprowadzenia do tabeli
            clean_stations=(1, date[i],precib[i],tobs[i])
            pr_id = add_Data(conn,clean_stations,stationNumber)

#usuwa pustą tabelę
conn = create_connection("stations.db")
cur= conn.cursor()
cur.execute(f"DROP TABLE station")
conn.commit()   
conn.close 



    