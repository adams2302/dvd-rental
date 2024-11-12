import os
import psycopg2
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import decimal
from collections import Counter

def main():
    #---------------------------------------FILM---------------------------------------

    # Tabelle 'film' im Keyspace erstellen
    nosql_command = """
    CREATE TABLE IF NOT EXISTS film (
        film_id int PRIMARY KEY,
        title text,
        description text,
        release_year int,
        language_id int,
        rental_duration int,
        rental_rate float,
        length int,
        replacement_cost float,
        rating text,
        last_update timestamp,
        special_features list<text>,
        fulltext text
    );"""
    cs_session.execute(nosql_command)

    # Alles aus der 'film' Tabelle in Postgres selektieren
    sql_command = 'SELECT * FROM film;'
    data = execute_sql(pg_conn, sql_command)

    # Blank Insert Kommando für Cassandra Tabelle 'film'
    nosql_command = """
    INSERT INTO film (
        film_id, title, description, release_year, language_id, rental_duration,
        rental_rate, length, replacement_cost, rating, last_update, special_features, fulltext
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    insert_statement = cs_session.prepare(nosql_command)

    # Jede Reihe aus der Postgres 'film' Tabelle in die Cassandra 'film' Tabelle kopieren
    for row in data:
        row = list(row)
        row[6] = float(row[6]) if isinstance(row[6], decimal.Decimal) else row[6]
        row[8] = float(row[8]) if isinstance(row[8], decimal.Decimal) else row[8]
        cs_session.execute(insert_statement, row)

    #---------------------------------------INVENTORY---------------------------------------

    # Tabelle 'inventory' im Keyspace erstellen
    nosql_command = """
        CREATE TABLE IF NOT EXISTS inventory (
        store_id int,
        film_id int,
        PRIMARY KEY (store_id, film_id)
    );"""
    cs_session.execute(nosql_command)

    # store_id und film_id aus der 'inventory' Tabelle in Postgres selektieren
    sql_command = 'SELECT store_id, film_id FROM inventory;'
    data = execute_sql(pg_conn, sql_command)

    # Blank Insert Kommando für Cassandra Tabelle 'inventory'
    nosql_command = """
    INSERT INTO inventory (
        store_id, film_id
    ) VALUES (?, ?)
    """
    insert_statement = cs_session.prepare(nosql_command)

    # Jede Reihe aus der Postgres 'inventory' Tabelle in die Cassandra 'inventory' Tabelle kopieren
    for row in data:
        cs_session.execute(insert_statement, row)

    #---------------------------------------RENTAL---------------------------------------

    # Tabelle 'rental' im Keyspace erstellen
    nosql_command = """
        CREATE TABLE IF NOT EXISTS rental (
        rental_id int,
        customer_id int,
        staff_id int,
        PRIMARY KEY (rental_id)
    );"""
    cs_session.execute(nosql_command)

    # srental_id, customer_id und staff_id aus der 'rental' Tabelle in Postgres selektieren
    sql_command = 'SELECT rental_id, customer_id, staff_id FROM rental;'
    data = execute_sql(pg_conn, sql_command)

    # Blank Insert Kommando für Cassandra Tabelle 'rental'
    nosql_command = """
    INSERT INTO rental (
        rental_id, customer_id, staff_id
    ) VALUES (?, ?, ?)
    """
    insert_statement = cs_session.prepare(nosql_command)

    # Jede Reihe aus der Postgres 'rental' Tabelle in die Cassandra 'rental' Tabelle kopieren
    for row in data:
        cs_session.execute(insert_statement, row)

    #---------------------------------------AUFGABEN---------------------------------------

    print("Aufgabe 4.a) Gesamtanzahl der verfügbaren Filme:")
    nosql_command = "SELECT COUNT(*) FROM film;"
    result = cs_session.execute(nosql_command)
    print(result.one()[0])

    print("Aufgabe 4.b) Anzahl der unterschiedlichen Filme je Standort:")
    result = cs_session.execute("SELECT store_id, film_id FROM inventory;")
    store_counts = {}
    for row in result:
        store_counts[row.store_id] = store_counts.get(row.store_id, 0) + 1
    for store_id, count in store_counts.items():
        print(f"Store ID {store_id}: {count} unique films")

    #print("Aufgabe 4.e) Die IDs der 10 Kunden mit den meisten Entleihungen:")
    #nosql_command = "SELECT customer_id FROM rental;"
    #result = cs_session.execute(nosql_command)
    # Zählung der Kunden-IDs durchführen
    #rental_counts = Counter(row.customer_id for row in rows)
    # Top 10 Kunden basierend auf der Anzahl der Ausleihen sortieren
    #top_customers = rental_counts.most_common(10)
    #print("Top 10 Kunden mit den meisten Ausleihen:")
    #for customer_id, count in top_customers:
     #   print(f"Customer ID: {customer_id}, Number of Rentals: {count}")


#--------------------------Verbindung zur Postgres DB herstellen-----------------------------------

def get_postgres_conn():

    connection = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )
    return connection

#--------------------------Verbindung zur Cassandra DB herstellen-----------------------------------

def get_cassandra_conn():

    cassandra_host = os.getenv("CASSANDRA_HOST", "cassandra")
    cluster = Cluster([cassandra_host])
    session = cluster.connect()

    return [cluster, session]

#--------------SQL Kommando auf Postgres DB ausführen und Ergebnis zurückgeben-----------------------

def execute_sql(conn, command):
    cur = conn.cursor()
    cur.execute(command)
    rows = cur.fetchall()
    cur.close()
    return rows

#--------------------------Ausfuehren des Programms-----------------------------------

try:
    print("\n--------------------------Python Script startet-----------------------------------")
    
    #Verbindung zur Postgres herstellen
    pg_conn = get_postgres_conn()

    # Cassandra verbindung herstellen
    cs_conn = get_cassandra_conn()
    cs_cluster = cs_conn[0]
    cs_session = cs_conn[1]

    # Keyspace (Datenbank) dvdrental erstellen, falls er nicht existiert
    nosql_command = "CREATE KEYSPACE IF NOT EXISTS dvdrental WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};"
    cs_session.execute(nosql_command)

    # Verbindung zum Keyspace herstellen
    cs_session.set_keyspace('dvdrental')

    main()

finally:
    # Am Ende Verbindung zu Postgres und Cassandra trennen und Ressourcen freigeben
    pg_conn.close()
    cs_cluster.shutdown()
    pg_conn.close()