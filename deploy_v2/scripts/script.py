import os
import psycopg2
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import decimal


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

    # Tabelle 'films_per_store' im Keyspace erstellen
    nosql_command = """
        CREATE TABLE IF NOT EXISTS films_per_store (
        store_id int,
        film_id int,
        PRIMARY KEY (store_id, film_id)
    );"""
    cs_session.execute(nosql_command)

    # store_id und film_id aus der 'inventory' Tabelle in Postgres selektieren
    sql_command = 'SELECT store_id, film_id FROM inventory;'
    data = execute_sql(pg_conn, sql_command)

    # Blank Insert Kommando für Cassandra Tabelle 'films_per_store'
    nosql_command = """
    INSERT INTO films_per_store (
        store_id, film_id
    ) VALUES (?, ?)
    """
    insert_statement = cs_session.prepare(nosql_command)

    # Jede Reihe aus der Postgres 'inventory' Tabelle in die Cassandra 'films_per_store' Tabelle kopieren
    for row in data:
        cs_session.execute(insert_statement, row)

    #---------------------------------------AUFGABEN---------------------------------------

    print("Aufgabe 4.a) Gesamtanzahl der verfügbaren Filme:")
    nosql_command = "SELECT COUNT(*) FROM film;"
    result = cs_session.execute(nosql_command)
    print(result.one()[0])

    print("Aufgabe 4.b) Anzahl der unterschiedlichen Filme je Standort")
    result = cs_session.execute("SELECT store_id, film_id FROM films_per_store;")
    store_counts = {}
    for row in result:
        store_counts[row.store_id] = store_counts.get(row.store_id, 0) + 1
    for store_id, count in store_counts.items():
        print(f"Store ID {store_id}: {count} unique films")

    # Create Keyspace (if not exists)
    cs_session.execute("""
    CREATE KEYSPACE IF NOT EXISTS dvdrental
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
    """)

    # Connect to the new keyspace
    cs_session.set_keyspace("dvdrental")

    # Create a table
    cs_session.execute("""
    CREATE TABLE IF NOT EXISTS users (
        user_id UUID PRIMARY KEY,
        name text,
        email text
    )
    """)
    #print("Cassandra setup complete. Keyspace and table created.")

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
    #----------------------------Verbindung zur Postgres herstellen-----------------------------------
   
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
    # Ensure resources are released at the end
    # Am Ende Verbindung zu Postgres und Cassandra trennen
    pg_conn.close()
    cs_cluster.shutdown()
    pg_conn.close()