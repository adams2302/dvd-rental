import os
import psycopg2
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import decimal
from collections import Counter, defaultdict
import uuid

# Hilfsfunktion zur Umwandlung von int zu UUID
def int_to_uuid(i):
    return uuid.UUID(int=i)

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
    try:
        cur = conn.cursor()
        cur.execute(command)
        rows = cur.fetchall()
        cur.close()
        return rows
    except Exception as e:
        print(f"Fehler bei der Ausführung des SQL-Kommandos: {e}")
        return []
#--------------Funktion um mit staff_id ein neues Passwort zu vergeben-----------------------    
def update_password(staff_id, new_password):
    query = "UPDATE staff SET password = %s WHERE staff_id = %s"
    cs_session.execute(query, (new_password, staff_id))
    print(f"Passwort für staff_id {staff_id} wurde erfolgreich aktualisiert.") 

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
        inventory_id int,
        store_id int,
        film_id int,
        PRIMARY KEY (store_id, film_id)
    );"""
    cs_session.execute(nosql_command)

    # store_id und film_id aus der 'inventory' Tabelle in Postgres selektieren
    sql_command = 'SELECT inventory_id, store_id, film_id FROM inventory;'
    data = execute_sql(pg_conn, sql_command)

    # Blank Insert Kommando für Cassandra Tabelle 'inventory'
    nosql_command = """
    INSERT INTO inventory (
        inventory_id, store_id, film_id
    ) VALUES (?, ?, ?)
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
        inventory_id int,
        customer_id int,
        staff_id int,
        PRIMARY KEY (rental_id)
    );"""
    cs_session.execute(nosql_command)

    # rental_id, customer_id und staff_id aus der 'rental' Tabelle in Postgres selektieren
    sql_command = 'SELECT rental_id, inventory_id, customer_id, staff_id FROM rental;'
    data = execute_sql(pg_conn, sql_command)

    # Blank Insert Kommando für Cassandra Tabelle 'rental'
    nosql_command = """
    INSERT INTO rental (
        rental_id, inventory_id, customer_id, staff_id
    ) VALUES (?, ?, ?, ?)
    """
    insert_statement = cs_session.prepare(nosql_command)

    # Jede Reihe aus der Postgres 'rental' Tabelle in die Cassandra 'rental' Tabelle kopieren
    for row in data:
        cs_session.execute(insert_statement, row)

    #---------------------------------------FILM ACTOR---------------------------------------

    # Tabelle 'film_actor' im Keyspace erstellen
    nosql_command = """
    CREATE TABLE IF NOT EXISTS film_actor (
        actor_id UUID,
        film_id UUID,
        PRIMARY KEY (actor_id, film_id)
    );"""
    cs_session.execute(nosql_command)

    # actor_id und film_id aus der 'film_actor' Tabelle in Postgres selektieren
    sql_command = 'SELECT actor_id, film_id FROM film_actor;'
    data = execute_sql(pg_conn, sql_command)

    # Blank Insert Kommando für Cassandra Tabelle 'film_actor'
    nosql_command = """
    INSERT INTO film_actor (
        actor_id, film_id
    ) VALUES (?, ?)
    """
    insert_statement = cs_session.prepare(nosql_command)

    # Jede Reihe aus der Postgres 'film_actor' Tabelle in die Cassandra 'film_actor' Tabelle kopieren
    for row in data:
        actor_id = row[0]  # Angenommener int-Wert
        film_id = row[1]   # Angenommener int-Wert

        # Wenn UUID erwartet wird, aber int vorhanden ist, UUID erstellen
        actor_id = int_to_uuid(actor_id)  # Umwandeln von int zu UUID
        film_id = int_to_uuid(film_id)    # Umwandeln von int zu UUID

        cs_session.execute(insert_statement, (actor_id, film_id))

     #---------------------------------------STAFF---------------------------------------

    # Tabelle 'rental' im Keyspace erstellen
    nosql_command = """
        CREATE TABLE IF NOT EXISTS staff (
        staff_id int,
        first_name text,
        last_name text,
        store_id int,
        username text,
        password text,
        PRIMARY KEY (staff_id)
    );"""
    cs_session.execute(nosql_command)

    # rental_id, customer_id und staff_id aus der 'staff' Tabelle in Postgres selektieren
    sql_command = 'SELECT staff_id, first_name, last_name, store_id, username, password FROM staff;'
    data = execute_sql(pg_conn, sql_command)

    # Blank Insert Kommando für Cassandra Tabelle 'staff'
    nosql_command = """
    INSERT INTO staff (
        staff_id, first_name, last_name, store_id, username, password
    ) VALUES (?, ?, ?, ?, ?, ?)
    """
    insert_statement = cs_session.prepare(nosql_command)

    # Jede Reihe aus der Postgres 'staff' Tabelle in die Cassandra 'staff' Tabelle kopieren
    for row in data:
        cs_session.execute(insert_statement, row)


    #---------------------------------------PAYMENT---------------------------------------

    # Tabelle 'rental' im Keyspace erstellen
    nosql_command = """
        CREATE TABLE IF NOT EXISTS payment (
        payment_id int,
        customer_id int,
        staff_id int,
        amount float,
        PRIMARY KEY (payment_id)
    );"""
    cs_session.execute(nosql_command)

    # rental_id, customer_id und staff_id aus der 'payment' Tabelle in Postgres selektieren
    sql_command = 'SELECT payment_id, customer_id, staff_id, amount FROM payment;'
    data = execute_sql(pg_conn, sql_command)

    # Blank Insert Kommando für Cassandra Tabelle 'payment'
    nosql_command = """
    INSERT INTO payment (
        payment_id, customer_id, staff_id, amount
    ) VALUES (?, ?, ?, ?)
    """
    insert_statement = cs_session.prepare(nosql_command)

    # Jede Reihe aus der Postgres 'payment' Tabelle in die Cassandra 'payment' Tabelle kopieren
    for row in data:
        cs_session.execute(insert_statement, row)

    #---------------------------------------CUSTOMER---------------------------------------

    # Tabelle 'customer' im Keyspace erstellen
    nosql_command = """
        CREATE TABLE IF NOT EXISTS customer (
        customer_id int,
        store_id int,
        first_name text,
        last_name text,
        address_id int,
        activebool boolean,
        PRIMARY KEY (customer_id)
    );"""
    cs_session.execute(nosql_command)

    # rental_id, customer_id und staff_id aus der 'customer' Tabelle in Postgres selektieren
    sql_command = 'SELECT customer_id, store_id, first_name, last_name, address_id, activebool FROM customer;'
    data = execute_sql(pg_conn, sql_command)

    # Blank Insert Kommando für Cassandra Tabelle 'customer'
    nosql_command = """
    INSERT INTO customer (
        customer_id, store_id, first_name, last_name, address_id, activebool
    ) VALUES (?, ?, ?, ?, ?, ?)
    """
    insert_statement = cs_session.prepare(nosql_command)

    # Jede Reihe aus der Postgres 'customer' Tabelle in die Cassandra 'customer' Tabelle kopieren
    for row in data:
        cs_session.execute(insert_statement, row)

    #---------------------------------------CITY---------------------------------------

    # Tabelle 'city' im Keyspace erstellen
    nosql_command = """
        CREATE TABLE IF NOT EXISTS city (
        city_id int,
        city text,
        country_id int,
        PRIMARY KEY (city_id)
    );"""
    cs_session.execute(nosql_command)

    # rental_id, customer_id und staff_id aus der 'city' Tabelle in Postgres selektieren
    sql_command = 'SELECT city_id, city, country_id FROM city;'
    data = execute_sql(pg_conn, sql_command)

    # Blank Insert Kommando für Cassandra Tabelle 'city'
    nosql_command = """
    INSERT INTO city (
        city_id, city, country_id
    ) VALUES (?, ?, ?)
    """
    insert_statement = cs_session.prepare(nosql_command)

    # Jede Reihe aus der Postgres 'city' Tabelle in die Cassandra 'city' Tabelle kopieren
    for row in data:
        cs_session.execute(insert_statement, row)

    #---------------------------------------COUNTRY---------------------------------------

    # Tabelle 'country' im Keyspace erstellen
    nosql_command = """
        CREATE TABLE IF NOT EXISTS country (
        country_id int,
        country text,
        PRIMARY KEY (country_id)
    );"""
    cs_session.execute(nosql_command)

    # rental_id, customer_id und staff_id aus der 'country' Tabelle in Postgres selektieren
    sql_command = 'SELECT country_id, country FROM country;'
    data = execute_sql(pg_conn, sql_command)

    # Blank Insert Kommando für Cassandra Tabelle 'country'
    nosql_command = """
    INSERT INTO country (
        country_id, country
    ) VALUES (?, ?)
    """
    insert_statement = cs_session.prepare(nosql_command)

    # Jede Reihe aus der Postgres 'country' Tabelle in die Cassandra 'country' Tabelle kopieren
    for row in data:
        cs_session.execute(insert_statement, row)

    #---------------------------------------AUFGABEN---------------------------------------

    #------Aufgabe 4.a
    print()
    print("Aufgabe 4.a) Gesamtanzahl der verfügbaren Filme:")
    nosql_command = "SELECT COUNT(*) FROM film;"
    result = cs_session.execute(nosql_command)
    print(result.one()[0])

    #------Aufgabe 4.b
    print()
    print("Aufgabe 4.b) Anzahl der unterschiedlichen Filme je Standort:")
    result = cs_session.execute("SELECT store_id, film_id FROM inventory;")
    store_counts = {}
    for row in result:
        store_counts[row.store_id] = store_counts.get(row.store_id, 0) + 1
    for store_id, count in store_counts.items():
        print(f"Store ID {store_id}: {count} unique films")

    #------Aufgabe 4.c
    print()
    print("Aufgabe 4.c) Die Vor- und Nachnamen der 10 Schauspieler mit den meisten Filmen:")
    # Zunächst alle Einträge aus der 'film_actor' Tabelle abfragen
    nosql_command = "SELECT actor_id, film_id FROM film_actor;"
    result = cs_session.execute(nosql_command)

    # Zähle die Anzahl der Filme pro Schauspieler
    actor_film_counts = Counter(row.actor_id for row in result)

    # Top 10 Schauspieler nach Filmanzahl ermitteln
    top_actor_ids = [actor_id for actor_id, _ in actor_film_counts.most_common(10)]

    # Überprüfe und konvertiere die actor_ids in UUID, falls sie noch nicht als UUID vorliegen
    top_actor_ids = [uuid.UUID(str(actor_id)) if isinstance(actor_id, str) else actor_id for actor_id in top_actor_ids]

    # Stelle sicher, dass Platzhalter richtig gesetzt sind (Anzahl der Platzhalter muss mit der Länge von top_actor_ids übereinstimmen)
    placeholders = ', '.join(['?'] * len(top_actor_ids))  # Platzhalter für die IDs

    # Cassandra-Abfrage mit 'IN' und Platzhaltern
    nosql_command = f"SELECT actor_id, first_name, last_name FROM actor WHERE actor_id IN ({placeholders});"

    # Versuche die Abfrage auszuführen
    try:
        # Führe die Abfrage aus, indem du top_actor_ids als Parameter übergibst
        actors_result = cs_session.execute(nosql_command, top_actor_ids)

        # Ergebnisse kombinieren und ausgeben
        top_actors = [(f"{row.first_name} {row.last_name}", actor_film_counts[row.actor_id]) for row in actors_result]

        # Sortiere die Schauspieler nach der Anzahl der Filme in absteigender Reihenfolge
        top_actors_sorted = sorted(top_actors, key=lambda x: x[1], reverse=True)

        # Ausgabe der Schauspieler und der Anzahl der Filme
        print("Top 10 Schauspieler mit den meisten Filmen:")
        for name, count in top_actors_sorted:
            print(f"{name}: {count} Filme")

    except Exception as e:
        print(f"Fehler bei der Abfrage: {e}")

    #------Aufgabe 4.d
    print()
    print("Aufgabe 4.d) Die Erlöse je Mitarbeiter:")
    # Abfrage für Mitarbeiter
    nosql_command = "SELECT staff_id FROM staff"
    staff_rows = cs_session.execute(nosql_command)
    # Abfrage für Zahlungen
    nosql_command = "SELECT staff_id, amount FROM payment"
    payment_rows = cs_session.execute(nosql_command)
    # Umsatz pro Mitarbeiter berechnen
    revenue_by_staff = defaultdict(float)
    for row in payment_rows:
        if row.staff_id is not None:
            revenue_by_staff[row.staff_id] += row.amount
    # Ergebnisse mit den Mitarbeitern verknüpfen
    for staff in staff_rows:
        staff_id = staff.staff_id
        revenue = revenue_by_staff.get(staff_id, 0)
        print(f"Staff ID: {staff_id}, Revenue: {revenue:.2f}")

    #------Aufgabe 4.e
    print()
    print("Aufgabe 4.e) Die IDs der 10 Kunden mit den meisten Entleihungen:")
    nosql_command = "SELECT customer_id FROM rental;"
    result = cs_session.execute(nosql_command)
    rental_counts = Counter(row.customer_id for row in result)  # Replaced 'rows' with 'result'
    top_customers = rental_counts.most_common(10) # Top 10 customers based on rental count
    print("Top 10 Kunden mit den meisten Ausleihen:")
    for customer_id, count in top_customers:
        print(f"Customer ID: {customer_id}, Number of Rentals: {count}")

    #------Aufgabe 4.f
    print()
    print("Aufgabe 4.f) Die Vor- und Nachnamen sowie die Niederlassung der 10 Kunden, die das meiste Geld ausgegeben haben:")
    # Abfrage der Kundendaten
    customer_query = "SELECT customer_id, first_name, last_name, store_id FROM customer"
    customers = cs_session.execute(customer_query)
    # Abfrage der Zahlungsdaten
    payment_query = "SELECT customer_id, amount FROM payment"
    payments = cs_session.execute(payment_query)
    # Mapping der Umsätze pro Kunde
    revenue_by_customer = defaultdict(float)
    for payment in payments:
        if payment.customer_id is not None:
            revenue_by_customer[payment.customer_id] += payment.amount
    # Kunden mit Umsätzen verknüpfen
    customer_revenues = []
    for customer in customers:
        full_name = f"{customer.first_name} {customer.last_name}"
        store_id = customer.store_id
        revenue = revenue_by_customer.get(customer.customer_id, 0)
        customer_revenues.append((full_name, store_id, revenue))
    # Nach Umsatz sortieren und Top 10 auswählen
    top_customers = sorted(customer_revenues, key=lambda x: x[2], reverse=True)[:10]
    # Ergebnisse drucken
    print("Top 10 Kunden nach Umsatz:")
    for name, store, revenue in top_customers:
        print(f"Name: {name}, Store: {store}, Revenue: {revenue:.2f}")

    #------Aufgabe 4.g
    print()
    print("Aufgabe 4.g) Die 10 meistgesehenen Filme unter Angabe des Titels, absteigend sortiert:")
    # Abfrage der Filmtitel
    film_query = "SELECT film_id, title FROM film"
    films = cs_session.execute(film_query)
    # Abfrage der Inventardaten
    inventory_query = "SELECT inventory_id, film_id FROM inventory"
    inventories = cs_session.execute(inventory_query)
    # Abfrage der Mietdaten
    rental_query = "SELECT inventory_id FROM rental"
    rentals = cs_session.execute(rental_query)
    # Mapping von film_id zu title
    film_map = {film.film_id: film.title for film in films}
    # Mapping von inventory_id zu film_id
    inventory_map = {inventory.inventory_id: inventory.film_id for inventory in inventories}
    # Zählung der Vermietungen pro Film
    rental_count = defaultdict(int)
    for rental in rentals:
        film_id = inventory_map.get(rental.inventory_id)
        if film_id is not None:
            rental_count[film_id] += 1
    # Verknüpfung mit den Filmtiteln
    film_rentals = [(film_map[film_id], count) for film_id, count in rental_count.items() if film_id in film_map]

    # Sortieren nach Anzahl der Vermietungen und Begrenzung auf die Top 10
    top_rented_films = sorted(film_rentals, key=lambda x: x[1], reverse=True)[:10]

    # Ergebnisse drucken
    print("Top 10 meistvermietete Filme:")
    for title, count in top_rented_films:
        print(f"Title: {title}, Rentals: {count}")

    #------Aufgabe 5.a
    print()
    print("Aufgabe 5.a) Vergabe eines neuen, sicheren Passworts für alle Mitarbeiter:")
    # Nutzen der eingangs erstellen Funktion zum Updaten eines Passworts
    update_password(1, 'DeinNeuesSicheresPasswort123!')
    update_password(2, 'DeinNeuesSicheresPasswort321!')
    #Ausgeben der neuen Passwörter
    print()
    print("Ausgabe der neuen Passwörter:")
    nosql_command = "SELECT staff_id, password FROM staff"
    result = cs_session.execute(nosql_command)
    for row in result:
        print(f"Staff ID: {row.staff_id}, Password: {row.password}")

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

    # Die Hauptfunktion ausführen
    main()
    
except Exception as e:
    print(f"Fehler aufgetreten: {e}")

finally:
    # Verbindung zur PostgreSQL und Cassandra DB schließen
    pg_conn.close()
    cs_cluster.shutdown()
