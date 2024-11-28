import os
import psycopg2
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import decimal
from collections import Counter, defaultdict
import uuid
import time

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
    nosql_command = "UPDATE staff SET password = %s WHERE staff_id = %s"
    cs_session.execute(nosql_command, (new_password, staff_id))
    print(f"Passwort für staff_id {staff_id} wurde erfolgreich aktualisiert.") 

def main():
    # bereits in Postgres existierende Tabellen
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
        PRIMARY KEY (inventory_id)
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

    #---------------------------------------------ACTOR---------------------------------------

    # Tabelle 'actor' im Keyspace erstellen
    nosql_command = """
    CREATE TABLE IF NOT EXISTS actor (
        actor_id UUID PRIMARY KEY,
        first_name text,
        last_name text,
        last_update timestamp
    );"""

    cs_session.execute(nosql_command)

    # actor_id und name aus der 'actor' Tabelle in Postgres selektieren
    sql_command = 'SELECT actor_id, first_name, last_name, last_update FROM actor;'
    data = execute_sql(pg_conn, sql_command)

    # Blank Insert Kommando für Cassandra Tabelle 'film_actor'
    nosql_command = """
    INSERT INTO actor (
        actor_id, first_name, last_name, last_update
    ) VALUES (?, ?, ?, ?)
    """
    insert_statement = cs_session.prepare(nosql_command)

    # Jede Reihe aus der Postgres 'actor' Tabelle in die Cassandra 'film_actor' Tabelle kopieren
    for row in data:
        actor_id = row[0]  # Angenommener int-Wert
        first_name = row[1]   # Angenommener int-Wert
        last_name = row[2]   # Angenommener int-Wert
        last_update = row[3]   # Angenommener int-Wert

        # Wenn UUID erwartet wird, aber int vorhanden ist, UUID erstellen
        actor_id = int_to_uuid(actor_id)  # Umwandeln von int zu UUID

        cs_session.execute(insert_statement, (actor_id, first_name, last_name, last_update))

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

    #---------------------------------------CUSTOMER-List---------------------------------------

    # Tabelle 'customer_list' im Keyspace erstellen
    nosql_command = """
        CREATE TABLE IF NOT EXISTS customer_list (
        id int,
        name text,
        address varchar,
        postal_code varchar,
        phone varchar,
        city varchar,
        country varchar,
        notes text,
        sid int,
        PRIMARY KEY (id)
    );"""
    cs_session.execute(nosql_command)

    # rental_id, customer_id und staff_id aus der 'customer' Tabelle in Postgres selektieren
    sql_command = """SELECT cu.customer_id AS id,
        (((cu.first_name)::text || ' '::text) || (cu.last_name)::text) AS name,
        a.address,
        a.postal_code AS "zip code",
        a.phone,
        city.city,
        country.country,
            CASE
                WHEN cu.activebool THEN 'active'::text
                ELSE ''::text
            END AS notes,
        cu.store_id AS sid
        FROM (((customer cu
            JOIN address a ON ((cu.address_id = a.address_id)))
            JOIN city ON ((a.city_id = city.city_id)))
            JOIN country ON ((city.country_id = country.country_id)));"""

    data = execute_sql(pg_conn, sql_command)

    # Blank Insert Kommando für Cassandra Tabelle 'customer'
    nosql_command = """
    INSERT INTO customer_list (
        id, name, address, postal_code, phone, city, country, notes, sid
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    insert_statement = cs_session.prepare(nosql_command)

    # Jede Reihe aus der Postgres 'customer' Tabelle in die Cassandra 'customer' Tabelle kopieren
    for row in data:
        cs_session.execute(insert_statement, row)

    #---------------------------------------CITY---------------------------------------

    # Tabelle 'city' im Keyspace erstellen
    nosql_command = """
        CREATE TABLE IF NOT EXISTS city (
        city_id int PRIMARY KEY,
        city text,
        country_id int
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
        city_id = row[0]  # Angenommener int-Wert
        city = row[1]   
        country_id = row[2]  

        cs_session.execute(insert_statement, (city_id, city, country_id))


    #---------------------------------------COUNTRY---------------------------------------

    # Tabelle 'country' im Keyspace erstellen
    nosql_command = """
        CREATE TABLE IF NOT EXISTS country (
        country_id int PRIMARY KEY,
        country text
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
        country_id = row[0]  # Angenommener int-Wert
        country = row[1]    
        
        cs_session.execute(insert_statement, (country_id, country))

    #---------------------------------------ADDRESS---------------------------------------

    # Tabelle 'address' im Keyspace erstellen
    nosql_command = """
        CREATE TABLE IF NOT EXISTS address (
        address_id int,
        address text,
        district text,
        city_id int,
        postal_code text,
        PRIMARY KEY (address_id)
    );"""
    cs_session.execute(nosql_command)

    # Daten aus der 'address' Tabelle in Postgres selektieren
    sql_command = 'SELECT address_id, address, district, city_id, postal_code FROM address;'
    data = execute_sql(pg_conn, sql_command)

    # Blank Insert Kommando für Cassandra Tabelle 'address'
    nosql_command = """
    INSERT INTO address (
        address_id, address, district, city_id, postal_code
    ) VALUES (?, ?, ?, ?, ?)
    """
    insert_statement = cs_session.prepare(nosql_command)

    # Jede Reihe aus der Postgres 'address' Tabelle in die Cassandra 'address' Tabelle kopieren
    for row in data:
        cs_session.execute(insert_statement, row)

    #---------------------------------------STORE---------------------------------------

    # Tabelle 'store' im Keyspace erstellen
    nosql_command = """
        CREATE TABLE IF NOT EXISTS store (
        store_id int PRIMARY KEY,
        manager_staff_id int,
        address_id int
    );"""
    cs_session.execute(nosql_command)

    # store_id, manager_staff_id, address_id aus der 'store' Tabelle in Postgres selektieren
    sql_command = 'SELECT store_id, manager_staff_id, address_id FROM store;'
    data = execute_sql(pg_conn, sql_command)

    # Blank Insert Kommando für Cassandra Tabelle 'store'
    nosql_command = """
    INSERT INTO store (
        store_id, manager_staff_id, address_id
    ) VALUES (?, ?, ?)
    """
    insert_statement = cs_session.prepare(nosql_command)

    # Jede Reihe aus der Postgres 'store' Tabelle in die Cassandra 'store' Tabelle kopieren
    for row in data:
        store_id = row[0]  # Angenommener int-Wert
        manager_staff_id = row[1]   
        address_id = row[2] 

        #store_id = int_to_uuid(store_id)
        
        cs_session.execute(insert_statement, (store_id, manager_staff_id, address_id))

    #---------------------------------------Film Count---------------------------------------
    
    # Erstellt die Tabelle film_rental_count_by_title.
    nosql_command = """
    CREATE TABLE IF NOT EXISTS film_rental_count_by_title (
        title TEXT PRIMARY KEY,
        rental_count INT
    );
    """
    cs_session.execute(nosql_command)
    
    # title und rental_count aus Postgres selektieren
    sql_command = """
    SELECT film.title AS title, COUNT(rental.rental_id) AS rental_count
    FROM rental
    JOIN inventory ON rental.inventory_id = inventory.inventory_id
    JOIN film ON inventory.film_id = film.film_id
    GROUP BY film.title;
    """
    data = execute_sql(pg_conn, sql_command)
    
    # Blank Insert Kommando für Cassandra Tabelle 'film_rental_count_by_title'
    nosql_command = """
    INSERT INTO film_rental_count_by_title (
        title, 
        rental_count
    )VALUES (?, ?)
    """
    insert_statement = cs_session.prepare(nosql_command)

    # Jede Reihe aus der Postgres-Abfrage in die Cassandra 'film_rental_count_by_title' Tabelle kopieren
    for row in data:
        cs_session.execute(insert_statement, row)
    
    # neue Tabellen
    #---------------------------------------Category Count---------------------------------------

    # Erstellt die Tabelle category_rental_count.
    nosql_command = """
    CREATE TABLE IF NOT EXISTS category_rental_count (
        category_name TEXT PRIMARY KEY,
        rental_count INT
    );
    """
    cs_session.execute(nosql_command)

    # category_name und rental_count aus Postgres selektieren
    sql_command = """
    SELECT category.name AS category_name, COUNT(rental.rental_id) AS rental_count
    FROM rental
    JOIN inventory ON rental.inventory_id = inventory.inventory_id
    JOIN film_category ON inventory.film_id = film_category.film_id
    JOIN category ON film_category.category_id = category.category_id
    GROUP BY category.name;
    """
    data = execute_sql(pg_conn, sql_command)
    
    # Blank Insert Kommando für Cassandra Tabelle 'category_rental_count'
    nosql_command = """
    INSERT INTO category_rental_count (category_name, rental_count)
    VALUES (?, ?)
    """
    insert_statement = cs_session.prepare(nosql_command)

    for row in data:
        cs_session.execute(insert_statement, row)

    # Abfragen
    #---------------------------------------AUFGABEN---------------------------------------

    #------Aufgabe 4.a
    print()
    print("Aufgabe 4.a) Gesamtanzahl der verfügbaren Filme:")
    nosql_command = "SELECT * FROM film;"
    rows = cs_session.execute(nosql_command)
    
    # Anzahl der film_id-Einträge berechnen
    count = sum(1 for _ in rows)  # Zählt alle Zeilen

    #Print
    print(f"Anzahl der Filme: {count}")

    #------Aufgabe 4.b
    print()
    print("Aufgabe 4.b) Anzahl der unterschiedlichen Filme je Standort:")
    # Datenabfrage definieren und ausführen
    rows = cs_session.execute("SELECT store_id, film_id FROM inventory;")
    # Daten der Tabelle in Python laden und in Liste "store_films" die Anzahl der Filme je Store zählen
    store_films = {}
    for row in rows:
        store_id = row.store_id
        film_id = row.film_id
        if store_id not in store_films:
            store_films[store_id] = set()
        store_films[store_id].add(film_id)
    # Anzahl der Filme je Store ausgeben
    for store_id, films in store_films.items():
        print(f"Store ID: {store_id}, Anzahl unterschiedlicher Filme: {len(films)}")

    #------Aufgabe 4.c
    print()
    print("Aufgabe 4.c) Vor- und Nachnamen der 10 Schauspieler mit den meisten Filmen, absteigend sortiert:")
    # Daten aus 'film_actor' abrufen
    film_actor_data = cs_session.execute("SELECT actor_id, film_id FROM film_actor;")
    # Zähle die Anzahl der Filme pro Schauspieler
    actor_film_count = Counter(row.actor_id for row in film_actor_data)
    # Sortiere die Schauspieler nach Anzahl der Filme (absteigend) und nimm die Top 10
    top_actors = actor_film_count.most_common(10)
    # Hole die Namen der Schauspieler basierend auf der actor_id und gib sie aus
    for actor_id, film_count in top_actors:
        actor_info = cs_session.execute(f"SELECT first_name, last_name FROM actor WHERE actor_id = {actor_id};").one()
        print(f"{actor_info.first_name} {actor_info.last_name} - {film_count} Filme")

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
    # Ergebnisse mit den Mitarbeitern verknüpfen und ausgeben
    for staff in staff_rows:
        staff_id = staff.staff_id
        revenue = revenue_by_staff.get(staff_id, 0)
        print(f"Staff ID: {staff_id}, Revenue: {revenue:.2f}")

    #------Aufgabe 4.e
    print()
    print("Aufgabe 4.e) Die IDs der 10 Kunden mit den meisten Entleihungen:")
    # Alle Kunden-IDs abfragen
    nosql_command = "SELECT customer_id FROM rental;"
    result = cs_session.execute(nosql_command)
    # Zählen der IDs und die zehn häufigsten IDs in Variable "top_customers" speichern und ausgeben
    rental_counts = Counter(row.customer_id for row in result) 
    top_customers = rental_counts.most_common(10) 
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
    # Ausleiheanzahl je Titel aus zuvor erstellter Tabelle abrufen, sortieren und die Top-10 ermitteln.
    result = cs_session.execute("SELECT title, rental_count FROM film_rental_count_by_title")
    sorted_result = sorted(result, key=lambda x: x.rental_count, reverse=True)
    top_10 = sorted_result[:10]
    # Daten mit Platzierung ausgeben
    for idx, row in enumerate(top_10, 1):
        print(f"{idx}. {row.title} - {row.rental_count} Ausleihen")

    #------Aufgabe 4.h
    print()
    print("Aufgabe 4.h) Die 3 meistgesehenen Filmkategorien:")
    
    result = cs_session.execute("SELECT category_name, rental_count FROM category_rental_count")
    sorted_result = sorted(result, key=lambda x: x.rental_count, reverse=True)
    top_3 = sorted_result[:3]

    for idx, row in enumerate(top_3, 1):
        print(f"{idx}. {row.category_name} - {row.rental_count} Ausleihen")

    #------Aufgabe 4.i
    print()
    print("Aufgabe 4.i) Eine Sicht auf die Kunden mit allen relevanten Informationen (auf 10 Kunden gefiltert):\n")

    # Abfrage, um alle Kundeninformationen abzurufen
    rows = cs_session.execute("SELECT id, name, address, postal_code, phone, city, country, notes, sid FROM customer_list LIMIT 10;").all()

    # Header mit den neuen Spaltennamen
    print(f"{'id':<5}{'name':<22}{'address':<40}{'zip':<6}{'phone':<14}{'city':<27}{'country':<20}{'notes':<7}{'sid':<2}")
    print("-" * 144)

    # Durch jede Zeile iterieren und die Spalten ausgeben
    for row in rows:
        print(f"{row.id:<5}{row.name:<22}{row.address:<40}{row.postal_code:<6}{row.phone:<14}{row.city:<27}{row.country:<20}{row.notes:<7}{row.sid:<2}")

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

    #------Aufgabe 5.b
    print()
    print("Aufgabe 5.b) Verlegung des Inventars an einen neuen Standort: Feldstraße 143, 22880 Wedel, Germany")
    city_name = 'Wedel'
    address = 'Feldstraße 143'

    # Abfragen der Country-ID von "Germany"
    nosql_command = "SELECT country_id FROM country WHERE country = 'Germany' ALLOW FILTERING"
    rows = cs_session.execute(nosql_command)
    country_id = rows[0].country_id
    
    # Wedel als neue Stadt einfügen
    nosql_command = "INSERT INTO city (city_id, city, country_id) VALUES (999, %s, %s)"
    cs_session.execute(nosql_command, (city_name, country_id))
    
    # ID der neuen Stadt abfragen
    # Hole die ID der neuen Stadt
    nosql_command = "SELECT city_id FROM city WHERE city = %s AND country_id = %s ALLOW FILTERING"
    rows = cs_session.execute(nosql_command, (city_name, country_id))
    city_id = rows[0].city_id
    
    # Neue Adresse einfügen
    nosql_command = "INSERT INTO address (address_id, address, city_id) VALUES (999, %s, %s)"
    cs_session.execute(nosql_command, (address, city_id))

    # Neuen Store erstellen
    nosql_command = "INSERT INTO store (store_id, manager_staff_id, address_id) VALUES (3, 1, 999)"
    cs_session.execute(nosql_command)
    print("Inhalt der Tabelle 'store':")
    nosql_command = "SELECT store_id, manager_staff_id, address_id FROM store;"
    rows = cs_session.execute(nosql_command)
    for row in rows:
        print(f"Store ID: {row.store_id}, Manager-ID: {row.manager_staff_id}, Address-ID: {row.address_id}")

    #Kontrollausgabe: 
    print("Vorhandene Store-ID in der Tabelle Inventory:")
    nosql_command = "SELECT store_id FROM inventory"
    rows = cs_session.execute(nosql_command)
    unique_store_ids = set(row.store_id for row in rows)
    print(unique_store_ids)

    print("Verlegung des Inventars wird durchgefürt...")
    select_query = "SELECT inventory_id FROM inventory"
    rows = cs_session.execute(select_query)
    update_query = "UPDATE inventory SET store_id = %s WHERE inventory_id = %s;"
    for row in rows:
        cs_session.execute(update_query, (3, row.inventory_id))

    #Kontrollausgabe: 
    print("Vorhandene Store-ID in der Tabelle Inventory:")
    nosql_command = "SELECT store_id FROM inventory"
    rows = cs_session.execute(nosql_command)
    unique_store_ids = set(row.store_id for row in rows)
    print(unique_store_ids)
    
    
    #------Aufgabe 6.a und 6.b
    print()
    print("Aufgabe 6.a)  Löscht alle Filme, die weniger als 60 Minuten Spielzeit haben und \nAufgabe 6.b)  Löscht alle damit zusammenhängenden Entleihungen:")

    # Abfrage für Filme mit einer Länge < 60
    nosql_command = "SELECT film_id FROM film WHERE length < 60 ALLOW FILTERING"
    film_rows = cs_session.execute(nosql_command)
    film_ids_to_delete = [row.film_id for row in film_rows]
    print(f"Zu löschende Filme (film_id): {film_ids_to_delete}")

    #print("\nLöschen der Filme aus der Tabelle film:")
    # Lösche die Filme aus der Tabelle film
    for film_id in film_ids_to_delete:
        nosql_command = f"DELETE FROM film WHERE film_id = {film_id}"
        cs_session.execute(nosql_command)
        #print(f"  → Eintrag in film gelöscht: film_id {film_id}")
        #print("Löschen der zugehörigen Einträge in inventory :")

        #Abfrage für inventory_id, die mit der film_id verknüpft sind
        nosql_command = f"SELECT inventory_id FROM inventory WHERE film_id = {film_id} ALLOW FILTERING"
        inventory_rows = cs_session.execute(nosql_command)
        inventory_ids_to_delete = [row.inventory_id for row in inventory_rows]
        #print(f"Gefundene inventory_id für film_id {film_id}: {inventory_ids_to_delete}")

        for inventory_id in inventory_ids_to_delete:
            #Lösche aus inventory basierend auf film_id
            nosql_command = f"DELETE FROM inventory WHERE inventory_id = {inventory_id}"
            cs_session.execute(nosql_command)
            #print(f"Einträge in inventory gelöscht für inventory_id: {inventory_id}")

            #Abfrage für rental_id, die mit der inventory_id verknüpft sind
            nosql_command = f"SELECT rental_id FROM rental WHERE inventory_id = {inventory_id} ALLOW FILTERING"
            rental_rows = cs_session.execute(nosql_command)
            rental_ids_to_delete = [row.rental_id for row in rental_rows]
            #print(f"Gefundene rental_id für inventory_id {inventory_id}: {rental_ids_to_delete}")

            # Lösche aus rental_id basierend auf inventory_id
            for rental_id in rental_ids_to_delete:
                nosql_command = f"DELETE FROM rental WHERE rental_id = {rental_id}"
                cs_session.execute(nosql_command)
                #print(f"Eintrag in rental gelöscht: rental_id {rental_id}")
        #print("Gefundene inventory_ids und rental_ids zur film_id, sowie der film selbst wurden gelöscht \n")

    # Überprüfen, ob noch Filme mit Länge < 60 vorhanden sind
    nosql_command = "SELECT film_id FROM film WHERE length < 60 ALLOW FILTERING"
    remaining_films = cs_session.execute(nosql_command)

    if remaining_films:
        print("Die folgenden Filme mit Länge < 60 sind noch vorhanden:")
        for row in remaining_films:
            print(f"  → film_id: {row.film_id}")
    else:
        print("Alle Filme mit Länge < 60 wurden erfolgreich gelöscht.")

    # Überprüfen, ob verwaiste Rentals existieren
    remaining_rentals_found = False

    for film_id in film_ids_to_delete:
        # Alle inventory_id für die aktuelle film_id abrufen
        nosql_command = f"SELECT inventory_id FROM inventory WHERE film_id = {film_id} ALLOW FILTERING"
        inventory_rows = cs_session.execute(nosql_command)
        inventory_ids_to_check = [row.inventory_id for row in inventory_rows]
        
        # Überprüfen, ob es noch Rentals für diese inventory_id gibt
        for inventory_id in inventory_ids_to_check:
            nosql_command = f"SELECT rental_id FROM rental WHERE inventory_id = {inventory_id} ALLOW FILTERING"
            rental_rows = cs_session.execute(nosql_command)
            
            for row in rental_rows:
                print(f"  → Verwaister rental gefunden: rental_id {row.rental_id}")
                remaining_rentals_found = True

    if not remaining_rentals_found:
        print("Alle zugehörigen Rentals wurden erfolgreich gelöscht.")


    print("\n---------------------Alle CQL-Anfragen wurden bearbeitet--------------------------")

try:
    print("\n--------------------------Python Script startet-----------------------------------")
    # Startzeit erfassen
    start_time = time.time()

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

    # Endzeit erfassen und Laufzeit berechnen
    end_time = time.time()  
    duration = end_time - start_time  # Laufzeit berechnen

    # Laufzeit in Sekunden ausgeben
    print(f"\nGesamtlaufzeit des Skripts: {duration:.2f} Sekunden")

    
except Exception as e:
    print(f"Fehler aufgetreten: {e}")

finally:
    # Verbindung zur PostgreSQL und Cassandra DB schließen
    pg_conn.close()
    cs_cluster.shutdown()
