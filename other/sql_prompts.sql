-- SQL-Befehle zu Aufgabe 4

-- Aufgabe 4a)
-- Gesamtanzahl der verfügbaren Filme
select count(film_id)
from inventory

-- Aufgabe 4b)
-- Anzahl der unterschiedlichen Filme je Standort
select store_id,
count(distinct film_id) as Anzahl_Unterschiedlicher_Filme
from inventory
group by store_id

-- Aufgabe 4c)
-- Die Vor- und Nachnamen der 10 Schauspieler mit den meisten Filmen, absteigend sortiert
select Concat(actor.first_name,' ',actor.last_name) as Name,
count(distinct film_actor.film_id) as NumberFilms
from actor
left join film_actor ON actor.actor_id = film_actor.actor_id
group by Name
order by NumberFilms desc
limit 10

-- Aufgabe 4d)
-- Die Erlöse je Mitarbeiter
select staff_list.name,
sum(payment.amount) as Revenue
from staff_list
left join payment ON staff_list.id = payment.staff_id
group by staff_list.name

-- Aufgabe 4e)
-- Die IDs der 10 Kunden mit den meisten Entleihungen
select customer_id as Customer_ID,
count(customer_id) as Number_Rentals
from rental
group by Customer_ID
order by Number_Rentals desc
limit 10

-- Aufgabe 4f)
-- Die Vor- und Nachnamen sowie die Niederlassung der 10 Kunden, die das meiste Geld ausgegeben haben
select Concat(customer.first_name,' ',customer.last_name) as Name,
customer.store_id as Store,
sum(payment.amount) as Revenue
from customer
left join payment ON customer.customer_id = payment.customer_id
group by Store, Name
order by Revenue desc
limit 10

-- Aufgabe 4g)
-- Die 10 meistgesehenen Filme unter Angabe des Titels, absteigend sortiert
select film.title,
count(rental.rental_id) as rental_count
from rental
JOIN inventory ON rental.inventory_id = inventory.inventory_id
JOIN film ON inventory.film_id = film.film_id
group by film.title
order by rental_count desc
limit 10

-- Aufgabe 4i)
-- Eine Sicht auf die Kunden mit allen relevanten Informationen wie im View „customer_list“ der vorhandenen Postgres-Datenbank
SELECT customer.customer_id AS id,
    concat(customer.first_name, ' ', customer.last_name) AS name,
    address.address,
    address.postal_code AS zip_code,
    address.phone,
    city.city,
    country.country,
        CASE
            WHEN (customer.active = 1) THEN 'active'::text
            WHEN (customer.active = 0) THEN 'inactive'::text
            ELSE NULL::text
        END AS notes,
    customer.store_id AS sid
   FROM (((customer
     JOIN address ON ((customer.address_id = address.address_id)))
     JOIN city ON ((address.city_id = city.city_id)))
     JOIN country ON ((city.country_id = country.country_id)));

-- Aufgabe 5a)
-- Allen Mitarbeitern neue Passwörter vergeben
UPDATE staff
SET password= 'DeinNeuesPaswort123!'
WHERE staff_id = 1;
 
UPDATE Staff
SET password= 'DeinNeuesPaswort321!'
WHERE staff_id = 2;