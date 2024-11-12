Semesterübung von Gruppe 10

Gruppenmitglieder: Adrian Adams und Lucas Knieling

Die Entwicklung des abgegebenen Codes hat im Stils des Pair-Programmings stattgefunden.
Die Aufwandsanteile belaufen sich auf 50% je Gruppenmitglied.


Start der Applikation:

1. Navigation im Terminal zum Applikationsorder
2. Befehl "docker-compose down  # Stoppt alle laufenden Container
>> docker-compose build --no-cache  # Baut die Container neu
>> docker-compose up -d --remove-orphans # Startet die Container im Hintergrund"
3. Warten bis die Services gestartet sind und laufen. Als erstet startet die PG Datenbank und wird befüllt. 
Zusätzlich startet die Cassandra DB. Sind beide Datenbanken verfügbar, startet der Python-Service und das hinterlegte Skript wird ausgeführt.
4. In den Logs des Python-Service Containers werden die geforderten Aufgaben ausgegeben. Befehl: "docker-compose logs python_service"