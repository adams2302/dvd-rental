Semesterübung von Gruppe 10

Gruppenmitglieder: Adrian Adams und Lucas Knieling

Die Entwicklung des abgegebenen Codes hat im Stils des Pair-Programmings stattgefunden.
Die Aufwandsanteile belaufen sich auf 50% je Gruppenmitglied.
Zur Entwicklung und Sicherung des Codes wurde ein GitHub Repository erstellt und gepflegt.


Start der Applikation:

1. Navigation im Terminal zum Applikationsorder
2. Befehl "docker-compose up -d" startet die Anwendung
3. Warten bis die Services gestartet sind und laufen. Als erstet startet die PG Datenbank und wird befüllt. 
Zusätzlich startet die Cassandra DB. Sind beide Datenbanken verfügbar, startet der Python-Service und das hinterlegte Skript wird ausgeführt.
4. In den Logs des Python-Service Containers werden die geforderten Aufgaben ausgegeben. Befehl: "docker-compose logs python_service"

Anmerkung: Je nach Kapazitäten des verwendeten Rechners, varriiert die Laufzeit. Eine Laufzeit von 2-3 Minuten ist während der Entwicklung üblich gewesen.
