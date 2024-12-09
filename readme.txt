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
Alternativ kann über Docker Desktop in den Python-Service Containers navigiert werden und die Logs ebenfalls angezeigt werden.


Anmerkung: 

- Der vollständige Code des Python-Scripts ist in folgender Datei einsehbar: deploy/scripts/script.py
- Im Ordner "other" befindet sich:
  - Eine Datei mit kopierten Log-Ausgaben der letzten Ausführung vor Abgabe.
  - Eine SQL-Datei mit SQL-Prompts, welche auf der PG-Datenbank verwendet wurden, um die Ausgaben des Python-Services während der Entwicklung zu validieren. 
- Je nach Kapazitäten des verwendeten Rechners, varriiert die Laufzeit. Eine Laufzeit von 2-3 Minuten ist während der Entwicklung üblich gewesen.
