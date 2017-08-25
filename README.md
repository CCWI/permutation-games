# permutation-games

Aus dem Eclipse-Projekt kann mit Maven ein JAR erzeugt werden. Für die Ausführung der Hadoop-Variante muss Hadoop auf dem System installiert sein. Dem Programm können folgende Parameter übergeben werden:

	- "game30": Spielt das Ratespiel 30
		- Für eine lokale Berechnung die Anzahl der parallel 
		  auszufuehrenden Threads, welche zwischen 1 und 15 (inklusive) 
		  liegen muss
			- Verzeichnis des lokalen Dateisystems, in welches die
			  Ausgabedatei geschrieben werden soll
		- "hadoop": Berechnung mit Apache Hadoop MapReduce
			- Verzeichnis des Ratespiels 30 im verteilten Dateisystem
	- "availableProcessors": Gibt die Anzahl an verfuegbaren Prozessoren aus
