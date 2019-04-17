# permutation-games
[![Build Status](https://travis-ci.org/CCWI/permutation-games.svg)](https://travis-ci.org/CCWI/permutation-games)

Aus dem Eclipse-Projekt kann mit Maven ein JAR erzeugt werden. Für die Ausführung mit Hadoop muss Hadoop auf dem System installiert sein. Die Ausführung mit Spark ist ohne weitere Voraussetzungen möglich. 

## Parameter für den Aufruf
Für die Ausführung können bis zu drei Parameter übergeben werden.

1. Parameter
  * **game30** Spielt das Ratespiel 30
  * **availableProcessors** Gibt die Anzahl an verfuegbaren Prozessoren aus
2. Parameter
  * **AnzahlThreads** Für eine lokale Berechnung ist die Anzahl der parallel auszufuehrenden Threads anzugeben, welche zwischen 1 und 15 (inklusive) liege muss
  * **hadoop** Berechnung mit Apache Hadoop MapReduce
  * **spark** Berechnung mit Apache Spark
3. Parameter
  * **Verzeichnis** Verzeichnis im verteilten Dateisystem für die Ein- und Ausgabe des Ratespiels 30 (nur für Hadoop und Spark erforderlich)

## Ausführung mit Hadoop auf YARN
```bash
yarn 
	jar 
	permutation-games-0.0.1-SNAPSHOT.jar 
	game30 
	hadoop 
	output
```

## Ausführung mit Spark auf YARN
```bash
export SPARK_MAJOR_VERSION=2
spark-submit 
	--class edu.hm.ccwi.permutationgames.Play 
	--master yarn 
	--num-executors 8 
	permutation-games-0.0.1-SNAPSHOT.jar 
	game30 
	spark 
	output
```
