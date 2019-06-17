
package edu.hm.ccwi.permutationgames;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.util.LongAccumulator;

import scala.Tuple2;

/**
 * Diese Klasse stellt Funktionen zur Verfuegung, mit denen sich alle Loesungen
 * des Ratespiels 30 finden und ausgeben lassen. Hierfuer kann zwischen drei
 * Verarbeitungsarten gewaehlt werden: Single-threaded, Multi-threaded oder mit
 * Apache Hadoop. Da 15 Steine richtig angeordnet werden muessen, sind 15! (ca.
 * 1,307 Billionen) verschiedene Permutationen zu betrachten. Eine gueltige
 * Loesung ist z. B. {7,1,12,10,14,8,5,3,9,15,2,4,6,11,13}. Insgesamt gibt es
 * 416 gueltige Loesungen.
 * 
 * @author Alexander Doeschl
 *
 */
public class Game30 {
    /**
     * Anzahl aller Permutationen - entspricht 15! = 1.307.674.368.000 (ca.
     * 1,307 Billionen)
     */
    private static final long TOTAL_NUM_PERM = factorial(15);
    /**
     * Menge aller Spielsteine des Spiels mit den Werten 1 bis 15
     */
    private static final Integer[] ALL_PAWNS = new Integer[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
    /**
     * Summe, welche die Spielsteine auf jeder Kanten ergeben muessen
     */
    private static final int SUM_OF_EDGE = 30;

    /**
     * Startzeit der Verarbeitung (wird nicht bei Hadoop-Verarbeitung verwendet)
     */
    private static long startTime = System.currentTimeMillis();
    /**
     * Thread-uebergreifende Zahl der bisher verarbeiteten Permutationen (wird
     * nicht bei Hadoop-Verarbeitung verwendet)
     */
    private static LongAdder numExecPerm = new LongAdder();
    /**
     * Thread-uebergreifende  Zahl der bisher gefundenen Loesungen fuer eine
     * uebersichtliche Ausgabe (wird nicht bei Hadoop-Verarbeitung verwendet)
     */
    private static AtomicInteger solutionNum = new AtomicInteger();
    /**
     * Pfad der Datei, welche die gefundenen Loesungen aller Threads haelt (wird
     * nicht bei Hadoop-Verarbeitung verwendet)
     */
    private String outputFile = "init";

    /**
     * Task-uebergreifende Zahl der bisher verarbeiteten Permutationen (wird nur
     * bei Hadoop-Verarbeitung verwendet)
     */
    public static enum GAME30_COUNTER {
        NUM_EXEC_PERMUTATIONS
    }

    /**
     * Die ersten 5 Ziffern der Anzahl aller Permutationen - entspricht ca.
     * 15!E-8 (wird nicht bei Hadoop-Verarbeitung verwendet)
     */
    private static final int TOTAL_NUM_PERM_E_MINUS_8 = (int) (TOTAL_NUM_PERM / 100000000);

    /**
     * Format fuer die Ausgabe der aktuellen Zeit (Datum und Uhrzeit) bei
     * Fortschrittsabfragen (wird nicht bei Hadoop-Verarbeitung verwendet)
     */
    private static final DateFormat currentDate = new SimpleDateFormat("yy-MM-dd: HH:mm:ss");

    /**
     * Ein Pool mit einem Thread, welcher periodisch fuer Fortschrittsabfragen
     * gestartet werden kann (wird nicht bei Hadoop-Verarbeitung verwendet)
     */
    private ScheduledExecutorService progressPool;
    /**
     * Ein Runnable-Objekt, das den Fortschritt des Spiels ausgibt (wird nicht
     * bei Hadoop-Verarbeitung verwendet)
     */
    private Runnable printProgressRunnable = new Runnable() {
        @Override
        public void run() {
            printProgress();
        }
    };

    /**
     * Spielt das Ratespiel 30 mit einem Thread und gibt alle Loesungen in einer
     * Datei aus.
     *
     * @param outputFile Der Pfad der Ausgabedatei
     * @throws IOException
     * @throws FileAlreadyExistsException
     */
    public void playSingleThreaded(String outputFile) throws IOException {

        this.createOutputFile(outputFile);

        System.out.println("Starts processing with 1 thread...");

        // Gibt alle 5 Minuten den Spielfortschritt aus
        progressPool = Executors.newScheduledThreadPool(1);
        progressPool.scheduleAtFixedRate(printProgressRunnable, 0, 5, TimeUnit.MINUTES);

        // Startet einen Solving-Thread
        Thread thread = new Thread(createSolvingRunnable(null, ALL_PAWNS));
        thread.start();

        // Wartet bis der Thread abgearbeitet ist
        try {
            thread.join();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        // Schliesst den Thread-Pool der Fortschrittsabfrage und gibt ein
        // letztes Mal den (abgeschlossenen) Spielfortschritt aus
        progressPool.shutdown();
        printProgress();

        // Gibt die tatsaechlich benoetigte Verarbeitungszeit aus
        long processingTime = System.currentTimeMillis() - startTime;
        String processingTimeHms = String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(processingTime),
                TimeUnit.MILLISECONDS.toMinutes(processingTime) % TimeUnit.HOURS.toMinutes(1),
                TimeUnit.MILLISECONDS.toSeconds(processingTime) % TimeUnit.MINUTES.toSeconds(1));
        System.out.println("Complete!\tProcessing time: " + processingTimeHms);
    }

    /**
     * Spielt das Ratespiel 30 mit n Threads (0 < n < 16) und gibt alle
     * Loesungen in einer Datei aus.
     *
     * @param outputFile Der Pfad der Ausgabedatei, in welche alle Threads schreiben
     * @param numThreads Anzahl der parallel auszufuehrenden Threads, welche zwischen 1
     *                   und 15 (inklusive) liegen muss
     * @throws IOException
     */
    public void playMultiThreaded(String outputFile, int numThreads) throws IOException {
        if (numThreads < 1 && numThreads > 15) {
            throw new IllegalArgumentException("Parameter numThreads must be between 1 and 15 (inclusive)!");
        }

        this.createOutputFile(outputFile);

        System.out.println("Starts processing with " + numThreads + " thread(s)...");

        // Gibt alle 5 Minuten den Spielfortschritt aus
        progressPool = Executors.newScheduledThreadPool(1);
        progressPool.scheduleAtFixedRate(printProgressRunnable, 0, 5, TimeUnit.MINUTES);

        // Ein Pool, welcher die in Parameter numThreads festgelegte Zahl an
        // Threads dauerhaft zur Verfuegung stellt. Jene Threads werden immer
        // wieder mit Runnable-Objekten aus einer Queue neu belegt
        ExecutorService solvingPool = Executors.newFixedThreadPool(numThreads);

        // Laedt alle Solving-Runnable-Objekte in die Queue des solvingPools
        for (int i = 1; i <= 15; i++) {
            solvingPool.submit(createSolvingRunnable(new Integer[]{i}, removeElement(ALL_PAWNS, i - 1)));
        }

        // Schliesst den solvingPool, wenn alle Threads sowie die Queue
        // abgearbeitet sind
        solvingPool.shutdown();
        try {
            solvingPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        // Schliesst den Thread-Pool der Fortschrittsabfrage und gibt ein
        // letztes Mal den (abgeschlossenen) Spielfortschritt aus
        progressPool.shutdown();
        printProgress();

        // Gibt die tatsaechlich benoetigte Verarbeitungszeit aus
        long processingTime = System.currentTimeMillis() - startTime;
        String processingTimeHms = String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(processingTime),
                TimeUnit.MILLISECONDS.toMinutes(processingTime) % TimeUnit.HOURS.toMinutes(1),
                TimeUnit.MILLISECONDS.toSeconds(processingTime) % TimeUnit.MINUTES.toSeconds(1));
        System.out.println("Complete!\tProcessing time: " + processingTimeHms);
    }

    /**
     * Loescht aus dem uebergebenen {@link java.lang.Integer Integer}-Array das
     * Element auf der angegebenen Position und gibt ein verkuerztes
     * {@link java.lang.Integer Integer}-Array zurueck.
     *
     * @param original   Das Array, aus dem ein Element entfernt werden soll
     * @param elementPos Index des Elements, das zu entfernen ist
     * @return Neues, verkuerztes {@link java.lang.Integer Integer}-Array ohne
     * das zu loeschende Element
     */
    private static Integer[] removeElement(Integer[] original, int elementPos) {
        Integer[] n = new Integer[original.length - 1];
        System.arraycopy(original, 0, n, 0, elementPos);
        System.arraycopy(original, elementPos + 1, n, elementPos, original.length - elementPos - 1);
        return n;
    }

    private void createOutputFile(String outputFile) throws IOException {
        // Erstellt, sofern moeglich, die Ausgabedatei oder wirft andernfalls
        // eine Exception inklusive Fehlerbeschreibung
        try {
            File file = new File(outputFile);

            if (file.createNewFile()) {
                this.outputFile = outputFile;
            } else {
                throw new FileAlreadyExistsException("Output file already exists!");
            }
        } catch (FileAlreadyExistsException e) {
            throw e;
        } catch (IOException e) {
            throw new IOException("Output directory does not exist or access is forbidden for user "
                    + System.getProperty("user.name") + "!", e);
        }
    }

    /**
     * Erstellt ein Runnable-Objekt, das die parallele Ausfuehrung der Methode
     * {@link #findSolutions(Integer[], Integer[], Context) findSolutions}
     * ermoeglicht. Die Summe aus den Laengen der Arrays
     * <code>staticLeadPawns</code> und <code>pawnsToPerm</code> darf 15 nicht
     * ueberschreiten.
     *
     * @param staticLeadPawns Folge von Spielsteinen, welche nicht zu permutieren ist,
     *                        sondern den <code>pawnsToPerm</code> unveraendert vorangesetzt
     *                        wird, um moegliche Loesungen zu bilden
     * @param pawnsToPerm     Menge der Spielsteine, welche im ausfuehrenden Thread
     *                        permutiert werden soll, um moegliche Loesungen zu finden
     * @return Runnable-Objekt zur Ausfuehrung der Methode
     * {@link #findSolutions(Integer[], Integer[], Context)
     * findSolutions}
     */
    private Runnable createSolvingRunnable(final Integer[] staticLeadPawns, final Integer[] pawnsToPerm) {
        return new Runnable() {
            @Override
            public void run() {
                findSolutions(staticLeadPawns, pawnsToPerm);
            }
        };
    }
    
    /**
     * Sucht nach Loesungen des Ratespiels 30 mittels iterativer Permutation und
     * gibt die gefundenen Loesungen aus. Falls ein Hadoop-Context uebergeben
     * wird, werden die Loesungen in diesen geschrieben, wenn nicht
     * (<code>hadoopContext</code> ist <code>null</code>), erfolgt die Ausgabe
     * in das lokale Dateisystem.
     *
     * @param staticLeadPawns Folge von Spielsteinen, welche nicht zu permutieren ist,
     *                        sondern den <code>pawnsToPerm</code> unveraendert vorangesetzt
     *                        wird, um moegliche Loesungen zu bilden
     * @param pawnsToPerm     Menge der Spielsteinen, welche es zu permutieren gilt, um
     *                        moegliche Loesungen zu finden
     */
    public ArrayList<String> findSolutions(Integer[] staticLeadPawns, Integer[] pawnsToPerm) {
    	return findSolutions(staticLeadPawns, pawnsToPerm, null, null);
    }
    
    /**
     * Sucht nach Loesungen des Ratespiels 30 mittels iterativer Permutation und
     * gibt die gefundenen Loesungen aus. Falls ein Hadoop-Context uebergeben
     * wird, werden die Loesungen in diesen geschrieben, wenn nicht
     * (<code>hadoopContext</code> ist <code>null</code>), erfolgt die Ausgabe
     * in das lokale Dateisystem.
     *
     * @param staticLeadPawns Folge von Spielsteinen, welche nicht zu permutieren ist,
     *                        sondern den <code>pawnsToPerm</code> unveraendert vorangesetzt
     *                        wird, um moegliche Loesungen zu bilden
     * @param pawnsToPerm     Menge der Spielsteinen, welche es zu permutieren gilt, um
     *                        moegliche Loesungen zu finden
     * @param hadoopContext   Hadoop-Context, in welchen es die gefundenen Loesungen
     *                        auszugeben gilt - falls <code>null</code>, erfolgt die Ausgabe
     *                        in das lokale Dateisystem
     */
    public ArrayList<String> findSolutions(Integer[] staticLeadPawns, Integer[] pawnsToPerm,
            Mapper<Object, Text, Text, NullWritable>.Context hadoopContext) {
    	return findSolutions(staticLeadPawns, pawnsToPerm, hadoopContext, null);
    }
    
    /**
     * Sucht nach Loesungen des Ratespiels 30 mittels iterativer Permutation und
     * gibt die gefundenen Loesungen aus. Falls ein Hadoop-Context uebergeben
     * wird, werden die Loesungen in diesen geschrieben, wenn nicht
     * (<code>hadoopContext</code> ist <code>null</code>), erfolgt die Ausgabe
     * in das lokale Dateisystem.
     *
     * @param staticLeadPawns Folge von Spielsteinen, welche nicht zu permutieren ist,
     *                        sondern den <code>pawnsToPerm</code> unveraendert vorangesetzt
     *                        wird, um moegliche Loesungen zu bilden
     * @param pawnsToPerm     Menge der Spielsteinen, welche es zu permutieren gilt, um
     *                        moegliche Loesungen zu finden
     * @param numPermutations Spark-Akkumulator, über den die Anzahl der berechneten Lösungen 
     * 						  gezählt wird - falls <code>null</code>, erfolgt die Ausgabe
     *                        in das lokale Dateisystem
     */
    public ArrayList<String> findSolutions(Integer[] staticLeadPawns, Integer[] pawnsToPerm,
    		LongAccumulator numPermutations) {
    	return findSolutions(staticLeadPawns, pawnsToPerm, null, numPermutations);
    }

    /**
     * Sucht nach Loesungen des Ratespiels 30 mittels iterativer Permutation und
     * gibt die gefundenen Loesungen aus. Falls ein Hadoop-Context uebergeben
     * wird, werden die Loesungen in diesen geschrieben, wenn nicht
     * (<code>hadoopContext</code> ist <code>null</code>), erfolgt die Ausgabe
     * in das lokale Dateisystem.
     *
     * @param staticLeadPawns Folge von Spielsteinen, welche nicht zu permutieren ist,
     *                        sondern den <code>pawnsToPerm</code> unveraendert vorangesetzt
     *                        wird, um moegliche Loesungen zu bilden
     * @param pawnsToPerm     Menge der Spielsteinen, welche es zu permutieren gilt, um
     *                        moegliche Loesungen zu finden
     * @param hadoopContext   Hadoop-Context, in welchen es die gefundenen Loesungen
     *                        auszugeben gilt - falls <code>null</code>, erfolgt die Ausgabe
     *                        in das lokale Dateisystem
     * @param numPermutations Spark-Akkumulator, über den die Anzahl der berechneten Lösungen 
     * 						  gezählt wird - falls <code>null</code>, erfolgt die Ausgabe
     *                        in das lokale Dateisystem
     */
    private ArrayList<String> findSolutions(Integer[] staticLeadPawns, Integer[] pawnsToPerm,
                                              Mapper<Object, Text, Text, NullWritable>.Context hadoopContext,
                                              LongAccumulator numPermutations) {
        int[] result = new int[15];
        ArrayList<String> resultSet = new ArrayList<String>();

        boolean withHadoop = false;
        boolean withSpark = false;
        if (hadoopContext != null) {
            withHadoop = true;
        } else if (numPermutations != null) {
        	withSpark = true;
        }

        // Belegt die Permutation mit den statischen Spielsteinen vor
        int leadPawnsLength = 0;
        if (staticLeadPawns != null) {
            leadPawnsLength = staticLeadPawns.length;
            for (int i = 0; i < leadPawnsLength; i++) {
                result[i] = staticLeadPawns[i];
            }
        }

        int permPawnsLength = pawnsToPerm.length;

        // Fuehrt iterative Teilpermutationen durch, verbindet diese mit den
        // statischen Spielsteinen und testet die daraus entstehenden
        // vollstaendigen Permutationen auf deren Erfuellung der Spielregeln
        Permutations<Integer> perm = new Permutations<Integer>(pawnsToPerm);

        // Eine Permutation, die es zu ueberpruefen gilt
        Integer[] aPerm;
        while (perm.hasNext()) {
            // Erfragt eine weitere Teilpermutation
            aPerm = perm.next();

            // Vervollstaendigt eine Permutation mit der Teilpermutation
            for (int i = 0; i < permPawnsLength; i++) {
                result[leadPawnsLength + i] = aPerm[i];
            }

            // Aktuallisiert den Fortschritt der Verarbeitung
            if (withHadoop) {
                hadoopContext.getCounter(GAME30_COUNTER.NUM_EXEC_PERMUTATIONS).increment(1);
            } else if (withSpark) {
            	numPermutations.add(1);
            } else {
                numExecPerm.increment();
            }

            // Testet, ob die Permutation die Spielregeln erfuellt - falls ja,
            // wird sie dem Hadoop-Context ueber- bzw. ins lokale Dateisystem
            // ausgegeben
            if (rulesSatisfied(result)) {
                if (withHadoop) {
                    try {
                        hadoopContext.write(new Text(getPermutationString(result)), NullWritable.get());
                    } catch (IOException | InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                } else if (withSpark) {
                	resultSet.add(getPermutationString(result));
                }	else {
                    solutionNum.incrementAndGet();
                    writeSolutionToLocalFs(getPermutationString(result));
                }
            }
        }
        return resultSet;
    }


	/**
	 * Hadoop-Mapper, welcher statische Spielsteine zugeteilt bekommt und fuer
	 * die Vervollstaendigung der restlichen Felder Permutationen durchfuehrt,
	 * um Loesungen fuer das Ratespiel 30 zu finden. Hierzu wird die Methode
	 * {@link #findSolutions(Integer[], Integer[], Context) findSolutions}
	 * verwendet, die die gefundenen Loesungen in den uebergebenen
	 * Hadoop-Context schreibt.
	 * 
	 * @author Alexander Doeschl
	 *
	 */
	public static class Game30Mapper extends Mapper<Object, Text, Text, NullWritable> {
		private Game30 game = new Game30();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// Splittet die eingelesene Folge von statischen Spielsteine am
			// Trennzeichen und ueberfuehrt diese in ein Array
			Integer[] staticLeadPawns = stringArrayToIntegerArray(value.toString().split("\\|"));

			// Loescht die statischen Steine aus der Menge aller Spielsteine, um
			// die zu permutierende Menge zu erhalten
			Integer[] pawnsToPerm = ALL_PAWNS;
			for (Integer staticLeadPawn : staticLeadPawns) {
				pawnsToPerm = removeElement(pawnsToPerm, Arrays.asList(pawnsToPerm).indexOf(staticLeadPawn));
			}

			game.findSolutions(staticLeadPawns, pawnsToPerm, context);
		}

		/**
		 * Erstellt aus einem {@link java.lang.String String}-Array ein
		 * {@link java.lang.Integer Integer}-Array.
		 * 
		 * @param array
		 *            Array, das es zu konvertieren gilt
		 * @return Neues {@link java.lang.Integer Integer}-Array
		 */
		private static Integer[] stringArrayToIntegerArray(String[] array) {
			return Arrays.asList(array).stream().mapToInt(Integer::parseInt).boxed().toArray(Integer[]::new);
		}
	}

	/**
	 * Hadoop-Reducer, welcher alle gefundenen Loesungen zusammenfasst und dabei
	 * nummeriert.
	 * 
	 * @author Alexander Doeschl
	 *
	 */
	public static class Game30Reducer extends Reducer<Text, NullWritable, Text, NullWritable> {
		private int solutionNum;

		@Override
		public void reduce(Text key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
			solutionNum++;
			context.write(new Text("Loesung " + solutionNum + ":\n" + key), NullWritable.get());
		}
	}
	
	/**
	 * Diese Klasse stellt Funktionen für die Berechnung
	 * der Lösungen des Ratespiels 30 in Spark bereit.
	 * @author Florian Gebhart
	 * @author Max-Emanuel Keller
	 *
	 */
	public static class Game30Spark {
		/**
		 * Methode, welche statische Spielsteine zugeteilt bekommt und fuer
		 * die Vervollstaendigung der restlichen Felder Permutationen durchfuehrt,
		 * um Loesungen fuer das Ratespiel 30 zu finden. Hierzu wird die Methode
		 * {@link #findSolutions(Integer[], Integer[], Context) findSolutions}
		 * verwendet, welche die gefundenen Lösungen als String-Array zurückliefert.
		 * @param numPermutations Akkumulator, der die Anzahl der gefundenen Permutationen zählt
		 * @return Spark-Funktion für die Berechnung der Lösungen
		 * 
		 * @author Max-Emanuel Keller
		 */
		public static FlatMapFunction<Integer[], String> calculateSolutions(LongAccumulator numPermutations) {
	    	return e -> {
	    		Game30 game30 = new Game30();
	            ArrayList<String> solutionsFound;
	            
	            Integer[] pawnsToPerm = ALL_PAWNS;

	            // Loescht die statischen Steine aus der Menge aller Spielsteine, um
	            // die zu permutierende Menge zu erhalten
	            for (Integer staticLeadPawn : e) {
	                pawnsToPerm = removeElement(pawnsToPerm, Arrays.asList(pawnsToPerm).indexOf(staticLeadPawn));
	            }
	            
	            // Berechnung der Lösungen für die übergebenen Steine
	            solutionsFound = game30.findSolutions(e, pawnsToPerm, numPermutations);
	            
	            return solutionsFound.iterator();
	        };
	    }
		
		/**
		 * Methode, welche die Indexnummer eines Elements zu dessen Nummerierung verwendet.
		 * @return Spark-Funktion für die Übernahme der Lösungsnummern
		 */
		public static Function<Tuple2<String,Long>,String> enumerateSolutions() {
			return e -> {
				StringBuilder solutionNumber = new StringBuilder();
				solutionNumber.append("Loesung ");
				solutionNumber.append(Long.valueOf(e._2 + 1));
				solutionNumber.append(":\n");
				solutionNumber.append(e._1);
				return solutionNumber.toString();
			};
		}
		
		/**
		 * Spark-Funktion für das Herausfiltern der nicht leeren Lösungen.
		 */
		public static final Function<String, Boolean> nonEmptySolutions = x -> x.length() > 0;
	}

	/**
	 * Prueft, ob eine Permutation die Spielregeln des Ratespiels 30 erfuellt.
	 * 
	 * @param perm
	 *            Zu pruefende Permutation
	 * @return <code>true</code>, wenn alle Spielregeln erfuellt sind,
	 *         andernfalls <code>false</code>
	 */
	private boolean rulesSatisfied(int[] perm) {
		if (((perm[0] + perm[1] + perm[2] + perm[3]) == SUM_OF_EDGE)
				&& ((perm[4] + perm[5] + perm[6] + perm[7]) == SUM_OF_EDGE)
				&& ((perm[8] + perm[9] + perm[10] + perm[11]) == SUM_OF_EDGE)
				&& ((perm[12] + perm[13] + perm[14]) == SUM_OF_EDGE) && ((perm[0] + perm[4] + perm[8]) == SUM_OF_EDGE)
				&& ((perm[1] + perm[5] + perm[9] + perm[12]) == SUM_OF_EDGE)
				&& ((perm[2] + perm[6] + perm[10] + perm[13]) == SUM_OF_EDGE)
				&& ((perm[3] + perm[7] + perm[11] + perm[14]) == SUM_OF_EDGE)
				&& ((perm[0] + perm[5] + perm[10] + perm[14]) == SUM_OF_EDGE)
				&& ((perm[3] + perm[6] + perm[9]) == SUM_OF_EDGE)) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Schreibt eine Loesung in das lokale Dateisystem.
	 * 
	 * @param solution
	 *            Formatierte Loesung, die auszugeben ist
	 */
	private synchronized void writeSolutionToLocalFs(String solution) {
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile, true))) {
			bw.append("Loesung " + solutionNum.incrementAndGet() + ":\n" + solution);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Stellt eine Permutation als einen optisch an das pysische Spiel 30
	 * angepassten {@link java.lang.String String} dar.
	 * 
	 * @param perm
	 *            Darzustellende Permutation
	 * 
	 * @return {@link java.lang.String String}-Darstellung der Permutation
	 */
	private static String getPermutationString(int[] perm) {
		return String.format(
				"%02d | %02d | %02d | %02d\n" +
                   "%02d | %02d | %02d | %02d\n" +
                   "%02d | %02d | %02d | %02d\n" +
                     "   | %02d | %02d | %02d\n",
				perm[0], perm[1], perm[2], perm[3], perm[4], perm[5], perm[6], perm[7], perm[8], perm[9], perm[10],
				perm[11], perm[12], perm[13], perm[14]);
	}

	/**
	 * Gibt den Spielfortschritt in Prozent sowie die Anzahl der ausgefuehrten
	 * Permutationen aus. Zudem wird die zu erwartende Restdauer errechnet und
	 * ausgegeben.
	 */
	private void printProgress() {
		long numExecPermLong = numExecPerm.sum();
		int numExecPermEMinus8 = (int) (numExecPermLong / 100000000);

		long eta = numExecPermEMinus8 == 0 ? 0
				: (TOTAL_NUM_PERM_E_MINUS_8 - numExecPermEMinus8) * (System.currentTimeMillis() - startTime)
						/ numExecPermEMinus8;

		String etaHms = numExecPermEMinus8 == 0 ? "N/A"
				: String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(eta),
						TimeUnit.MILLISECONDS.toMinutes(eta) % TimeUnit.HOURS.toMinutes(1),
						TimeUnit.MILLISECONDS.toSeconds(eta) % TimeUnit.MINUTES.toSeconds(1));

		double percent = (double) numExecPermEMinus8 * 100 / TOTAL_NUM_PERM_E_MINUS_8;

		System.out.println(currentDate.format(new Date()) + "  Calculated: " + String.format("%6.2f", percent) + " %  "
				+ String.format("%13d", numExecPermLong) + "/" + TOTAL_NUM_PERM + "  Remaining time: " + etaHms);
	}

	/**
	 * Berechnet die Fakultaet fuer die Zahl n und gibt das Ergebnis zurueck.
	 * 
	 * @param n
	 *            Positive Zahl, fuer welche die Fakultaet zu bestimmten ist
	 * @return Fakultaet fuer die Zahl n:
	 *         <ul>
	 *         <li>n > 0: n!
	 *         <li>n = 0: 1
	 *         <li>n < 0: 0
	 *         </ul>
	 */
	private static long factorial(int n) {
		long fac = 1;
		if (n >= 0) {
			while (n > 1) {
				fac = fac * n;
				n = n - 1;
			}
			return fac;
		} else {
			return 0;
		}
	}
}
