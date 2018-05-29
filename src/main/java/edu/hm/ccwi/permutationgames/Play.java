package edu.hm.ccwi.permutationgames;

import java.io.IOException;
import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import edu.hm.ccwi.permutationgames.Game30.Game30Mapper;
import edu.hm.ccwi.permutationgames.Game30.Game30Reducer;
import edu.hm.ccwi.permutationgames.Game30.Game30Spark;

/**
 * Mit dieser Klasse koennen verschiedene Spiel mit Permutationen auf Basis
 * unterschiedlicher Algorithmen gespielt bzw. geloest werden.
 *
 * @author Alexander Doeschl
 */
public class Play {

	/**
	 * Argument für die Anzeige der Anzahl der verfügbaren Prozessoren
	 */
	private static final String ARGUMENT_AVAILABLE_PROCESSORS = "availableProcessors";

	/**
	 * Argument für die Berechnung mit Spark
	 */
	private static final String ARGUMENT_SPARK = "hadoop-spark";

	/**
	 * Argument für die Berechnung mit Hadoop
	 */
	private static final String ARGUMENT_HADOOP = "hadoop-mapreduce";

	/**
	 * Argument für die Berechnung von Spiel 30
	 */
	private static final String ARGUMENT_GAME30 = "game30";

	/**
	 * Name der App (Spark) bzw. des Jobs (Hadoop)
	 */
	public static final String JOB_NAME = "Game30";
	
	/**
	 * Logger für die Ausgabe von Log-Meldungen mit Log4j
	 */
	private static Logger logger = Logger.getLogger(Play.class);
	
	/**
	 * Kurze Erklärung zu den Parameter, die mit dem Aufruf des Programms übergeben werden können.
	 */
    public static final String HELP_TEXT = "Bedienung des Programmes:" + "\n\t- \"game30\": Spielt das Ratespiel 30"
	        + "\n\t\t- Für eine lokale Berechnung die Anzahl der parallel "
	        + "\n\t\t  auszufuehrenden Threads, welche zwischen 1 und 15 (inklusive) " + "\n\t\t  liegen muss"
	        + "\n\t\t\t- Verzeichnis des lokalen Dateisystems, in welches die"
	        + "\n\t\t\t  Ausgabedatei geschrieben werden soll"
	        + "\n\t\t- \"hadoop-mapreduce\": Berechnung mit Apache Hadoop MapReduce"
	        + "\n\t\t- \"hadoop-spark\": Berechnung mit Apache Hadoop Spark"
	        + "\n\t\t\t- Verzeichnis des Ratespiels 30 im verteilten Dateisystem"
	        + "\n\t- \"availableProcessors\": Gibt die Anzahl an verfuegbaren Prozessoren aus";

	public static void main(String[] args) {

        if (args.length > 2 && args.length < 4) {
            if (args[0].equals(ARGUMENT_GAME30)) {
                if (args[1].equals(ARGUMENT_HADOOP) || args[1].equals(ARGUMENT_SPARK)) {

                    String inputDir = args[2] + "/input/";
                    String outputDir = args[2] + "/output_" + getTime() + "/";

                    try {
                        if (args[1].equals(ARGUMENT_HADOOP))
                            playGame30ApacheHadoop(inputDir, outputDir);
                        else
                            playGame30Spark(outputDir);
                    } catch (ClassNotFoundException | IOException | InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                } else {
                    String outputDir = args[2];

                    if (System.getProperty("os.name").toLowerCase().contains("win")) {
                        if (!outputDir.endsWith("\\")) {
                            outputDir += "\\";
                        }
                    } else {
                        if (!outputDir.endsWith("/")) {
                            outputDir += "/";
                        }
                    }

                    try {
                        int numThreads = Integer.parseInt(args[1]);

                        if (numThreads == 1) {
                            try {
                                playGame30SingleThreaded(outputDir + "game30_output_" + getTime());
                            } catch (IOException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }
                        } else if (numThreads > 1 && numThreads <= 15) {
                            try {
                                playGame30MultiThreaded(outputDir + "game30_output_" + getTime(), numThreads);
                            } catch (IOException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }
                        } else {
                            printHelp();
                        }
                    } catch (NumberFormatException e) {
                        printHelp();
                    }
                }
            } else {
                printHelp();
            }
        } else if (args.length > 0 && args[0].equals(ARGUMENT_AVAILABLE_PROCESSORS)) {
            System.out.println("# available processors: " + Runtime.getRuntime().availableProcessors());
        } else {
            printHelp();
        }
    }

    /**
     * Spielt das Ratespiel 30 mit einem Thread und gibt alle Loesungen in eine
     * Datei des lokalen Dateisystems aus.
     *
     * @param outputFile Pfad der Ausgabedatei, in welche die Loesungen geschrieben
     *                   werden
     * @throws IOException
     */
    private static void playGame30SingleThreaded(String outputFile) throws IOException {
        Game30 game = new Game30();
        game.playSingleThreaded(outputFile);
    }

    /**
     * Spielt das Ratespiel 30 mit n Threads (0 < n < 16) und gibt alle
     * Loesungen in eine Datei des lokalen Dateisystems aus.
     *
     * @param outputFile Pfad der Ausgabedatei, in welche alle Threads die Loesungen
     *                   schreiben
     * @param numThreads Anzahl der parallel auszufuehrenden Threads, welche zwischen 1
     *                   und 15 (inklusive) liegen muss
     * @throws IOException
     */
    private static void playGame30MultiThreaded(String outputFile, int numThreads) throws IOException {
        Game30 game = new Game30();
        game.playMultiThreaded(outputFile, numThreads);
    }

    /**
     * Spielt das Ratespiel 30 mit Hadoop und gibt alle Loesungen in eine Datei
     * des verteilten Dateisystems aus. Hierfuer wird zunaechst eine Liste aller
     * Moeglichkeiten erstellt, wie die ersten beiden Felder des Spiels belegt
     * werden koennen. Diese Liste ist im verteilten Dateisystem abgelegt und
     * dient als Input der Hadoop-Verarbeitung. Die Parallelisierung erfolgt,
     * indem jedem Zahlenpaar dieser Liste eine Mapper-Instanz zugeteilt wird,
     * welche die Permutation der restlichen Felder und somit die
     * Loesungsfindung uebernimmt. Die Ergebnisse werden in einer
     * Reducer-Instanz nummeriert zusammengefasst und im Anschluss von Hadoop
     * ins verteilte Dateisystem ausgegeben.
     *
     * @param inputDir  Ordner im verteilten Dateisystem, welcher die Liste der
     *                  statischen Spielsteine enthalten soll, welche wiederum den
     *                  Input der Hadoop-Verarbeitung darstellt
     * @param outputDir Ordner im verteilten Dateisystem, in welchen die Datei mit den
     *                  gefundenen Loesungen ausgegeben wird
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     * @see Game30Mapper
     * @see Game30Reducer
     */
    private static void playGame30ApacheHadoop(String inputDir, String outputDir)
            throws IOException, ClassNotFoundException, InterruptedException {
        // Schreibt die Liste der statischen Spielsteine in das
        // Input-Verzeichnis
        writeGame30InputToDistributedFS(inputDir);

        // Ein Hadoop-Job-Objekt buendelt alle Informationen einer
        // Hadoop-Verarbeitung und kann von einem Hadoop-Cluster ausgefuehrt
        // werden
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, JOB_NAME);

        job.setJarByClass(Play.class);

        // Setzt die Mapper- und die Reducer-Klasse des Hadoop-Jobs
        job.setMapperClass(Game30Mapper.class);
        job.setReducerClass(Game30Reducer.class);

        // Gibt an, dass nur ein Reduce-Task verwendet werden soll, wodurch alle
        // gefundenen Loesungen in nur einer Ausgabedatei ausgegeben werden
        job.setNumReduceTasks(1);

        // Setzt die Typen der Ausgaben der Map- und der Reduce-Phase
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // Gibt an, dass jede Zeile der Input-Datei an eine neue Mapper-Instanz
        // zu uebergeben ist
        job.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.setNumLinesPerSplit(job, 1);

        // Setzt das Input- sowie das Output-Verzeichnis des Hadoop-Jobs
        // (verteiltes Dateisystem)
        NLineInputFormat.addInputPath(job, new Path(inputDir));
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        // Reicht den Hadoop-Job zur Ausfuehrung an das Cluster weiter und gibt
        // den Fortschritt der Verarbeitung aus
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * Die Methode playGame30Spark() initialisiert eine Liste mit 210 Ausgangssituationen
     * des Spiels Game30, indem jeweils die ersten zwei Steine fix vorgegeben werden.
     * Diese Liste wird dem SparkContext übergeben, um anschließend mit der Methode map()
     * die verteilte Verarbeitung anzustoßen.
     * Für jede der 210 Ausganssituationen wird ein neues Game30 initialiesiert
     * und die Methode playSpark() aufgerufen.
     * Der Rückgabewert ist eine Liste aller gefunden Lösungen.
     * Das Ergebnis aller Rückgabewerte ist ein JavaRDD welches im Anschluss
     * als Textfile im HDFS abgelegt wird.
     *
     * @author Florian Gebhart
     */
    private static void playGame30Spark(String outputDir) {
        try {
        	// Statisches Array mit den möglichen Kombinationen der ersten beiden Spielsteine
        	List<Integer[]> initList = new ArrayList<Integer[]>();
        	
        	// Konfiguration für Spark
        	SparkConf sparkConf = new SparkConf();
        	sparkConf.setAppName(JOB_NAME);
        	// Lokale Ausführung, sofern kein Master gesetzt wurde
        	if (!sparkConf.contains("spark.master")) {
        		sparkConf.set("spark.master", "local[2]");
        	}
        	// Spark Context für die Ausfürhung mit Spark
            JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

            // Die ersten beiden Steine werden als statisches Array fest definiert
            // [1,2] - [1,3] - ... - [14,15]
            // insgesamt 15*14=210 mögliche Ausgangssituationen.
            // Das lässt bis zu 210 parallele Prozesse zu
            for (int firstPawn = 1; firstPawn <= 15; firstPawn++) {
                for (int secondPawn = 1; secondPawn <= 15; secondPawn++) {
                    if (firstPawn != secondPawn) {
                        initList.add(new Integer[]{firstPawn, secondPawn});
                    }
                }
            }
            
            // Die Liste aller Aussgangssituationen wird verteilt im Cluster abgelegt
            JavaRDD<Integer[]> data = sparkContext.parallelize(initList, initList.size());
            LongAccumulator numberOfSolutionsFound = sparkContext.sc().longAccumulator();

            // Für jedes Element der Liste wird ein neues Game30 initialisiert
            // und die Methode playSpark aufgerufen.
            // Der Rückgabewert von playSpark ist wiederum eine Liste
            // aller Lösungen der jeweiligen Ausgangssituation.
			JavaRDD<String> solutionText = data.flatMap(Game30Spark.calculateSolutions(numberOfSolutionsFound));
            
			// Über die Methode filter werden die Elemente mit den 
			// tatsächlichen (nicht leeren) Lösungen herausgefiltert.
			JavaRDD<String> nonEmptySolutions = solutionText.filter(Game30Spark.nonEmptySolutions);
			
			// Mit dem Aufruf der Aktion (Action) saveAsTextFile werden
			// die Ergebnisse tatsächlich berechnet und als Textfile 
			// im HDFS gespeichert.
			nonEmptySolutions.saveAsTextFile(outputDir);
            
			// Log-Ausgabe der Anzahl der gefundenen Lösungen
            logger.info("Solutions found: " + numberOfSolutionsFound);

            // Schließen des Spark-Kontext
            sparkContext.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    

    /**
     * Gibt die aktuelle Zeit in der Form yy-MM-dd_HHmmss zurueck.
     *
     * @return Die aktuelle Zeit als {@link java.lang.String String}
     */
    private static String getTime() {
        DateFormat dateFormat = new SimpleDateFormat("yy-MM-dd_HHmmss");
        Date date = new Date();
        return dateFormat.format(date);
    }

    /**
     * Legt im verteilten Dateisystem eine Datei namens <code>leadPawns</code>
     * an, welche alle Moeglichkeiten haelt, wie die ersten beiden Felder des
     * Ratespiels 30 belegt werden koennen. Diese Liste umfasst 210 Zahlenpaare
     * in der Form na | nb, wobei na sowie nb natürliche Zahlen sind, fuer die 0
     * < n < 16 gilt. Jede Zeile der Ausgabe enthaelt genau ein Zahlenpaar und
     * stellt den Input fuer eine Hadoop-Mapper-Instanz dar.
     *
     * @param destinationDir Ordner im verteilten Dateisystem, in welchen die Input-Datei
     *                       ausgegeben wird
     */
    private static void writeGame30InputToDistributedFS(String destinationDir) {
        try {
            FileSystem fileSystem = FileSystem.get(URI.create(destinationDir), new Configuration());

            StringBuilder content = new StringBuilder();
            for (int firstPawn = 1; firstPawn <= 15; firstPawn++) {
                for (int secondPawn = 1; secondPawn <= 15; secondPawn++) {
                    if (firstPawn != secondPawn) {
                        content.append(firstPawn);
                        content.append("|");
                        content.append(secondPawn);
                        content.append("\n");
                    }
                }
            }

            FSDataOutputStream fsOutStream = fileSystem.create(new Path(destinationDir + "leadPawns"));

            fsOutStream.write(content.toString().getBytes());

            fsOutStream.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * Gibt die Hilfe des Programmes aus
     */
    public static void printHelp() {
        System.out.println(HELP_TEXT);
    }

}
