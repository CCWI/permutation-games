package edu.hm.ccwi.permutationgames;

import java.io.IOException;
import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.hm.ccwi.permutationgames.Game30.Game30Mapper;
import edu.hm.ccwi.permutationgames.Game30.Game30Reducer;

/**
 * Mit dieser Klasse koennen verschiedene Spiel mit Permutationen auf Basis
 * unterschiedlicher Algorithmen gespielt bzw. geloest werden.
 * 
 * @author Alexander Doeschl
 *
 */
public class Play {

	public static void main(String[] args) {
		if (args.length > 2) {
			if (args[0].equals("game30")) {
				if (args[1].equals("hadoop")) {
					String game30Dir = args[2];

					if (!game30Dir.endsWith("/")) {
						game30Dir += "/";
					}

					String inputDir = game30Dir + "input/";
					String outputDir = game30Dir + "output_" + getTime() + "/";

					try {
						playGame30ApacheHadoop(inputDir, outputDir);
					} catch (ClassNotFoundException | IOException | InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} else {
					String outputDir = args[2];

					if (System.getProperty("os.name").toLowerCase().indexOf("win") >= 0) {
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
		} else if (args.length > 0 && args[0].equals("availableProcessors")) {
			System.out.println("# available processors: " + Runtime.getRuntime().availableProcessors());
		} else {
			printHelp();
		}
	}

	/**
	 * Spielt das Ratespiel 30 mit einem Thread und gibt alle Loesungen in eine
	 * Datei des lokalen Dateisystems aus.
	 * 
	 * @param outputFile
	 *            Pfad der Ausgabedatei, in welche die Loesungen geschrieben
	 *            werden
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
	 * @param outputFile
	 *            Pfad der Ausgabedatei, in welche alle Threads die Loesungen
	 *            schreiben
	 * @param numThreads
	 *            Anzahl der parallel auszufuehrenden Threads, welche zwischen 1
	 *            und 15 (inklusive) liegen muss
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
	 * @see Game30Mapper
	 * @see Game30Reducer
	 * @param inputDir
	 *            Ordner im verteilten Dateisystem, welcher die Liste der
	 *            statischen Spielsteine enthalten soll, welche wiederum den
	 *            Input der Hadoop-Verarbeitung darstellt
	 * @param outputDir
	 *            Ordner im verteilten Dateisystem, in welchen die Datei mit den
	 *            gefundenen Loesungen ausgegeben wird
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
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
		Job job = Job.getInstance(conf, "Game30");

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
	 * @param destinationDir
	 *            Ordner im verteilten Dateisystem, in welchen die Input-Datei
	 *            ausgegeben wird
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
		System.out.println("Bedienung des Programmes:" + "\n\t- \"game30\": Spielt das Ratespiel 30"
				+ "\n\t\t- Für eine lokale Berechnung die Anzahl der parallel "
				+ "\n\t\t  auszufuehrenden Threads, welche zwischen 1 und 15 (inklusive) " + "\n\t\t  liegen muss"
				+ "\n\t\t\t- Verzeichnis des lokalen Dateisystems, in welches die"
				+ "\n\t\t\t  Ausgabedatei geschrieben werden soll"
				+ "\n\t\t- \"hadoop\": Berechnung mit Apache Hadoop MapReduce"
				+ "\n\t\t\t- Verzeichnis des Ratespiels 30 im verteilten Dateisystem"
				+ "\n\t- \"availableProcessors\": Gibt die Anzahl an verfuegbaren Prozessoren aus");
	}

}
