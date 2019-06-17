package edu.hm.ccwi.permutationgames;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;
import org.junit.Test;

import edu.hm.ccwi.permutationgames.Game30.Game30Spark;
import junit.framework.TestCase;

public class PlayTest extends TestCase
{
    @Test
	public void testFindOneSolution()
    {
        ArrayList<String> actualResult = new ArrayList<String>();
        ArrayList<String> expectedResult = new ArrayList<String>();

        expectedResult.add("03 | 04 | 08 | 15\n"
        				+ "13 | 10 | 06 | 01\n"
        				+ "14 | 09 | 05 | 02\n"
        				+ "   | 07 | 11 | 12\n");

        Game30 game = new Game30();
        actualResult = game.findSolutions(new Integer[]{3,4,8,15,13,10,6,1,14,9,5,2}, new Integer[]{7,11,12});

        assertEquals(expectedResult, actualResult);
    }

    @Test
    public void testFindMultipleSolutions()
    {
        ArrayList<String> actualResult1 = new ArrayList<String>();
        ArrayList<String> expectedResult1 = new ArrayList<String>();
        ArrayList<String> actualResult2 = new ArrayList<String>();
        ArrayList<String> expectedResult2 = new ArrayList<String>();

        expectedResult1.add("03 | 04 | 10 | 13\n"
        				+ "15 | 06 | 08 | 01\n"
        				+ "12 | 09 | 07 | 02\n"
        				+ "   | 11 | 05 | 14\n");
        expectedResult1.add("03 | 04 | 10 | 13\n"
        				+ "15 | 08 | 06 | 01\n"
        				+ "12 | 11 | 05 | 02\n"
        				+ "   | 07 | 09 | 14\n");

        expectedResult2.add("05 | 06 | 09 | 10\n"
        				+ "11 | 03 | 12 | 04\n"
        				+ "14 | 08 | 07 | 01\n"
        				+ "   | 13 | 02 | 15\n");
        expectedResult2.add("05 | 06 | 09 | 10\n"
        				+ "11 | 08 | 07 | 04\n"
        				+ "14 | 13 | 02 | 01\n"
        				+ "   | 03 | 12 | 15\n");
        expectedResult2.add("05 | 06 | 09 | 10\n"
        				+ "14 | 03 | 12 | 01\n"
        				+ "11 | 08 | 07 | 04\n"
        				+ "   | 13 | 02 | 15\n");
        expectedResult2.add("05 | 06 | 09 | 10\n"
        				+ "14 | 08 | 07 | 01\n"
        				+ "11 | 13 | 02 | 04\n"
        				+ "   | 03 | 12 | 15\n");

        Game30 game = new Game30();
        actualResult1 = game.findSolutions(new Integer[]{3,4,10,13,15}, new Integer[]{1,2,5,6,7,8,9,11,12,14});
        actualResult2 = game.findSolutions(new Integer[]{5,6,9,10}, new Integer[]{1,2,3,4,7,8,11,12,13,14,15});

        assertEquals(expectedResult1.size(), actualResult1.size());
        assertEquals(expectedResult1, actualResult1);

        assertEquals(expectedResult2.size(), expectedResult2.size());
        assertEquals(expectedResult2, actualResult2);
    }
    
    @Test
    public void testSparkFindOneSolution() {
    	ArrayList<String> actualResult1 = new ArrayList<String>();
        ArrayList<String> expectedResult1 = new ArrayList<String>();
        
        expectedResult1.add("Loesung 1:\n"
        				+ "03 | 04 | 08 | 15\n"
						+ "13 | 10 | 06 | 01\n"
						+ "14 | 09 | 05 | 02\n"
						+ "   | 07 | 11 | 12\n");
        expectedResult1.add("Loesung 2:\n"
        				+ "03 | 04 | 08 | 15\n"
        				+ "14 | 09 | 05 | 02\n"
        				+ "13 | 10 | 06 | 01\n"
        				+ "   | 07 | 11 | 12\n");
        expectedResult1.add("Loesung 3:\n"
        				+ "03 | 04 | 09 | 14\n"
						+ "15 | 08 | 05 | 02\n"
						+ "12 | 11 | 06 | 01\n"
						+ "   | 07 | 10 | 13\n");
        expectedResult1.add("Loesung 4:\n"
        				+ "03 | 04 | 10 | 13\n"
						+ "12 | 07 | 09 | 02\n"
						+ "15 | 08 | 06 | 01\n"
						+ "   | 11 | 05 | 14\n");
        expectedResult1.add("Loesung 5:\n"
        				+ "03 | 04 | 10 | 13\n"
						+ "15 | 06 | 08 | 01\n"
						+ "12 | 09 | 07 | 02\n"
						+ "   | 11 | 05 | 14\n");
        expectedResult1.add("Loesung 6:\n"
        				+ "03 | 04 | 10 | 13\n"
						+ "15 | 08 | 06 | 01\n"
						+ "12 | 11 | 05 | 02\n"
						+ "   | 07 | 09 | 14\n");
        expectedResult1.add("Loesung 7:\n"
        				+ "03 | 04 | 11 | 12\n"
						+ "13 | 07 | 08 | 02\n"
						+ "14 | 10 | 05 | 01\n"
						+ "   | 09 | 06 | 15\n");
        expectedResult1.add("Loesung 8:\n"
        				+ "03 | 04 | 11 | 12\n"
						+ "14 | 07 | 08 | 01\n"
						+ "13 | 10 | 05 | 02\n"
						+ "   | 09 | 06 | 15\n");
        
    	// Statisches Array mit den möglichen Kombinationen der ersten beiden Spielsteine
    	List<Integer[]> initList = new ArrayList<Integer[]>();
    	
    	// Konfiguration für Spark
    	SparkConf sparkConf = new SparkConf();
    	sparkConf.setAppName(Play.JOB_NAME);
    	sparkConf.set("spark.master", "local[2]");
    	
    	// Spark Context für die Ausfürhung mit Spark
    	JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
    	
    	// Hinzufügen der ersten beiden Steine
    	initList.add(new Integer[]{3,4});
    	
    	//Die Liste aller Aussgangssituationen wird verteilt im Cluster abgelegt
        JavaRDD<Integer[]> data = sparkContext.parallelize(initList, initList.size());
        LongAccumulator numberOfPermutations = sparkContext.sc().longAccumulator();
        LongAccumulator numberOfSolutionsFound = sparkContext.sc().longAccumulator();

        // Für jedes Element der Liste wird ein neues Game30 initialisiert
        // und die Methode playSpark aufgerufen.
        // Der Rückgabewert von playSpark ist wiederum eine Liste
        // aller Lösungen der jeweiligen Ausgangssituation.
		JavaRDD<String> solutionText = data.flatMap(Game30Spark.calculateSolutions(numberOfPermutations));
		
		// Die gefundenen Lösungen werden mit einem Index versehen
		JavaPairRDD<String, Long> solutionsIndexed = solutionText.zipWithIndex();
					
		// Der Index wird in eine laufende Lösungsnummer überführt
		JavaRDD<String> solutionsEnumerated = solutionsIndexed.map(Game30Spark.enumerateSolutions());
		
		// Mit dem Aufruf der Aktion (Action) saveAsTextFile werden
		// die Ergebnisse tatsächlich berechnet und im Treiberprogramm gesammelt.
		List<String> solutions = solutionsEnumerated.repartition(1).collect();
		actualResult1.addAll(solutions);
        
        assertEquals(numberOfSolutionsFound.value(), Long.valueOf(expectedResult1.size()));
        assertEquals(expectedResult1.size(), actualResult1.size());
        assertEquals(expectedResult1, actualResult1);

        // Schließen des Spark-Kontext
        sparkContext.close();
    }
}
