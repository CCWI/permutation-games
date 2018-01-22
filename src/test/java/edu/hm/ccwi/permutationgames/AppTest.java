package edu.hm.ccwi.permutationgames;

import junit.framework.TestCase;
import java.util.ArrayList;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

public class AppTest extends TestCase
{
    public void testFindOneSolution()
    {
        ArrayList<String> resultSet1 = new ArrayList();
        ArrayList<String> set1 = new ArrayList();

        set1.add("03 | 04 | 08 | 15\n13 | 10 | 06 | 01\n14 | 09 | 05 | 02\n   | 07 | 11 | 12\n");

        Game30 game = new Game30();
        resultSet1 = game.findSolutions(new Integer[]{3,4,8,15,13,10,6,1,14,9,5,2}, new Integer[]{7,11,12}, null);

        assertEquals(set1, resultSet1);
    }

    public void testFindMultipleSolutions()
    {
        ArrayList<String> resultSet1 = new ArrayList();
        ArrayList<String> set1 = new ArrayList();
        ArrayList<String> resultSet2 = new ArrayList();
        ArrayList<String> set2 = new ArrayList();

        set1.add("03 | 04 | 10 | 13\n15 | 06 | 08 | 01\n12 | 09 | 07 | 02\n   | 11 | 05 | 14\n");
        set1.add("03 | 04 | 10 | 13\n15 | 08 | 06 | 01\n12 | 11 | 05 | 02\n   | 07 | 09 | 14\n");

        set2.add("05 | 06 | 09 | 10\n11 | 03 | 12 | 04\n14 | 08 | 07 | 01\n   | 13 | 02 | 15\n");
        set2.add("05 | 06 | 09 | 10\n11 | 08 | 07 | 04\n14 | 13 | 02 | 01\n   | 03 | 12 | 15\n");
        set2.add("05 | 06 | 09 | 10\n14 | 03 | 12 | 01\n11 | 08 | 07 | 04\n   | 13 | 02 | 15\n");
        set2.add("05 | 06 | 09 | 10\n14 | 08 | 07 | 01\n11 | 13 | 02 | 04\n   | 03 | 12 | 15\n");

        Game30 game = new Game30();
        resultSet1 = game.findSolutions(new Integer[]{3,4,10,13,15}, new Integer[]{1,2,5,6,7,8,9,11,12,14}, null);
        resultSet2 = game.findSolutions(new Integer[]{5,6,9,10}, new Integer[]{1,2,3,4,7,8,11,12,13,14,15}, null);

        assertEquals(set1.size(), 2);
        assertEquals(set1, resultSet1);

        assertEquals(set2.size(), 4);
        assertEquals(set2, resultSet2);
    }

    public void testInvalidArguments() {

        ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outContent));

        String help = "Bedienung des Programmes:" + "\n\t- \"game30\": Spielt das Ratespiel 30"
                + "\n\t\t- Für eine lokale Berechnung die Anzahl der parallel "
                + "\n\t\t  auszufuehrenden Threads, welche zwischen 1 und 15 (inklusive) " + "\n\t\t  liegen muss"
                + "\n\t\t\t- Verzeichnis des lokalen Dateisystems, in welches die"
                + "\n\t\t\t  Ausgabedatei geschrieben werden soll"
                + "\n\t\t- \"hadoop-mapreduce\": Berechnung mit Apache Hadoop MapReduce"
                + "\n\t\t- \"hadoop-spark\": Berechnung mit Apache Hadoop Spark"
                + "\n\t\t\t- Verzeichnis des Ratespiels 30 im verteilten Dateisystem"
                + "\n\t- \"availableProcessors\": Gibt die Anzahl an verfuegbaren Prozessoren aus\n";

        //no arguments
        Play.main(new String[]{});
        assertEquals(help, outContent.toString());
        outContent.reset();

        //unknown argument
        Play.main(new String[]{"play"});
        assertEquals(help, outContent.toString());
        outContent.reset();

        //only game30
        Play.main(new String[]{"game30"});
        assertEquals(help, outContent.toString());
        outContent.reset();

        //game30 with unknown mode to play
        Play.main(new String[]{"game30", "ehhh"});
        assertEquals(help, outContent.toString());
        outContent.reset();

        //more then 3 args
        Play.main(new String[]{"game30", "hadoop-spark", "/user/hadoop/input", "/user/hadoop/output"});
        assertEquals(help, outContent.toString());
        outContent.reset();

        Play.main(new String[]{"game30", "1", "/user/hadoop/input", "/user/hadoop/output"});
        assertEquals(help, outContent.toString());
        outContent.reset();

        //hadoop-spark without path
        Play.main(new String[]{"game30", "hadoop-spark"});
        assertEquals(help, outContent.toString());
        outContent.reset();

        //hadoop-mapreduce without path
        Play.main(new String[]{"game30", "hadoop-mapreduce"});
        assertEquals(help, outContent.toString());
        outContent.reset();

        //Thread game without path
        Play.main(new String[]{"game30", "5"});
        assertEquals(help, outContent.toString());
        outContent.reset();

        //Game30 with invalid number of threads
        Play.main(new String[]{"game30", "0", "/user/home"});
        assertEquals(help, outContent.toString());
        outContent.reset();

        Play.main(new String[]{"game30", "16", "/user/home"});
        assertEquals(help, outContent.toString());
        outContent.reset();
    }
}
