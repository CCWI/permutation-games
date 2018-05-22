package edu.hm.ccwi.permutationgames;

import java.util.ArrayList;

import org.junit.Test;

import junit.framework.TestCase;

public class AppTest extends TestCase
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
        actualResult = game.findSolutions(new Integer[]{3,4,8,15,13,10,6,1,14,9,5,2}, new Integer[]{7,11,12}, null);

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
        actualResult1 = game.findSolutions(new Integer[]{3,4,10,13,15}, new Integer[]{1,2,5,6,7,8,9,11,12,14}, null);
        actualResult2 = game.findSolutions(new Integer[]{5,6,9,10}, new Integer[]{1,2,3,4,7,8,11,12,13,14,15}, null);

        assertEquals(expectedResult1.size(), 2);
        assertEquals(expectedResult1, actualResult1);

        assertEquals(expectedResult2.size(), 4);
        assertEquals(expectedResult2, actualResult2);
    }

//    @Test
//    public void testInvalidArguments() {
//
//        ByteArrayOutputStream output = new ByteArrayOutputStream();
//        System.setOut(new PrintStream(output));
//
//        String help = Play.HELP_TEXT;
//
//        // no arguments
//        Play.main(new String[]{});
//        assertEquals(help.toString(), output.toString());
//        output.reset();
//
//        // unknown argument
//        Play.main(new String[]{"play"});
//        assertEquals(help, output.toString());
//        output.reset();
//
//        // only game30
//        Play.main(new String[]{"game30"});
//        assertEquals(help, output.toString());
//        output.reset();
//
//        // game30 with unknown mode to play
//        Play.main(new String[]{"game30", "ehhh"});
//        assertEquals(help, output.toString());
//        output.reset();
//
//        // more then 3 args
//        Play.main(new String[]{"game30", "hadoop-spark", "/user/hadoop/input", "/user/hadoop/output"});
//        assertEquals(help, output.toString());
//        output.reset();
//
//        Play.main(new String[]{"game30", "1", "/user/hadoop/input", "/user/hadoop/output"});
//        assertEquals(help, output.toString());
//        output.reset();
//
//        // hadoop-spark without path
//        Play.main(new String[]{"game30", "hadoop-spark"});
//        assertEquals(help, output.toString());
//        output.reset();
//
//        // hadoop-mapreduce without path
//        Play.main(new String[]{"game30", "hadoop-mapreduce"});
//        assertEquals(help, output.toString());
//        output.reset();
//
//        // Thread game without path
//        Play.main(new String[]{"game30", "5"});
//        assertEquals(help, output.toString());
//        output.reset();
//
//        // Game30 with invalid number of threads
//        Play.main(new String[]{"game30", "0", "/user/home"});
//        assertEquals(help, output.toString());
//        output.reset();
//
//        Play.main(new String[]{"game30", "16", "/user/home"});
//        assertEquals(help, output.toString());
//        output.reset();
//    }
}
