package com.example.kafka;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Unit test for simple App.
 */
public class KMergeTest
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public KMergeTest(String testName )
    {
        super( testName );
    }


    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( KMergeTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testAppBasic()
    {
        ArrayList<Integer>[] testArrayList = new ArrayList[3]; // Create an array of ArrayLists with size 5
        testArrayList[0] = new ArrayList<>(Arrays.asList(1,3,9));
        testArrayList[1] = new ArrayList<>(Arrays.asList(4,6,8));
        testArrayList[2] = new ArrayList<>(Arrays.asList(2,5,7));

        ArrayList<Integer> result = Merge.merge(testArrayList);
        ArrayList<Integer> properResult = new ArrayList<>(Arrays.asList(1,2,3,4,5,6,7,8,9));

        assertTrue(result.equals(properResult) );
    }

    public void testAppMissingEntries()
    {
        ArrayList<Integer>[] testArrayList = new ArrayList[3]; // Create an array of ArrayLists with size 5
        testArrayList[0] = new ArrayList<>(Arrays.asList(1,9));
        testArrayList[1] = new ArrayList<>(Arrays.asList(4,6,8));
        testArrayList[2] = new ArrayList<>(Arrays.asList(2,5,7));

        ArrayList<Integer> result = Merge.merge(testArrayList);
        ArrayList<Integer> properResult = new ArrayList<>(Arrays.asList(1,2,4,5,6,7,8,9));

        assertTrue(result.equals(properResult) );
    }
}

