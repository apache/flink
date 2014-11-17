package org.apache.flink.util;

import org.apache.commons.lang3.RandomUtils;

public class TestHelper
{

	private static int INT_MIN = 0;
	private static int INT_MAX = 1000;

    public static int uniqueInt() {
        int result = uniqueInt(INT_MIN, INT_MAX);
        return result;
    }

    public static int uniqueInt(int min, int max) {
		int result = RandomUtils.nextInt(min, max);
    	return result;
    }

    public static int uniqueInt(int[] exclude) {
    	int result = uniqueInt(INT_MIN, INT_MAX, exclude);
    	return result;
    }

    public static int uniqueInt(int min, int max, int[] exclude) {
        int result = uniqueInt(min, max);
        for (int e : exclude) {
        	if (result == e) {
        		result = uniqueInt(min, max, exclude);
        		break;
        	}
        }
        return result;
    }

    public static long uniqueLong() {
        long result = uniqueLong(0, 1000);
        return result;
    }

    public static long uniqueLong(long min, long max) {
        long result = RandomUtils.nextLong(min, max);
        return result;
    }

}