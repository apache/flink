package org.apache.flink.util;

import org.apache.commons.lang3.RandomUtils;

public class TestHelper
{

    public static int uniqueInt() {
        int result = uniqueInt(0, 1000);
        return result;
    }

    public static int uniqueInt(int min, int max) {
        int result = RandomUtils.nextInt(min, max);
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