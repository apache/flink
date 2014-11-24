/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util;

import org.apache.commons.lang3.RandomUtils;

/**
 * Convenience functions to construct random objects for unit tests.
 */
public class TestHelper
{

	public static int INT_MIN = 0;
	public static int INT_MAX = 1000;

	/**
	 * Return a random integer between {@value INT_MIN} (inclusive) and {@value INT_MAX} (exclusive).  
	 */
    public static int uniqueInt() {
        int result = uniqueInt(INT_MIN, INT_MAX);
        return result;
    }

	/**
	 * Return a random integer between {@code min} (inclusive) and {@code max} (exclusive).  
	 */
    public static int uniqueInt(int min, int max) {
		int result = RandomUtils.nextInt(min, max);
    	return result;
    }

	/**
	 * Return a random integer between {@value INT_MIN} (inclusive) and {@value INT_MAX} (exclusive) that is not listed in {@code exclude}.
	 */
    public static int uniqueInt(int[] exclude) {
    	int result = uniqueInt(INT_MIN, INT_MAX, exclude);
    	return result;
    }

	/**
	 * Return a random integer between {@code min} (inclusive) and {@code max} (exclusive) that is not listed in {@code exclude}.
	 */
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

	/**
	 * Return a random long between {@value INT_MIN} (inclusive) and {@value INT_MAX} (exclusive).  
	 */
    public static long uniqueLong() {
        long result = uniqueLong(INT_MIN, INT_MAX);
        return result;
    }

	/**
	 * Return a random long between {@code min} (inclusive) and {@code max} (exclusive).  
	 */
    public static long uniqueLong(long min, long max) {
        long result = RandomUtils.nextLong(min, max);
        return result;
    }

}