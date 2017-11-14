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

package org.apache.flink.table.runtime.utils;

import org.apache.flink.table.functions.ScalarFunction;

import java.util.Arrays;

/**
 * Test scalar functions.
 */
public class JavaUserDefinedScalarFunctions {

	/**
	 * Increment input.
	 */
	public static class JavaFunc0 extends ScalarFunction {
		public long eval(Long l) {
			return l + 1;
		}
	}

	/**
	 * Concatenate inputs as strings.
	 */
	public static class JavaFunc1 extends ScalarFunction {
		public String eval(Integer a, int b,  Long c) {
			return a + " and " + b + " and " + c;
		}
	}

	/**
	 * Append product to string.
	 */
	public static class JavaFunc2 extends ScalarFunction {
		public String eval(String s, Integer... a) {
			int m = 1;
			for (int n : a) {
				m *= n;
			}
			return s + m;
		}
	}

	/**
	 * Test overloading.
	 */
	public static class JavaFunc3 extends ScalarFunction {
		public int eval(String a, int... b) {
			return b.length;
		}

		public String eval(String c) {
			return c;
		}
	}

	/**
	 * Concatenate arrays as strings.
	 */
	public static class JavaFunc4 extends ScalarFunction {
		public String eval(Integer[] a, String[] b) {
			return Arrays.toString(a) + " and " + Arrays.toString(b);
		}
	}

	/**
	 * Determines Quarter (1,2,3,4) of a given date.
	 */
	public static class JavaFunc5 extends ScalarFunction {
		public int eval(java.sql.Date date) {
			int month = date.getMonth();
			int quarter = 0;
			switch (month) {
				case 0: case 1: case 2:
					quarter = 1;
					break;
				case 3: case 4: case 5:
					quarter = 2;
					break;
				case 6: case 7: case 8:
					quarter = 3;
					break;
				case 9: case 10: case 11:
					quarter = 4;
					break;
				default:
					break;
			}
			return quarter;
		}
	}

	/**
	 * Converts a string in JDBC date escape format to a Date value.
	 */
	public static class JavaFunc6 extends ScalarFunction {
		public java.sql.Date eval(String s) {
			java.sql.Date date = null;
			try {
				date = java.sql.Date.valueOf(s);
			} catch (IllegalArgumentException e) {
				date = new java.sql.Date(0);
			}
			return date;
		}
	}

	/**
	 * Determines Month (1..12) of a given date.
	 */
	public static class JavaFunc7 extends ScalarFunction {
		public int eval(java.sql.Date date) {
			int month = date.getMonth();
			return month + 1;
		}
	}
}
