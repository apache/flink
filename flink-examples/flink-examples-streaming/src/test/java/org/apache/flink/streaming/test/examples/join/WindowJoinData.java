/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.test.examples.join;

/**
 * Class with sample data for window join examples.
 */
public class WindowJoinData {

	public static final String GRADES_INPUT = "0,john,5\n" + "0,tom,3\n" + "0,alice,1\n" + "0,grace,5\n" +
			"1,john,4\n" + "1,bob,1\n" + "1,alice,2\n" + "1,alice,3\n" + "1,bob,5\n" + "1,alice,3\n" + "1,tom,5\n" +
			"2,john,2\n" + "2,john,1\n" + "2,grace,2\n" + "2,jerry,2\n" + "2,tom,4\n" + "2,bob,4\n" + "2,bob,2\n" +
			"3, tom,2\n" + "3,alice,5\n" + "3,grace,5\n" + "3,grace,1\n" + "3,alice,1\n" + "3,grace,3\n" + "3,tom,1\n" +
			"4,jerry,5\n" + "4,john,3\n" + "4,john,4\n" + "4,john,1\n" + "4,jerry,3\n" + "4,grace,3\n" + "4,bob,3\n" +
			"5,john,3\n" + "5,jerry,4\n" + "5,tom,5\n" + "5,tom,4\n" + "5,john,2\n" + "5,jerry,1\n" + "5,bob,1\n" +
			"6,john,5\n" + "6,grace,4\n" + "6,tom,5\n" + "6,john,4\n" + "6,tom,1\n" + "6,grace,1\n" + "6,john,2\n" +
			"7,jerry,3\n" + "7,jerry,5\n" + "7,tom,2\n" + "7,tom,2\n" + "7,alice,4\n" + "7,tom,4\n" + "7,jerry,4\n" +
			"8,john,3\n" + "8,grace,4\n" + "8,tom,3\n" + "8,jerry,4\n" + "8,john,5\n" + "8,john,4\n" + "8,jerry,1\n" +
			"9,john,5\n" + "9,alice,2\n" + "9,tom,1\n" + "9,alice,5\n" + "9,grace,4\n" + "9,bob,4\n" + "9,jerry,1\n" +
			"10,john,5\n" + "10,tom,4\n" + "10,tom,5\n" + "10,jerry,5\n" + "10,tom,1\n" + "10,grace,3\n" + "10,bob,5\n" +
			"11,john,1\n" + "11,alice,1\n" + "11,grace,3\n" + "11,grace,1\n" + "11,jerry,1\n" + "11,jerry,4\n" +
			"12,bob,4\n" + "12,alice,3\n" + "12,tom,5\n" + "12,alice,4\n" + "12,alice,4\n" + "12,grace,4\n" + "12,john,5\n" +
			"13,john,5\n" + "13,grace,4\n" + "13,tom,4\n" + "13,john,4\n" + "13,john,5\n" + "13,alice,5\n" + "13,jerry,5\n" +
			"14,john,3\n" + "14,tom,5\n" + "14,jerry,4\n" + "14,grace,4\n" + "14,john,3\n" + "14,bob,2";

	public static final String SALARIES_INPUT = "0,john,6469\n" + "0,jerry,6760\n" + "0,jerry,8069\n" +
			"1,tom,3662\n" + "1,grace,8427\n" + "1,john,9425\n" + "1,bob,9018\n" + "1,john,352\n" + "1,tom,3770\n" +
			"2,grace,7622\n" + "2,jerry,7441\n" + "2,alice,1468\n" + "2,bob,5472\n" + "2,grace,898\n" +
			"3,tom,3849\n" + "3,grace,1865\n" + "3,alice,5582\n" + "3,john,9511\n" + "3,alice,1541\n" +
			"4,john,2477\n" + "4,grace,3561\n" + "4,john,1670\n" + "4,grace,7290\n" + "4,grace,6565\n" +
			"5,tom,6179\n" + "5,tom,1601\n" + "5,john,2940\n" + "5,bob,4685\n" + "5,bob,710\n" + "5,bob,5936\n" +
			"6,jerry,1412\n" + "6,grace,6515\n" + "6,grace,3321\n" + "6,tom,8088\n" + "6,john,2876\n" +
			"7,bob,9896\n" + "7,grace,7368\n" + "7,grace,9749\n" + "7,bob,2048\n" + "7,alice,4782\n" +
			"8,alice,3375\n" + "8,tom,5841\n" + "8,bob,958\n" + "8,bob,5258\n" + "8,tom,3935\n" + "8,jerry,4394\n" +
			"9,alice,102\n" + "9,alice,4931\n" + "9,alice,5240\n" + "9,jerry,7951\n" + "9,john,5675\n" +
			"10,bob,609\n" + "10,alice,5997\n" + "10,jerry,9651\n" + "10,alice,1328\n" + "10,bob,1022\n" +
			"11,grace,2578\n" + "11,jerry,9704\n" + "11,tom,4476\n" + "11,grace,3784\n" + "11,alice,6144\n" +
			"12,bob,6213\n" + "12,alice,7525\n" + "12,jerry,2908\n" + "12,grace,8464\n" + "12,jerry,9920\n" +
			"13,bob,3720\n" + "13,bob,7612\n" + "13,alice,7211\n" + "13,jerry,6484\n" + "13,alice,1711\n" +
			"14,jerry,5994\n" + "14,grace,928\n" + "14,jerry,2492\n" + "14,grace,9080\n" + "14,tom,4330\n" +
			"15,bob,8302\n" + "15,john,4981\n" + "15,tom,1781\n" + "15,grace,1379\n" + "15,jerry,3700\n" +
			"16,jerry,3584\n" + "16,jerry,2038\n" + "16,jerry,3902\n" + "16,tom,1336\n" + "16,jerry,7500\n" +
			"17,tom,3648\n" + "17,alice,2533\n" + "17,tom,8685\n" + "17,bob,3968\n" + "17,tom,3241\n" + "17,bob,7461\n" +
			"18,jerry,2138\n" + "18,alice,7503\n" + "18,alice,6424\n" + "18,tom,140\n" + "18,john,9802\n" +
			"19,grace,2977\n" + "19,grace,889\n" + "19,john,1338";

	/** Utility class, should not be instantiated. */
	private WindowJoinData() {}
}
