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

package org.apache.flink.table.tpcds.utils;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Result comparator for TPC-DS test according to the TPC-DS standard specification v2.11.0.
 * Query result in Flink for all TPC-DS queries is strictly matched with TPC-DS answer set.
 */
public class TpcdsResultComparator {

	private static final int VALIDATE_QUERY_NUM = 103;
	private static final List<String> VALIDATE_QUERIES = Arrays.asList(
			"1", "2", "3", "4", "5", "6", "7", "8", "9", "10",
			"11", "12", "13", "14a", "14b", "15", "16", "17", "18", "19", "20",
			"21", "22", "23a", "23b", "24a", "24b", "25", "26", "27", "28", "29", "30",
			"31", "32", "33", "34", "35", "36", "37", "38", "39a", "39b", "40",
			"41", "42", "43", "44", "45", "46", "47", "48", "49", "50",
			"51", "52", "53", "54", "55", "56", "57", "58", "59", "60",
			"61", "62", "63", "64", "65", "66", "67", "68", "69", "70",
			"71", "72", "73", "74", "75", "76", "77", "78", "79", "80",
			"81", "82", "83", "84", "85", "86", "87", "88", "89", "90",
			"91", "92", "93", "94", "95", "96", "97", "98", "99"
	);

	private static final String REGEX_SPLIT_BAR = "\\|";
	private static final String FILE_SEPARATOR = "/";
	private static final String RESULT_SUFFIX = ".ans";
	private static final double TOLERATED_DOUBLE_DEVIATION = 0.01d;

	public static void main(String[] args) throws Exception {
		ParameterTool params = ParameterTool.fromArgs(args);
		String expectedDir = params.getRequired("expectedDir");
		String actualDir = params.getRequired("actualDir");
		int passCnt = 0;
		for (String queryId : VALIDATE_QUERIES) {
			File expectedFile = new File(expectedDir + FILE_SEPARATOR + queryId + RESULT_SUFFIX);
			File actualFile = new File(actualDir + FILE_SEPARATOR + queryId + RESULT_SUFFIX);

			if (compareResult(expectedFile, actualFile)) {
				passCnt++;
				System.out.println("[INFO] validate success, file: " + expectedFile.getName() + " total query:" + VALIDATE_QUERY_NUM + " passed query:" + passCnt);
			} else {
				System.out.println("[ERROR] validate fail, file: " + expectedFile.getName() + "\n");
			}
		}
		if (passCnt == VALIDATE_QUERY_NUM) {
			System.exit(0);
		}
		System.exit(1);
	}

	private static boolean compareResult(File expectedFile, File actualFile) throws Exception {
		BufferedReader expectedReader = new BufferedReader(new FileReader(expectedFile));
		BufferedReader actualReader = new BufferedReader(new FileReader(actualFile));

		int expectedLineNum = 0;
		int actualLineNum = 0;

		String expectedLine, actualLine;
		while ((expectedLine = expectedReader.readLine()) != null &&
				(actualLine = actualReader.readLine()) != null) {
			expectedLineNum++;
			actualLineNum++;

			// reslut top 8 line of query 34,
			// result line  2、3  0f query 77
			// result line 18、 19 of query 79
			// have different order with answer set, because of Flink keep nulls last for DESC, nulls first for ASC.
			// it's still up to TPC-DS standard.
			if ("34.ans".equals(expectedFile.getName()) && expectedLineNum == 1) {
				List<String> expectedTop8Line = new ArrayList<>(8);
				List<String> actualTop8Line = new ArrayList<>(8);

				String expectedLine2 = expectedReader.readLine();
				expectedLineNum++;
				while (expectedLineNum < 8) {
					expectedTop8Line.add(expectedReader.readLine());
					expectedLineNum++;
				}
				expectedTop8Line.add(expectedLine);
				expectedTop8Line.add(expectedLine2);

				actualTop8Line.add(actualLine);
				while (actualLineNum < 8) {
					actualTop8Line.add(actualReader.readLine());
					actualLineNum++;
				}
				for (int i = 0; i < 8; i++) {
					if (!isEqualLine(expectedTop8Line.get(i), actualTop8Line.get(i), i)) {
						return false;
					}
				}
				continue;
			}
			if ("77.ans".equals(expectedFile.getName()) && expectedLineNum == 2) {
				String expectedLine3 = expectedReader.readLine();
				String actualLine3 = actualReader.readLine();
				expectedLineNum++;
				actualLineNum++;

				if (isEqualLine(expectedLine, actualLine3, 2) && isEqualLine(expectedLine3, actualLine, 3)) {
					continue;
				}
				if (isEqualLine(expectedLine, actualLine, 2) && isEqualLine(expectedLine3, actualLine3, 3)) {
					continue;
				}
				return false;
			}

			if ("79.ans".equals(expectedFile.getName()) && expectedLineNum == 18) {
				String expectedLine19 = expectedReader.readLine();
				String actualLine19 = actualReader.readLine();
				expectedLineNum++;
				actualLineNum++;

				if (isEqualLine(expectedLine, actualLine, 18) && isEqualLine(expectedLine19, actualLine19, 19)) {
					continue;
				}
				if (isEqualLine(expectedLine, actualLine19, 18) && isEqualLine(expectedLine19, actualLine, 19)) {
					continue;
				}
				return false;
			}

			if (!isEqualLine(expectedLine, actualLine, expectedLineNum)) {
				return false;
			}
		}

		while (expectedReader.readLine() != null) {
			expectedLineNum++;
		}
		while (actualReader.readLine() != null) {
			actualLineNum++;
		}

		if (expectedLineNum != actualLineNum) {
			System.out.println(
					"[ERROR] Incorrect number of lines! Expecting " + expectedLineNum +
							" lines, but found " + actualLineNum + " lines.");
			return false;
		}

		return true;
	}

	private static boolean isEqualLine(String expectedLine, String actualLine, int lineNum) {
		String[] expected = expectedLine.split(REGEX_SPLIT_BAR, -1);
		String[] actual = actualLine.split(REGEX_SPLIT_BAR, -1);
		if (expected.length != actual.length) {
			System.out.println(
					"[ERROR] Incorrect number of columns! Expecting " + expected.length +
							" columns, but found " + actual.length + " columns.");
			return false;
		}
		for (int i = 0; i < expected.length; i++) {
			if (!isEqualCol(expected[i].trim(), actual[i].trim())) {
				System.out.println("[ERROR] Incorrect result on line " + lineNum + " column " + (i + 1) +
						"! Expecting " + expected[i] + ", but found " + actual[i] + ".");
				return false;
			}
		}
		return true;
	}

	private static boolean isEqualCol(String expected, String actual) {
		if (isEqualNumber(expected, actual)) {
			return true;
		} else if (isEqualNull(expected, actual)) {
			return true;
		} else {
			return expected.equals(actual);
		}
	}

	private static boolean isEqualNumber(String expected, String actual) {
		try {
			Double expectVal = new Double(expected);
			Double actualVal = new Double(actual);
			if (expectVal.equals(actualVal)) {
				return true;
			}
			if (Math.abs(expectVal - actualVal) <= TOLERATED_DOUBLE_DEVIATION) {
				return true;
			}
		} catch (NumberFormatException e) {
			// Column data can not convert to double, return false instead of throwing NumberFormatException.
			return false;
		}
		return false;
	}

	private static boolean isEqualNull(String expected, String actual) {
		if (null == actual || "".equals(actual)) {
			if ("%".equals(expected) || "-".equals(expected) || "NULL".equals(expected) || "[NULL]".equals(expected)) {
				return true;
			}
		}
		return false;
	}
}
