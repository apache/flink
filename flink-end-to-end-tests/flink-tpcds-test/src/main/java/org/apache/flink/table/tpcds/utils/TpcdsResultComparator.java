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

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/**
 * Result comparator for TPC-DS test according to the TPC-DS standard specification v2.11.0.
 * Query result in Flink for all TPC-DS queries is strictly matched with TPC-DS answer set.
 */
@SuppressWarnings("UseOfSystemOutOrSystemErr")
public class TpcdsResultComparator {

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

	private static final HashSet<String> NULL_LITERALS = new HashSet<>(Arrays.asList("%", "-", "NULL", "[NULL]"));

	private static final String REGEX_SPLIT_BAR = "\\|";
	private static final String RESULT_SUFFIX = ".ans";
	private static final double TOLERATED_DOUBLE_DEVIATION = 0.01d;

	public static void main(String[] args) throws Exception {
		ParameterTool params = ParameterTool.fromArgs(args);
		String expectedDir = params.getRequired("expectedDir");
		String actualDir = params.getRequired("actualDir");

		int passCnt = 0;

		for (String queryId : VALIDATE_QUERIES) {
			File expectedFile = new File(expectedDir, queryId + RESULT_SUFFIX);
			File actualFile = new File(actualDir, queryId + RESULT_SUFFIX);

			if (compareResult(queryId, expectedFile, actualFile)) {
				passCnt++;
				System.out.println(String.format("[INFO] Validation succeeded for file: %s (%d/%d)",
					expectedFile.getName(), passCnt, VALIDATE_QUERIES.size()));
			} else {
				System.out.println("[ERROR] Validation failed for file: " + expectedFile.getName() + '\n');
			}
		}

		if (passCnt == VALIDATE_QUERIES.size()) {
			System.exit(0);
		}
		System.exit(1);
	}

	private static boolean compareResult(String queryId, File expectedFile, File actualFile) throws Exception {
		final String[] expectedLines = Files.readAllLines(expectedFile.toPath(), StandardCharsets.UTF_8).toArray(new String[0]);
		final String[] actualLines = Files.readAllLines(actualFile.toPath(), StandardCharsets.UTF_8).toArray(new String[0]);

		if (expectedLines.length != actualLines.length) {
			System.out.println(String.format(
				"[ERROR] Incorrect number of lines! Expecting %d lines, but found %d lines.",
				expectedLines.length, actualLines.length));

			return false;
		}

		if ("34".equals(queryId)) {
			return compareQuery34(expectedLines, actualLines);
		}
		else if ("77".equals(queryId)) {
			return compareQuery77(expectedLines, actualLines);
		}
		else if ("79".equals(queryId)) {
			return compareQuery79(expectedLines, actualLines);
		}
		else {
			return compareLinesPrintingErrors(expectedLines, actualLines, 0);
		}
	}

	// ------------------------------------------------------------------------

	private static boolean compareQuery34(String[] expectedLines, String[] actualLines) {
		// take the first two lines and move them back to lines 7 and 8
		final String expected1 = expectedLines[0];
		final String expected2 = expectedLines[1];
		System.arraycopy(expectedLines, 2, expectedLines, 0, 6);
		expectedLines[6] = expected1;
		expectedLines[7] = expected2;

		return compareLinesPrintingErrors(expectedLines, actualLines, 0);
	}

	private static boolean compareQuery77(String[] expectedLines, String[] actualLines) {
		if (!compareLinePrintingErrors(expectedLines[0], actualLines[0], 1)) {
			return false;
		}
		if (!comparePairOfLinesPrintingErrors(expectedLines[1], expectedLines[2], actualLines[1], actualLines[2], 2, 3)) {
			return false;
		}
		return compareLinesPrintingErrors(expectedLines, actualLines, 3);
	}

	private static boolean compareQuery79(String[] expectedLines, String[] actualLines) {
		if (!compareLinesPrintingErrors(expectedLines, actualLines, 0, 17)) {
			return false;
		}
		if (!comparePairOfLinesPrintingErrors(expectedLines[17], expectedLines[18], actualLines[17], actualLines[18], 18, 19)) {
			return false;
		}
		return compareLinesPrintingErrors(expectedLines, actualLines, 20);
	}

	// ------------------------------------------------------------------------

	private static boolean compareLinesPrintingErrors(String[] expectedLines, String[] actualLines, int from, int num) {
		for (int line = from; line < from + num; line++) {
			if (!compareLinePrintingErrors(expectedLines[line], actualLines[line], line)) {
				return false;
			}
		}
		return true;
	}

	private static boolean compareLinesPrintingErrors(String[] expectedLines, String[] actualLines, int from) {
		final int remaining = expectedLines.length - from;
		return compareLinesPrintingErrors(expectedLines, actualLines, from, remaining);
	}

	private static boolean comparePairOfLinesPrintingErrors(
			String expected1, String expected2,
			String actual1, String actual2,
			int lineNum1, int lineNum2) {

		final boolean matchesPair = compareLine(expected1, actual1) && compareLine(expected2, actual2);
		final boolean matchesCross = compareLine(expected2, actual1) && compareLine(expected1, actual2);

		if (!(matchesPair || matchesCross)) {
			System.out.println(String.format("[ERROR] Lines %d/%d do not match in any pairing." +
					"\n - Expected %d: %s\n - Expected %d: %s\n - Actual %d: %s\n - Actual %d: %s",
				lineNum1, lineNum2, lineNum1, expected1, lineNum2, expected2,
				lineNum1, actual1, lineNum2, actual2));

			return false;
		}

		return true;
	}

	// ------------------------------------------------------------------------

	private static boolean compareLine(String expectedLine, String actualLine) {
		return compareLineInternal(expectedLine, actualLine, -1, false);
	}

	private static boolean compareLinePrintingErrors(String expectedLine, String actualLine, int lineNum) {
		return compareLineInternal(expectedLine, actualLine, lineNum, true);
	}

	private static boolean compareLineInternal(String expectedLine, String actualLine, int lineNum, boolean printError) {
		final String[] expected = expectedLine.split(REGEX_SPLIT_BAR, -1);
		final String[] actual = actualLine.split(REGEX_SPLIT_BAR, -1);

		if (expected.length != actual.length) {
			if (printError) {
				System.out.println(String.format(
					"[ERROR] Incorrect number of columns! Expecting %d columns, but found %d columns.",
					expected.length, actual.length));
			}
			return false;
		}

		for (int i = 0; i < expected.length; i++) {
			if (!isEqualCol(expected[i].trim(), actual[i].trim())) {
				if (printError) {
					System.out.println(String.format(
						"[ERROR] Incorrect result on line %d column %d! Expecting %s but found %s.",
						lineNum, i + 1, expected[i], actual[i]));
				}
				return false;
			}
		}

		return true;
	}

	// ------------------------------------------------------------------------

	private static boolean isEqualCol(String expected, String actual) {
		return isEqualNull(expected, actual) || isEqualNumber(expected, actual) || expected.equals(actual);
	}

	private static boolean isEqualNumber(String expected, String actual) {
		try {
			double expectVal = Double.parseDouble(expected);
			double actualVal = Double.parseDouble(actual);
			return Math.abs(expectVal - actualVal) <= TOLERATED_DOUBLE_DEVIATION;
		} catch (NumberFormatException e) {
			// cannot parse, so column is not a numeric column
			return false;
		}
	}

	private static boolean isEqualNull(String expected, String actual) {
		return (null == actual || actual.isEmpty()) && NULL_LITERALS.contains(expected);
	}
}
