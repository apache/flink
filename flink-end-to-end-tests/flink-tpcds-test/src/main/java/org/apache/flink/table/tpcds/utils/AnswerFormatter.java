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
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Answer set format tool class. convert delimiter from spaces or tabs to bar('|') in TPC-DS answer set.
 * before convert, need to format TPC-DS result as following:
 * 1. split answer set which has multi query results to multi answer set, includes query14, 23, 24, 39.
 * 2. replace tabs by spaces in answer set by vim.
 * (1) cd answer_set directory
 * (2) vim 1.ans with command model,
 * :set ts=8
 * :set noexpandtab
 * :%retab!
 * :args ./*.ans
 * :argdo %retab! |update
 * (3) save and quit vim.
 */
public class AnswerFormatter {

	private static final int SPACE_BETWEEN_COL = 1;
	private static final String RESULT_HEAD_STRING_BAR = "|";
	private static final String RESULT_HEAD_STRING_DASH = "--";
	private static final String RESULT_HEAD_STRING_SPACE = " ";
	private static final String COL_DELIMITER = "|";
	private static final String ANSWER_FILE_SUFFIX = ".ans";
	private static final String REGEX_SPLIT_BAR = "\\|";
	private static final String FILE_SEPARATOR = "/";

	/**
	 * 1.flink keeps NULLS_FIRST in ASC order, keeps NULLS_LAST in DESC order,
	 * choose corresponding answer set file here.
	 * 2.for query 8、14a、18、70、77, decimal precision of answer set is too low
	 * and unreasonable, compare result with result from SQL server, they can
	 * strictly match.
	 */
	private static final List<String> ORIGIN_ANSWER_FILE = Arrays.asList(
		"1", "2", "3", "4", "5_NULLS_FIRST", "6_NULLS_FIRST", "7", "8_SQL_SERVER", "9", "10",
		"11", "12", "13", "14a_SQL_SERVER", "14b_NULLS_FIRST", "15_NULLS_FIRST", "16",
		"17", "18_SQL_SERVER", "19", "20_NULLS_FIRST", "21_NULLS_FIRST", "22_NULLS_FIRST",
		"23a_NULLS_FIRST", "23b_NULLS_FIRST", "24a", "24b", "25", "26", "27_NULLS_FIRST",
		"28", "29", "30", "31", "32", "33", "34_NULLS_FIRST", "35_NULLS_FIRST", "36_NULLS_FIRST",
		"37", "38", "39a", "39b", "40", "41", "42", "43", "44", "45", "46_NULLS_FIRST",
		"47", "48", "49", "50", "51", "52", "53", "54", "55", "56_NULLS_FIRST", "57",
		"58", "59", "60", "61", "62_NULLS_FIRST", "63", "64", "65_NULLS_FIRST", "66_NULLS_FIRST",
		"67_NULLS_FIRST", "68_NULLS_FIRST", "69", "70_SQL_SERVER", "71_NULLS_LAST", "72_NULLS_FIRST", "73",
		"74", "75", "76_NULLS_FIRST", "77_SQL_SERVER", "78", "79_NULLS_FIRST", "80_NULLS_FIRST",
		"81", "82", "83", "84", "85", "86_NULLS_FIRST", "87", "88", "89", "90",
		"91", "92", "93_NULLS_FIRST", "94", "95", "96", "97", "98_NULLS_FIRST", "99_NULLS_FIRST"
	);

	public static void main(String[] args) throws Exception {
		ParameterTool params = ParameterTool.fromArgs(args);
		String originDir = params.getRequired("originDir");
		String destDir = params.getRequired("destDir");
		for (int i = 0; i < ORIGIN_ANSWER_FILE.size(); i++) {
			String file = ORIGIN_ANSWER_FILE.get(i);
			String originFileName = file + ANSWER_FILE_SUFFIX;
			String destFileName = file.split("_")[0] + ANSWER_FILE_SUFFIX;
			File originFile = new File(originDir + FILE_SEPARATOR + originFileName);
			File destFile = new File(destDir + FILE_SEPARATOR + destFileName);
			format(originFile, destFile);
		}
	}

	/**
	 * TPC-DS answer set has three kind of formats, recognize them and convert to unified format.
	 * @param originFile origin answer set file from TPC-DS.
	 * @param destFile file to save formatted answer set.
	 * @throws Exception
	 */
	private static void format(File originFile, File destFile) throws Exception {
		BufferedReader reader = new BufferedReader(new FileReader(originFile));
		BufferedWriter writer = new BufferedWriter(new FileWriter(destFile));

		String line;
		List<Integer> colLengthList;
		List<String> content = new ArrayList<>();
		while ((line = reader.readLine()) != null) {
			content.add(line);
		}

		if (isFormat1(content)) {
			colLengthList = Arrays.stream(content.get(1).split(REGEX_SPLIT_BAR))
				.map(col -> col.length())
				.collect(Collectors.toList());
			writeContent(writer, content, colLengthList);
		} else if (isFormat2(content)) {
			colLengthList = Arrays.stream(content.get(1).split(RESULT_HEAD_STRING_SPACE))
				.map(col -> col.length())
				.collect(Collectors.toList());
			writeContent(writer, content, colLengthList);
		} else {
			writeContent(writer, content, null);
		}

		reader.close();
		writer.close();
	}

	private static Boolean isFormat1(List<String> content) {
		return content.size() > 1
			&& content.get(0).contains(RESULT_HEAD_STRING_BAR)
			&& content.get(1).contains(RESULT_HEAD_STRING_DASH);
	}

	private static Boolean isFormat2(List<String> content) {
		return content.size() > 1
			&& content.get(1).contains(RESULT_HEAD_STRING_DASH);
	}

	private static String formatRow(String row, List<Integer> colLengthList) {
		StringBuilder sb = new StringBuilder();
		int start;
		int end = 0;
		for (int i = 0; i < colLengthList.size(); i++) {
			if (i == 0) {
				start = end;
			} else {
				start = end + SPACE_BETWEEN_COL;
			}

			start = start < row.length() ? start : row.length();
			end = (start + colLengthList.get(i)) < row.length() ? (start + colLengthList.get(i)) : row.length();
			sb.append(row.substring(start, end).trim());

			if (i != colLengthList.size() - 1) {
				sb.append(COL_DELIMITER);
			}
		}
		return sb.toString();
	}

	private static void writeContent(BufferedWriter writer, List<String> content, List<Integer> colLengthList) throws Exception {
		if (colLengthList != null) {
			for (int i = 2; i < content.size(); i++) {
				if (content.get(i).isEmpty() || content.get(i).endsWith("rows selected.)")) {
					break;
				} else {
					String row = content.get(i);
					String formatRow = formatRow(row, colLengthList);
					writer.write(formatRow);
					writer.write("\n");
				}
			}
		}
		else {
			for (int i = 1; i < content.size(); i++) {
				if (i == (content.size() - 1) && content.get(i).endsWith("rows)")) {
					break;
				} else {
					String formattedLine = content.get(i);
					writer.write(formattedLine);
					writer.write("\n");
				}
			}
		}
	}
}
