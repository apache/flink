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

package org.apache.flink.table.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;

import com.ibm.icu.lang.UCharacter;
import com.ibm.icu.lang.UProperty;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Utilities for print formatting.
 */
@Internal
public class PrintUtils {

	// constants for printing
	public static final int MAX_COLUMN_WIDTH = 30;
	public static final String NULL_COLUMN = "(NULL)";
	private static final String COLUMN_TRUNCATED_FLAG = "...";

	private PrintUtils() {
	}

	/**
	 * Displays the result in a tableau form.
	 *
	 * <p>For example:
	 * +-------------+---------+-------------+
	 * | boolean_col | int_col | varchar_col |
	 * +-------------+---------+-------------+
	 * |        true |       1 |         abc |
	 * |       false |       2 |         def |
	 * |      (NULL) |  (NULL) |      (NULL) |
	 * +-------------+---------+-------------+
	 * 3 rows in result
	 *
	 * <p>Changelog is not supported until FLINK-16998 is finished.
	 */
	public static void printAsTableauForm(
			TableSchema tableSchema,
			Iterator<Row> it,
			PrintWriter printWriter) {
		printAsTableauForm(tableSchema, it, printWriter, MAX_COLUMN_WIDTH, NULL_COLUMN);
	}

	/**
	 * Displays the result in a tableau form.
	 *
	 * <p>For example:
	 * +-------------+---------+-------------+
	 * | boolean_col | int_col | varchar_col |
	 * +-------------+---------+-------------+
	 * |        true |       1 |         abc |
	 * |       false |       2 |         def |
	 * |      (NULL) |  (NULL) |      (NULL) |
	 * +-------------+---------+-------------+
	 * 3 rows in result
	 *
	 * <p>Changelog is not supported until FLINK-16998 is finished.
	 */
	public static void printAsTableauForm(
			TableSchema tableSchema,
			Iterator<Row> it,
			PrintWriter printWriter,
			int maxColumnWidth,
			String nullColumn) {
		List<String[]> rows = new ArrayList<>();

		// fill field names first
		List<TableColumn> columns = tableSchema.getTableColumns();
		rows.add(columns.stream().map(TableColumn::getName).toArray(String[]::new));
		while (it.hasNext()) {
			rows.add(rowToString(it.next(), nullColumn));
		}

		int[] colWidths = columnWidthsByContent(columns, rows, maxColumnWidth);
		String borderline = genBorderLine(colWidths);

		// print field names
		printWriter.println(borderline);
		printSingleRow(colWidths, rows.get(0), printWriter);
		printWriter.println(borderline);

		// print content
		if (rows.size() > 1) {
			rows.subList(1, rows.size()).forEach(row -> printSingleRow(colWidths, row, printWriter));
			printWriter.println(borderline);
		}

		int numRows = rows.size() - 1;
		final String rowTerm;
		if (numRows > 1) {
			rowTerm = "rows";
		} else {
			rowTerm = "row";
		}
		printWriter.println((rows.size() - 1) + " " + rowTerm + " in set");
		printWriter.flush();
	}

	public static String[] rowToString(Row row) {
		return rowToString(row, NULL_COLUMN);
	}

	public static String[] rowToString(Row row, String nullColumn) {
		final String[] fields = new String[row.getArity()];
		for (int i = 0; i < row.getArity(); i++) {
			final Object field = row.getField(i);
			if (field == null) {
				fields[i] = nullColumn;
			} else {
				fields[i] = StringUtils.arrayAwareToString(field);
			}
		}
		return fields;
	}

	private static int[] columnWidthsByContent(
			List<TableColumn> columns,
			List<String[]> rows,
			int maxColumnWidth) {
		// fill width with field names first
		int[] colWidths = columns.stream().mapToInt(col -> col.getName().length()).toArray();

		// fill column width with real data
		for (String[] row : rows) {
			for (int i = 0; i < row.length; ++i) {
				colWidths[i] = Math.max(colWidths[i], getStringDisplayWidth(row[i]));
			}
		}

		// adjust column width with maximum length
		for (int i = 0; i < colWidths.length; ++i) {
			colWidths[i] = Math.min(colWidths[i], maxColumnWidth);
		}

		return colWidths;
	}

	public static String genBorderLine(int[] colWidths) {
		StringBuilder sb = new StringBuilder();
		sb.append("+");
		for (int width : colWidths) {
			sb.append(EncodingUtils.repeat('-', width + 1));
			sb.append("-+");
		}
		return sb.toString();
	}

	public static void printSingleRow(int[] colWidths, String[] cols, PrintWriter printWriter) {
		StringBuilder sb = new StringBuilder();
		sb.append("|");
		int idx = 0;
		for (String col : cols) {
			sb.append(" ");
			int displayWidth = getStringDisplayWidth(col);
			if (displayWidth <= colWidths[idx]) {
				sb.append(EncodingUtils.repeat(' ', colWidths[idx] - displayWidth));
				sb.append(col);
			} else {
				sb.append(truncateString(col, colWidths[idx] - COLUMN_TRUNCATED_FLAG.length()));
				sb.append(COLUMN_TRUNCATED_FLAG);
			}
			sb.append(" |");
			idx++;
		}
		printWriter.println(sb.toString());
		printWriter.flush();
	}

	private static String truncateString(String col, int targetWidth) {
		int passedWidth = 0;
		int i = 0;
		for (; i < col.length(); i++) {
			if (isFullWidth(Character.codePointAt(col, i))) {
				passedWidth += 2;
			} else {
				passedWidth += 1;
			}
			if (passedWidth >= targetWidth) {
				break;
			}
		}
		String substring = col.substring(0, i);

		// pad with ' ' before the column
		int lackedWidth = targetWidth - getStringDisplayWidth(substring);
		if (lackedWidth > 0) {
			substring = EncodingUtils.repeat(' ', lackedWidth) + substring;
		}
		return substring;
	}

	public static int getStringDisplayWidth(String str) {
		int numOfFullWidthCh = (int) str.codePoints().filter(PrintUtils::isFullWidth).count();
		return str.length() + numOfFullWidthCh;
	}

	/**
	 * Check codePoint is FullWidth or not according to Unicode Standard version 12.0.0.
	 * See http://unicode.org/reports/tr11/
	 */
	public static boolean isFullWidth(int codePoint) {
		int value = UCharacter.getIntPropertyValue(codePoint, UProperty.EAST_ASIAN_WIDTH);
		switch (value) {
			case UCharacter.EastAsianWidth.NEUTRAL:
				return false;
			case UCharacter.EastAsianWidth.AMBIGUOUS:
				return false;
			case UCharacter.EastAsianWidth.HALFWIDTH:
				return false;
			case UCharacter.EastAsianWidth.FULLWIDTH:
				return true;
			case UCharacter.EastAsianWidth.NARROW:
				return false;
			case UCharacter.EastAsianWidth.WIDE:
				return true;
			default:
				throw new RuntimeException("unknown UProperty.EAST_ASIAN_WIDTH: " + value);
		}
	}
}
