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

package org.apache.flink.table.client.cli;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;

import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Utilities for CLI formatting.
 */
public final class CliUtils {

	public static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

	private CliUtils() {
		// private
	}

	public static void repeatChar(AttributedStringBuilder sb, char c, int count) {
		IntStream.range(0, count).forEach(i -> sb.append(c));
	}

	public static void normalizeColumn(AttributedStringBuilder sb, String col, int maxWidth) {
		// limit column content
		if (col.length() > maxWidth) {
			sb.append(col, 0, maxWidth - 1);
			sb.append('~');
		} else {
			repeatChar(sb, ' ', maxWidth - col.length());
			sb.append(col);
		}
	}

	public static List<AttributedString> formatTwoLineHelpOptions(int width, List<Tuple2<String, String>> options) {
		final AttributedStringBuilder line1 = new AttributedStringBuilder();
		final AttributedStringBuilder line2 = new AttributedStringBuilder();

		// we assume that every options has not more than 11 characters (+ key and space)
		final int columns = (int) Math.ceil(((double) options.size()) / 2);
		final int space = (width - CliStrings.DEFAULT_MARGIN.length() - columns * 13) / columns;
		final Iterator<Tuple2<String, String>> iter = options.iterator();
		while (iter.hasNext()) {
			// first line
			Tuple2<String, String> option = iter.next();
			line1.style(AttributedStyle.DEFAULT.inverse());
			line1.append(option.f0);
			line1.style(AttributedStyle.DEFAULT);
			line1.append(' ');
			line1.append(option.f1);
			repeatChar(line1, ' ', (11 - option.f1.length()) + space);
			// second line
			if (iter.hasNext()) {
				option = iter.next();
				line2.style(AttributedStyle.DEFAULT.inverse());
				line2.append(option.f0);
				line2.style(AttributedStyle.DEFAULT);
				line2.append(' ');
				line2.append(option.f1);
				repeatChar(line2, ' ', (11 - option.f1.length()) + space);
			}
		}

		return Arrays.asList(line1.toAttributedString(), line2.toAttributedString());
	}

	public static String[] rowToString(Row row) {
		final String[] fields = new String[row.getArity()];
		for (int i = 0; i < row.getArity(); i++) {
			final Object field = row.getField(i);
			if (field == null) {
				fields[i] = CliStrings.NULL_COLUMN;
			} else {
				fields[i] = field.toString();
			}
		}
		return fields;
	}

	public static String[] typesToString(DataType[] types) {
		final String[] typesAsString = new String[types.length];
		for (int i = 0; i < types.length; i++) {
			typesAsString[i] = types[i].toString();
		}
		return typesAsString;
	}
}
