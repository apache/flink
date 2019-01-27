/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.sql.parser.util;

import org.apache.calcite.util.Util;
import org.junit.ComparisonFailure;

import java.util.regex.Pattern;

/**
 * Static utilities for JUnit tests.
 */
public abstract class TestUtil {
	//~ Static fields/initializers ---------------------------------------------

	private static final Pattern LINE_BREAK_PATTERN =
		Pattern.compile("\r\n|\r|\n");

	private static final Pattern TAB_PATTERN = Pattern.compile("\t");

	private static final String LINE_BREAK =
		"\\\\n\"" + Util.LINE_SEPARATOR + " + \"";

	//~ Methods ----------------------------------------------------------------

	public static void assertEqualsVerbose(
		String expected,
		String actual) {
		if (actual == null) {
			if (expected == null) {
				return;
			} else {
				String message = "Expected:\n"
					+ expected
					+ "\nActual: null";
				throw new ComparisonFailure(message, expected, null);
			}
		}
		if ((expected != null) && expected.equals(actual)) {
			return;
		}
		String s = toJavaString(actual);

		String message =
			"Expected:\n"
				+ expected
				+ "\nActual:\n"
				+ actual
				+ "\nActual java:\n"
				+ s + '\n';
		throw new ComparisonFailure(message, expected, actual);
	}

	/**
	 * Converts a string (which may contain quotes and newlines) into a java
	 * literal.
	 *
	 * <p>For example,
	 * <pre><code>string with "quotes" split
	 * across lines</code></pre>
	 * becomes
	 *
	 * <pre><code>"string with \"quotes\" split" + NL +
	 *  "across lines"</code></pre>
	 */
	public static String quoteForJava(String s) {
		s = Util.replace(s, "\\", "\\\\");
		s = Util.replace(s, "\"", "\\\"");
		s = LINE_BREAK_PATTERN.matcher(s).replaceAll(LINE_BREAK);
		s = TAB_PATTERN.matcher(s).replaceAll("\\\\t");
		s = "\"" + s + "\"";
		final String spurious = " + \n\"\"";
		if (s.endsWith(spurious)) {
			s = s.substring(0, s.length() - spurious.length());
		}
		return s;
	}

	/**
	 * Converts a string (which may contain quotes and newlines) into a java
	 * literal.
	 *
	 * <p>For example,</p>
	 *
	 * <pre><code>string with "quotes" split
	 * across lines</code></pre>
	 *
	 * <p>becomes</p>
	 *
	 * <pre><code>TestUtil.fold(
	 *  "string with \"quotes\" split\n",
	 *  + "across lines")</code></pre>
	 */
	public static String toJavaString(String s) {
		// Convert [string with "quotes" split
		// across lines]
		// into [fold(
		// "string with \"quotes\" split\n"
		// + "across lines")]
		//
		s = Util.replace(s, "\"", "\\\"");
		s = LINE_BREAK_PATTERN.matcher(s).replaceAll(LINE_BREAK);
		s = TAB_PATTERN.matcher(s).replaceAll("\\\\t");
		s = "\"" + s + "\"";
		String spurious = "\n \\+ \"\"";
		if (s.endsWith(spurious)) {
			s = s.substring(0, s.length() - spurious.length());
		}
		return s;
	}

	/**
	 * Combines an array of strings, each representing a line, into a single
	 * string containing line separators.
	 */
	public static String fold(String... strings) {
		StringBuilder buf = new StringBuilder();
		for (String string : strings) {
			buf.append(string);
			buf.append('\n');
		}
		return buf.toString();
	}

	/**
	 * Quotes a string for Java or JSON.
	 */
	public static String escapeString(String s) {
		return escapeString(new StringBuilder(), s).toString();
	}

	/**
	 * Quotes a string for Java or JSON, into a builder.
	 */
	public static StringBuilder escapeString(StringBuilder buf, String s) {
		buf.append('"');
		int n = s.length();
		char lastChar = 0;
		for (int i = 0; i < n; ++i) {
			char c = s.charAt(i);
			switch (c) {
				case '\\':
					buf.append("\\\\");
					break;
				case '"':
					buf.append("\\\"");
					break;
				case '\n':
					buf.append("\\n");
					break;
				case '\r':
					if (lastChar != '\n') {
						buf.append("\\r");
					}
					break;
				default:
					buf.append(c);
					break;
			}
			lastChar = c;
		}
		return buf.append('"');
	}

	/**
	 * Quotes a pattern.
	 */
	public static String quotePattern(String s) {
		return s.replaceAll("\\\\", "\\\\")
			.replaceAll("\\.", "\\\\.")
			.replaceAll("\\+", "\\\\+")
			.replaceAll("\\{", "\\\\{")
			.replaceAll("\\}", "\\\\}")
			.replaceAll("\\|", "\\\\||")
			.replaceAll("[$]", "\\\\\\$")
			.replaceAll("\\?", "\\\\?")
			.replaceAll("\\*", "\\\\*")
			.replaceAll("\\(", "\\\\(")
			.replaceAll("\\)", "\\\\)")
			.replaceAll("\\[", "\\\\[")
			.replaceAll("\\]", "\\\\]");
	}
}

// End TestUtil.java
