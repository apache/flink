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

package org.apache.flink.configuration;

import org.apache.flink.annotation.Internal;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Helper class for splitting a string on a given delimiter with quoting logic.
 */
@Internal
class StructuredOptionsSplitter {

	private enum State {
		CONSUMING,
		SINGLE_QUOTED_CONSUMING,
		DOUBLE_QUOTED_CONSUMING,
		UNKNOWN
	}

	private State state = State.UNKNOWN;
	private final List<String> splits = new ArrayList<>();
	private StringBuilder builder = new StringBuilder();

	/**
	 * Splits the given string on the given delimiter. It supports quoting parts of the string with
	 * either single (') or double quotes ("). Quotes can be escaped by doubling the quotes.
	 *
	 * <p>Examples:
	 * <ul>
	 *     <li>'A;B';C => [A;B], [C]</li>
	 *     <li>"AB'D";B;C => [AB'D], [B], [C]</li>
	 *     <li>"AB'""D;B";C => [AB'\"D;B], [C]</li>
	 * </ul>
	 *
	 * <p>For more examples check the tests.
	 * @param string a string to split
	 * @param delimiter delimiter to split on
	 * @return a list of splits
	 */
	static List<String> splitEscaped(String string, char delimiter) {
		StructuredOptionsSplitter splitter = new StructuredOptionsSplitter();
		return splitter.split(string, delimiter);
	}

	private List<String> split(String string, char delimiter) {
		for (int cursor = 0; cursor < string.length(); cursor++) {
			final char c = string.charAt(cursor);
			final Character nextChar;
			if (cursor + 1 < string.length()) {
				nextChar = string.charAt(cursor + 1);
			} else {
				nextChar = null;
			}

			switch (state) {
				case CONSUMING:
					if (c == '\'' || c == '"') {
						throw new IllegalArgumentException(
							"Could not split string. Illegal quoting at position: " + cursor);
					} else if (c == delimiter) {
						endSplit(builder);
					} else {
						builder.append(c);
					}
					continue;
				case SINGLE_QUOTED_CONSUMING:
					cursor = consumeInQuotes(c, nextChar, cursor, delimiter, '\'');
					continue;
				case DOUBLE_QUOTED_CONSUMING:
					cursor = consumeInQuotes(c, nextChar, cursor, delimiter, '\"');
					continue;
				case UNKNOWN:
					if (c == '\'') {
						state = State.SINGLE_QUOTED_CONSUMING;
					} else if (c == '\"') {
						state = State.DOUBLE_QUOTED_CONSUMING;
					} else if (c == delimiter) {
						endSplit(builder);
					} else {
						builder.append(c);
						state = State.CONSUMING;
					}
			}
		}

		if (state == State.CONSUMING || state == State.UNKNOWN) {
			splits.add(builder.toString());
		} else {
			throw new IllegalArgumentException("Could not split string. Quoting was not closed properly.");
		}

		return splits;
	}

	private int consumeInQuotes(
			char currentChar,
			@Nullable Character nextChar,
			int cursor,
			char delimiter,
			char quote) {
		if (currentChar == quote) {
			if (Objects.equals(nextChar, quote)) {
				builder.append(currentChar);
				cursor += 1;
			} else if (Objects.equals(nextChar, delimiter)) {
				endSplit(builder);
				cursor += 1;
			} else {
				throw new IllegalArgumentException(
					"Could not split string. Illegal quoting at position: " + cursor);
			}
		} else {
			builder.append(currentChar);
		}
		return cursor;
	}

	private void endSplit(StringBuilder builder) {
		splits.add(builder.toString());
		builder.setLength(0);
		state = State.UNKNOWN;
	}

	private StructuredOptionsSplitter() {
	}
}
