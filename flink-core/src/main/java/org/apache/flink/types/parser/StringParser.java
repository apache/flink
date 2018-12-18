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

package org.apache.flink.types.parser;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Converts a variable length field of a byte array into a {@link String}. The byte contents between
 * delimiters is interpreted as an ASCII string. The string may be quoted in double quotes. For quoted
 * strings, whitespaces (space and tab) leading and trailing before and after the quotes are removed.
 */
@PublicEvolving
public class StringParser extends FieldParser<String> {

	private boolean quotedStringParsing = false;
	private byte quoteCharacter;
	private static final byte BACKSLASH = 92;

	private String result;

	public void enableQuotedStringParsing(byte quoteCharacter) {
		this.quotedStringParsing = true;
		this.quoteCharacter = quoteCharacter;
	}

	@Override
	public int parseField(byte[] bytes, int startPos, int limit, byte[] delimiter, String reusable) {

		if (startPos == limit) {
			setErrorState(ParseErrorState.EMPTY_COLUMN);
			this.result = "";
			return limit;
		}

		int i = startPos;

		final int delimLimit = limit - delimiter.length + 1;

		if(quotedStringParsing && bytes[i] == quoteCharacter) {
			// quoted string parsing enabled and first character is a quote
			i++;

			// search for ending quote character, continue when it is escaped
			while (i < limit && (bytes[i] != quoteCharacter || bytes[i - 1] == BACKSLASH)) {
				i++;
			}

			if (i == limit) {
				setErrorState(ParseErrorState.UNTERMINATED_QUOTED_STRING);
				return -1;
			} else {
				i++;
				// check for proper termination
				if (i == limit) {
					// either by end of line
					this.result = new String(bytes, startPos + 1, i - startPos - 2, getCharset());
					return limit;
				} else if ( i < delimLimit && delimiterNext(bytes, i, delimiter)) {
					// or following field delimiter
					this.result = new String(bytes, startPos + 1, i - startPos - 2, getCharset());
					return i + delimiter.length;
				} else {
					// no proper termination
					setErrorState(ParseErrorState.UNQUOTED_CHARS_AFTER_QUOTED_STRING);
					return -1;
				}
			}
		} else {

			// look for delimiter
			while( i < delimLimit && !delimiterNext(bytes, i, delimiter)) {
				i++;
			}

			if (i >= delimLimit) {
				this.result = new String(bytes, startPos, limit - startPos, getCharset());
				return limit;
			} else {
				// delimiter found.
				if (i == startPos) {
					setErrorState(ParseErrorState.EMPTY_COLUMN); // mark empty column
				}
				this.result = new String(bytes, startPos, i - startPos, getCharset());
				return i + delimiter.length;
			}
		}
	}

	@Override
	public String createValue() {
		return "";
	}

	@Override
	public String getLastResult() {
		return this.result;
	}
}
