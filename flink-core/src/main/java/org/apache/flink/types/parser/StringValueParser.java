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

import org.apache.flink.types.StringValue;

/**
 * Converts a variable length field of a byte array into a {@link StringValue}. The byte contents between
 * delimiters is interpreted as an ASCII string. The string may be quoted in double quotes. For quoted
 * strings, whitespaces (space and tab) leading and trailing before and after the quotes are removed.
 * 
 * @see StringValue
 */
public class StringValueParser extends FieldParser<StringValue> {

	private static final byte WHITESPACE_SPACE = (byte) ' ';
	private static final byte WHITESPACE_TAB = (byte) '\t';
	
	private static final byte QUOTE_DOUBLE = (byte) '"';
	
	private StringValue result;

	@Override
	public int parseField(byte[] bytes, int startPos, int limit, byte[] delimiter, StringValue reusable) {

		this.result = reusable;
		int i = startPos;
		byte current;

		final int delimLimit = limit-delimiter.length+1;

		// count initial whitespace lines
		while (i < limit && ((current = bytes[i]) == WHITESPACE_SPACE || current == WHITESPACE_TAB)) {
			i++;
		}
		
		// first none whitespace character
		if (i < limit && bytes[i] == QUOTE_DOUBLE) {
			// quoted string
			i++; // the quote
			
			// we count only from after the quote
			int quoteStart = i;
			while (i < limit && bytes[i] != QUOTE_DOUBLE) {
				i++;
			}
			
			if (i < limit) {
				// end of the string
				reusable.setValueAscii(bytes, quoteStart, i-quoteStart);
				
				i++; // the quote
				
				// skip trailing whitespace characters
				while (i < limit) {

					if (i < delimLimit && delimiterNext(bytes, i, delimiter)) {
						return i+delimiter.length;
					}
					current = bytes[i];
					if (current == WHITESPACE_SPACE || current == WHITESPACE_TAB) {
						i++;
					}
					else {
						setErrorState(ParseErrorState.UNQUOTED_CHARS_AFTER_QUOTED_STRING);
						return -1;	// illegal case of non-whitespace characters trailing
					}
				}
				if( i > limit ){
					i--;
				}
				return (i == limit ? limit : i + delimiter.length);
			} else {
				// exited due to line end without quote termination
				setErrorState(ParseErrorState.UNTERMINATED_QUOTED_STRING);
				return -1;
			}
		}
		else {
			// unquoted string -delim.length
			while (i < limit) {
				if (i < delimLimit && delimiterNext(bytes, i, delimiter)) {
					break;
				}
				i++;
			}
			// set from the beginning. unquoted strings include the leading whitespaces
			reusable.setValueAscii(bytes, startPos, i-startPos);
			return (i == limit ? limit : i + delimiter.length);
		}
	}
	
	@Override
	public StringValue createValue() {
		return new StringValue();
	}

	@Override
	public StringValue getLastResult() {
		return this.result;
	}
}
