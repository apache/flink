/**
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

/**
 * Converts a variable length field of a byte array into a {@link String}. The byte contents between
 * delimiters is interpreted as an ASCII string. The string may be quoted in double quotes. For quoted
 * strings, whitespaces (space and tab) leading and trailing before and after the quotes are removed.
 */
public class StringParser extends FieldParser<String> {
	
	private static final byte WHITESPACE_SPACE = (byte) ' ';
	private static final byte WHITESPACE_TAB = (byte) '\t';
	
	private static final byte QUOTE_DOUBLE = (byte) '"';
	
	private String result;
	
	@Override
	public int parseField(byte[] bytes, int startPos, int limit, char delim, String reusable) {
		
		int i = startPos;
		
		final byte delByte = (byte) delim;
		byte current;
		
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
				this.result = new String(bytes, quoteStart, i-quoteStart);
				
				i++; // the quote
				
				// skip trailing whitespace characters 
				while (i < limit && (current = bytes[i]) != delByte) {
					if (current == WHITESPACE_SPACE || current == WHITESPACE_TAB) {
						i++;
					}
					else {
						setErrorState(ParseErrorState.UNQUOTED_CHARS_AFTER_QUOTED_STRING);
						return -1;	// illegal case of non-whitespace characters trailing
					}
				}
				
				return (i == limit ? limit : i+1);
			} else {
				// exited due to line end without quote termination
				setErrorState(ParseErrorState.UNTERMINATED_QUOTED_STRING);
				return -1;
			}
		}
		else {
			// unquoted string
			while (i < limit && bytes[i] != delByte) {
				i++;
			}
			
			// set from the beginning. unquoted strings include the leading whitespaces
			this.result = new String(bytes, startPos, i-startPos);
			return (i == limit ? limit : i+1);
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
