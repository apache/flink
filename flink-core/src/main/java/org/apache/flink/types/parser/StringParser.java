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

/**
 * Converts a variable length field of a byte array into a {@link String}. The byte contents between
 * delimiters is interpreted as an ASCII string. The string may be quoted in double quotes. For quoted
 * strings, whitespaces (space and tab) leading and trailing before and after the quotes are removed.
 */
public class StringParser extends FieldParser<String> {
	
	private static final byte WHITESPACE_SPACE = (byte) ' ';
	private static final byte WHITESPACE_TAB = (byte) '\t';
	
	private static final byte QUOTE_DOUBLE = (byte) '"';

	private static enum ParserStates {
		NONE, IN_QUOTE, STOP
	}

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

		// first determine the boundaries of the cell
		ParserStates parserState = ParserStates.NONE;

		// the current position evaluated against the cell boundary
		int endOfCellPosition = i - 1;

		while (parserState != ParserStates.STOP && endOfCellPosition < limit) {
			endOfCellPosition++;
			// make sure we don't step over the end of the buffer
			if(endOfCellPosition == limit) {
				break;
			}
			current = bytes[endOfCellPosition];
			if(current == delByte) {
				// if we are in a quote do nothing, otherwise we reached the end
				parserState = parserState == ParserStates.IN_QUOTE ? parserState: ParserStates.STOP;
			} else if(current == QUOTE_DOUBLE) {
				// we entered a quote
				if(parserState == ParserStates.IN_QUOTE) {
					// we end the quote
					parserState = ParserStates.NONE;
				} else {
					// we start a new quote
					parserState = ParserStates.IN_QUOTE;
				}
			}
		}

		if(parserState == ParserStates.IN_QUOTE) {
			// exited due to line end without quote termination
			setErrorState(ParseErrorState.UNTERMINATED_QUOTED_STRING);
			return -1;
		}


		// boundary of the cell is now
		// i --> endOfCellPosition

		// first none whitespace character
		if (i < limit && bytes[i] == QUOTE_DOUBLE) {

			// check if there are characters at the end
			current = bytes[endOfCellPosition - 1];

			// if the character preceding the end of the cell is not a WHITESPACE or the end QUOTE_DOUBLE
			// there are unquoted characters at the end

			if (!(current == WHITESPACE_SPACE || current == WHITESPACE_TAB || current == QUOTE_DOUBLE)) {
				setErrorState(ParseErrorState.UNQUOTED_CHARS_AFTER_QUOTED_STRING);
				return -1;	// illegal case of non-whitespace characters trailing
			}

			// skip trailing whitespace after quote .. by moving the cursor backwards
			int skipAtEnd = 0;
			while (bytes[endOfCellPosition - 1 - skipAtEnd] == WHITESPACE_SPACE || bytes[endOfCellPosition - 1 - skipAtEnd] == WHITESPACE_TAB) {
				skipAtEnd++;
			}

			// now unescape
			boolean notEscaped = true;
			int endOfContent = i + 1;
			for(int counter = endOfContent; counter < endOfCellPosition - skipAtEnd; counter++) {
				notEscaped = bytes[counter] != QUOTE_DOUBLE || !notEscaped;
				if (notEscaped) {
					// realign
					bytes[endOfContent++] = bytes[counter];
				}
			}

			this.result = new String(bytes, i + 1, endOfContent - i - 1);

			return (endOfCellPosition == limit ? limit : endOfCellPosition + 1);
		}
		else {
			// unquoted string

			// set from the beginning. unquoted strings include the leading whitespaces
			this.result = new String(bytes, i, endOfCellPosition - i);
			return (endOfCellPosition == limit ? limit : endOfCellPosition + 1);
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
