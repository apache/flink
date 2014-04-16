/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.types.parser;

/**
 * Parses a decimal text field into a IntValue.
 * Only characters '1' to '0' and '-' are allowed.
 * The parser does not check for the maximum value.
 */
public class IntParser extends FieldParser<Integer> {
	
	private static final long OVERFLOW_BOUND = 0x7fffffffL;
	private static final long UNDERFLOW_BOUND = 0x80000000L;

	private int result;
	
	@Override
	public int parseField(byte[] bytes, int startPos, int limit, char delimiter, Integer reusable) {
		long val = 0;
		boolean neg = false;
		
		if (bytes[startPos] == '-') {
			neg = true;
			startPos++;
			
			// check for empty field with only the sign
			if (startPos == limit || bytes[startPos] == delimiter) {
				setErrorState(ParseErrorState.NUMERIC_VALUE_ORPHAN_SIGN);
				return -1;
			}
		}
		
		for (int i = startPos; i < limit; i++) {
			if (bytes[i] == delimiter) {
				this.result = (int) (neg ? -val : val);
				return i+1;
			}
			if (bytes[i] < 48 || bytes[i] > 57) {
				setErrorState(ParseErrorState.NUMERIC_VALUE_ILLEGAL_CHARACTER);
				return -1;
			}
			val *= 10;
			val += bytes[i] - 48;
			
			if (val > OVERFLOW_BOUND && (!neg || val > UNDERFLOW_BOUND)) {
				setErrorState(ParseErrorState.NUMERIC_VALUE_OVERFLOW_UNDERFLOW);
				return -1;
			}
		}
		
		this.result = (int) (neg ? -val : val);
		return limit;
	}
	
	@Override
	public Integer createValue() {
		return Integer.MIN_VALUE;
	}

	@Override
	public Integer getLastResult() {
		return Integer.valueOf(this.result);
	}
	
	/**
	 * Static utility to parse a field of type int from a byte sequence that represents text characters
	 * (such as when read from a file stream).
	 * 
	 * @param bytes The bytes containing the text data that should be parsed.
	 * @param startPos The offset to start the parsing.
	 * @param length The length of the byte sequence (counting from the offset).
	 * 
	 * @return The parsed value.
	 * 
	 * @throws NumberFormatException Thrown when the value cannot be parsed because the text represents not a correct number.
	 */
	public static final int parseField(byte[] bytes, int startPos, int length) {
		return parseField(bytes, startPos, length, (char) 0xffff);
	}
	
	/**
	 * Static utility to parse a field of type int from a byte sequence that represents text characters
	 * (such as when read from a file stream).
	 * 
	 * @param bytes The bytes containing the text data that should be parsed.
	 * @param startPos The offset to start the parsing.
	 * @param length The length of the byte sequence (counting from the offset).
	 * @param delimiter The delimiter that terminates the field.
	 * 
	 * @return The parsed value.
	 * 
	 * @throws NumberFormatException Thrown when the value cannot be parsed because the text represents not a correct number.
	 */
	public static final int parseField(byte[] bytes, int startPos, int length, char delimiter) {
		if (length <= 0) {
			throw new NumberFormatException("Invalid input: Empty string");
		}
		long val = 0;
		boolean neg = false;
		
		if (bytes[startPos] == '-') {
			neg = true;
			startPos++;
			length--;
			if (length == 0 || bytes[startPos] == delimiter) {
				throw new NumberFormatException("Orphaned minus sign.");
			}
		}
		
		for (; length > 0; startPos++, length--) {
			if (bytes[startPos] == delimiter) {
				return (int) (neg ? -val : val);
			}
			if (bytes[startPos] < 48 || bytes[startPos] > 57) {
				throw new NumberFormatException("Invalid character.");
			}
			val *= 10;
			val += bytes[startPos] - 48;
			
			if (val > OVERFLOW_BOUND && (!neg || val > UNDERFLOW_BOUND)) {
				throw new NumberFormatException("Value overflow/underflow");
			}
		}
		return (int) (neg ? -val : val);
	}
}
