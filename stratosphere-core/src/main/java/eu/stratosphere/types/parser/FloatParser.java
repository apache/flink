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
 * Parses a text field into a {@link FloatValue}
 */
public class FloatParser extends FieldParser<Float> {
	
	private float result;
	
	@Override
	public int parseField(byte[] bytes, int startPos, int limit, char delim, Float reusable) {
		
		int i = startPos;
		final byte delByte = (byte) delim;
		
		while (i < limit && bytes[i] != delByte) {
			i++;
		}
		
		String str = new String(bytes, startPos, i-startPos);
		try {
			this.result = Float.parseFloat(str);
			return (i == limit) ? limit : i+1;
		}
		catch (NumberFormatException e) {
			setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
			return -1;
		}
	}
	
	@Override
	public Float createValue() {
		return Float.MIN_VALUE;
	}

	@Override
	public Float getLastResult() {
		return Float.valueOf(this.result);
	}
	
	/**
	 * Static utility to parse a field of type float from a byte sequence that represents text characters
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
	public static final float parseField(byte[] bytes, int startPos, int length) {
		return parseField(bytes, startPos, length, (char) 0xffff);
	}
	
	/**
	 * Static utility to parse a field of type float from a byte sequence that represents text characters
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
	public static final float parseField(byte[] bytes, int startPos, int length, char delimiter) {
		if (length <= 0) {
			throw new NumberFormatException("Invalid input: Empty string");
		}
		int i = 0;
		final byte delByte = (byte) delimiter;
		
		while (i < length && bytes[i] != delByte) {
			i++;
		}
		
		String str = new String(bytes, startPos, i);
		return Float.parseFloat(str);
	}
}
