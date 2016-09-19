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

import java.math.BigInteger;
import org.apache.flink.annotation.PublicEvolving;

/**
 * Parses a text field into a {@link java.math.BigInteger}.
 */
@PublicEvolving
public class BigIntParser extends FieldParser<BigInteger> {

	private static final BigInteger BIG_INTEGER_INSTANCE = BigInteger.ZERO;

	private BigInteger result;

	@Override
	public int parseField(byte[] bytes, int startPos, int limit, byte[] delimiter, BigInteger reusable) {
		int i = startPos;

		final int delimLimit = limit - delimiter.length + 1;

		while (i < limit) {
			if (i < delimLimit && delimiterNext(bytes, i, delimiter)) {
				if (i == startPos) {
					setErrorState(ParseErrorState.EMPTY_COLUMN);
					return -1;
				}
				break;
			}
			i++;
		}

		if (i > startPos &&
				(Character.isWhitespace(bytes[startPos]) || Character.isWhitespace(bytes[(i - 1)]))) {
			setErrorState(ParseErrorState.NUMERIC_VALUE_ILLEGAL_CHARACTER);
			return -1;
		}

		String str = new String(bytes, startPos, i - startPos);
		try {
			this.result = new BigInteger(str);
			return (i == limit) ? limit : i + delimiter.length;
		} catch (NumberFormatException e) {
			setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
			return -1;
		}
	}

	@Override
	public BigInteger createValue() {
		return BIG_INTEGER_INSTANCE;
	}

	@Override
	public BigInteger getLastResult() {
		return this.result;
	}

	/**
	 * Static utility to parse a field of type BigInteger from a byte sequence that represents text
	 * characters
	 * (such as when read from a file stream).
	 *
	 * @param bytes    The bytes containing the text data that should be parsed.
	 * @param startPos The offset to start the parsing.
	 * @param length   The length of the byte sequence (counting from the offset).
	 * @return The parsed value.
	 * @throws NumberFormatException Thrown when the value cannot be parsed because the text 
	 * represents not a correct number.
	 */
	public static final BigInteger parseField(byte[] bytes, int startPos, int length) {
		return parseField(bytes, startPos, length, (char) 0xffff);
	}

	/**
	 * Static utility to parse a field of type BigInteger from a byte sequence that represents text
	 * characters
	 * (such as when read from a file stream).
	 *
	 * @param bytes     The bytes containing the text data that should be parsed.
	 * @param startPos  The offset to start the parsing.
	 * @param length    The length of the byte sequence (counting from the offset).
	 * @param delimiter The delimiter that terminates the field.
	 * @return The parsed value.
	 * @throws NumberFormatException Thrown when the value cannot be parsed because the text 
	 * represents not a correct number.
	 */
	public static final BigInteger parseField(byte[] bytes, int startPos, int length, char delimiter) {
		if (length <= 0) {
			throw new NumberFormatException("Invalid input: Empty string");
		}
		int i = 0;
		final byte delByte = (byte) delimiter;

		while (i < length && bytes[startPos + i] != delByte) {
			i++;
		}

		if (i > 0 &&
				(Character.isWhitespace(bytes[startPos]) || Character.isWhitespace(bytes[startPos + i - 1]))) {
			throw new NumberFormatException("There is leading or trailing whitespace in the numeric field.");
		}

		String str = new String(bytes, startPos, i);
		return new BigInteger(str);
	}
}
