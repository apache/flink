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

public class BooleanParser extends FieldParser<Boolean> {

	private boolean result;

	/** Values for true and false respectively. Must be lower case. */
	private static final byte[][] TRUE = new byte[][] {
			"true".getBytes(),
			"1".getBytes()
	};
	private static final byte[][] FALSE = new byte[][] {
			"false".getBytes(),
			"0".getBytes()
	};

	@Override
	public int parseField(byte[] bytes, int startPos, int limit, byte[] delim, Boolean reuse) {

		final int delimLimit = limit - delim.length + 1;

		int i = startPos;

		while (i < limit) {
			if (i < delimLimit && delimiterNext(bytes, i, delim)) {
				break;
			}
			i++;
		}

		for (byte[] aTRUE : TRUE) {
			if (byteArrayEquals(bytes, startPos, i - startPos, aTRUE)) {
				result = true;
				return (i == limit) ? limit : i + delim.length;
			}
		}

		for (byte[] aFALSE : FALSE) {
			if (byteArrayEquals(bytes, startPos, i - startPos, aFALSE)) {
				result = false;
				return (i == limit) ? limit : i + delim.length;
			}
		}

		setErrorState(ParseErrorState.BOOLEAN_INVALID);
		return -1;
	}

	@Override
	public Boolean getLastResult() {
		return result;
	}

	@Override
	public Boolean createValue() {
		return false;
	}

	/**
	 * Checks if a part of a byte array matches another byte array with chars (case-insensitive).
	 * @param source The source byte array.
	 * @param start The offset into the source byte array.
	 * @param length The length of the match.
	 * @param other The byte array which is fully compared to the part of the source array.
	 * @return true if other can be found in the specified part of source, false otherwise.
	 */
	private static boolean byteArrayEquals(byte[] source, int start, int length, byte[] other) {
		if (length != other.length) {
			return false;
		}
		for (int i = 0; i < other.length; i++) {
			if (Character.toLowerCase(source[i + start]) != other[i]) {
				return false;
			}
		}
		return true;
	}
}
