/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.util;

/**
 * This class provides a number of convenience methods to deal with numbers and the conversion of them.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class NumberUtils {

	/**
	 * Private constructor to prevent instantiation.
	 */
	private NumberUtils() {
	}

	public static short byteArrayToShort(final byte[] arr, final int offset) {

		return (short) (((arr[offset] << 8)) | ((arr[offset + 1] & 0xFF)));
	}

	public static void shortToByteArray(final short val, final byte[] arr, final int offset) {

		arr[offset] = (byte) ((val & 0xFF00) >> 8);
		arr[offset + 1] = (byte) (val & 0x00FF);
	}

	/**
	 * Serializes and writes the given integer number to the provided byte array.
	 * 
	 * @param integerToSerialize
	 *        the integer number of serialize
	 * @param byteArray
	 *        the byte array to write to
	 * @param offset
	 *        the offset at which to start writing inside the byte array
	 */
	public static void integerToByteArray(final int integerToSerialize, final byte[] byteArray, final int offset) {

		for (int i = 0; i < 4; ++i) {
			final int shift = i << 3; // i * 8
			byteArray[(offset + 3) - i] = (byte) ((integerToSerialize & (0xff << shift)) >>> shift);
		}
	}

	/**
	 * Reads and deserializes an integer number from the given byte array.
	 * 
	 * @param byteArray
	 *        the byte array to read from
	 * @param offset
	 *        the offset at which to start reading the byte array
	 * @return the deserialized integer number
	 */
	public static int byteArrayToInteger(final byte[] byteArray, final int offset) {

		int integer = 0;

		for (int i = 0; i < 4; ++i) {
			integer |= (byteArray[(offset + 3) - i] & 0xff) << (i << 3);
		}

		return integer;
	}
}
