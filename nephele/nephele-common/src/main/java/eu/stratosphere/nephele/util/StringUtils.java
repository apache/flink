/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

/**
 * This file is based on source code from the Hadoop Project (http://hadoop.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. 
 */

package eu.stratosphere.nephele.util;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Utility class to convert objects into strings in vice-versa.
 * 
 * @author warneke
 */
public final class StringUtils {

	/**
	 * Empty private constructor to overwrite public one.
	 */
	private StringUtils() {
	}

	/**
	 * Makes a string representation of the exception.
	 * 
	 * @param e
	 *        the exception to stringify
	 * @return A string with exception name and call stack.
	 */
	public static String stringifyException(final Throwable e) {
		final StringWriter stm = new StringWriter();
		final PrintWriter wrt = new PrintWriter(stm);
		e.printStackTrace(wrt);
		wrt.close();
		return stm.toString();
	}

	/**
	 * Given an array of bytes it will convert the bytes to a hex string
	 * representation of the bytes.
	 * 
	 * @param bytes
	 *        the bytes to convert in a hex string
	 * @param start
	 *        start index, inclusively
	 * @param end
	 *        end index, exclusively
	 * @return hex string representation of the byte array
	 */
	public static String byteToHexString(final byte[] bytes, final int start, final int end) {
		if (bytes == null) {
			throw new IllegalArgumentException("bytes == null");
		}
		final StringBuilder s = new StringBuilder();
		for (int i = start; i < end; i++) {
			s.append(String.format("%02x", bytes[i]));
		}
		return s.toString();
	}

	/**
	 * Given an array of bytes it will convert the bytes to a hex string
	 * representation of the bytes.
	 * 
	 * @param bytes
	 *        the bytes to convert in a hex string
	 * @return hex string representation of the byte array
	 */
	public static String byteToHexString(final byte[] bytes) {
		return byteToHexString(bytes, 0, bytes.length);
	}

	/**
	 * Given a hex string this will return the byte array corresponding to the
	 * string .
	 * 
	 * @param hex
	 *        the hex String array
	 * @return a byte array that is a hex string representation of the given
	 *         string. The size of the byte array is therefore hex.length/2
	 */
	public static byte[] hexStringToByte(final String hex) {
		final byte[] bts = new byte[hex.length() / 2];
		for (int i = 0; i < bts.length; i++) {
			bts[i] = (byte) Integer.parseInt(hex.substring(2 * i, 2 * i + 2), 16);
		}
		return bts;
	}
}
