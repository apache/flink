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

/**
 * This file is based on source code from the Hadoop Project (http://hadoop.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. 
 */

package eu.stratosphere.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;

/**
 * Utility class to convert objects into strings in vice-versa.
 */
public final class StringUtils {

	/**
	 * Empty private constructor to overwrite public one.
	 */
	private StringUtils() {}

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
	
	/**
	 * Helper function to escape Strings for display in HTML pages
	 * 
	 * @param str
	 * 	String to Escape
	 * @return escaped String
	 */
	public static String escapeHtml(String str) {
		int len = str.length();
		char[] s = str.toCharArray();
		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < len; i += 1) {
			char c = s[i];
			if ((c == '\\') || (c == '"') || (c == '/')) {
				sb.append('\\');
				sb.append(c);
			}
			else if (c == '\b')
				sb.append("\\b");
			else if (c == '\t')
				sb.append("\\t");
			else if (c == '\n')
				sb.append("<br>");
			else if (c == '\f')
				sb.append("\\f");
			else if (c == '\r')
				sb.append("\\r");
			else if (c == '>')
				sb.append("&gt;");
			else if (c == '<')
				sb.append("&lt;");
			else {
				if (c < ' ') {
					// Unreadable throw away
				} else {
					sb.append(c);
				}
			}
		}

		return sb.toString();
	}
	
	
	public static final String arrayAwareToString(Object o) {
		if (o == null) {
			return "null";
		}
		if (o.getClass().isArray()) {
			return arrayToString(o);
		}
		
		return o.toString();
	}
	
	
	public static final String arrayToString(Object array) {
		if (array instanceof int[]) {
			return Arrays.toString((int[]) array);
		}
		if (array instanceof long[]) {
			return Arrays.toString((long[]) array);
		}
		if (array instanceof Object[]) {
			return Arrays.toString((Object[]) array);
		}
		if (array instanceof byte[]) {
			return Arrays.toString((byte[]) array);
		}
		if (array instanceof double[]) {
			return Arrays.toString((double[]) array);
		}
		if (array instanceof float[]) {
			return Arrays.toString((float[]) array);
		}
		if (array instanceof boolean[]) {
			return Arrays.toString((boolean[]) array);
		}
		if (array instanceof char[]) {
			return Arrays.toString((char[]) array);
		}
		if (array instanceof short[]) {
			return Arrays.toString((short[]) array);
		}
		
		return "<unknown array type>";
	}
	
	public static final String showControlCharacters(String str) {
		int len = str.length();
		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < len; i += 1) {
			char c = str.charAt(i);
			switch (c) {
			case '\b':
				sb.append("\\b");
				break;
			case '\t':
				sb.append("\\t");
				break;
			case '\n':
				sb.append("\\n");
				break;
			case '\f':
				sb.append("\\f");
				break;
			case '\r':
				sb.append("\\r");
				break;
			default:
				sb.append(c);
			}
		}

		return sb.toString();
	}
}
