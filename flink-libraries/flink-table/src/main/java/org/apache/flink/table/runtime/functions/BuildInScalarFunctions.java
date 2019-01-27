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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.runtime.functions.utils.DecimalUtils;
import org.apache.flink.table.runtime.functions.utils.JsonUtils;

import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * All build-in scalar Functions.
 */
public class BuildInScalarFunctions {
	public static final Logger LOG = LoggerFactory.getLogger(BuildInScalarFunctions.class);

	private static final Map<String, String> EMPTY_MAP = new HashMap<>(0);

	private static ThreadLocalCache<String, Pattern> regexpPatternCache =
			new ThreadLocalCache<String, Pattern>(64) {
				public Pattern getNewInstance(String regex) {
					return Pattern.compile(regex);
				}
			};

	/**
	 * Creates a map by parsing text. Split text into key-value pairs
	 * using two delimiters. The first delimiter separates pairs, and the
	 * second delimiter separates key and value. If only one parameter is given,
	 * default delimiters are used: ',' as delimiter1 and '=' as delimiter2.
	 * @param text the input text
	 * @return the map
	 */
	public static Map<String, String> strToMap(String text) {
		return strToMap(text, ",", "=");
	}

	/**
	 * Creates a map by parsing text. Split text into key-value pairs
	 * using two delimiters. The first delimiter separates pairs, and the
	 * second delimiter separates key and value.
	 * @param text the input text
	 * @param listDelimiter the delimiter to separates pairs
	 * @param keyValueDelimiter the delimiter to separates key and value
	 * @return the map
	 */
	public static Map<String, String> strToMap(String text, String listDelimiter, String keyValueDelimiter) {
		if (StringUtils.isEmpty(text)) {
			return EMPTY_MAP;
		}

		String[] keyValuePairs = text.split(listDelimiter);
		Map<String, String> ret = new HashMap<>(keyValuePairs.length);
		for (String keyValuePair : keyValuePairs) {
			String[] keyValue = keyValuePair.split(keyValueDelimiter, 2);
			if (keyValue.length < 2) {
				ret.put(keyValuePair, null);
			} else {
				ret.put(keyValue[0], keyValue[1]);
			}
		}
		return ret;
	}

	/**
	 * The number of milliseconds in a day.
	 *
	 * <p>This is the modulo 'mask' used when converting
	 * TIMESTAMP values to DATE and TIME values.
	 */
	public static final long MILLIS_PER_DAY = 86400000; // = 24 * 60 * 60 * 1000

	public static String jsonValue(String jsonString, String pathString) {
		return JsonUtils.getInstance().getJsonObject(jsonString, pathString);
	}

	public static int sround(int b0) {
		return org.apache.calcite.runtime.SqlFunctions.sround(b0, 0);
	}

	public static long sround(long b0) {
		return org.apache.calcite.runtime.SqlFunctions.sround(b0, 0);
	}

	public static Decimal sround(Decimal b0) {
		return sround(b0, 0);
	}

	public static Decimal sround(Decimal b0, int b1) {
		return Decimal.sround(b0, b1);
	}

	public static Decimal sign(Decimal b0) {
		return Decimal.sign(b0);
	}

	public static double sround(double b0) {
		return org.apache.calcite.runtime.SqlFunctions.sround(b0, 0);
	}

	public static boolean isDecimal(Object s) {
		return DecimalUtils.isDecimal(s);
	}

	public static boolean isDigit(Object obj) {
		if (obj == null){
			return false;
		}
		if ((obj instanceof Long)
				|| (obj instanceof Integer)
				|| (obj instanceof Short)
				|| (obj instanceof Byte)){
			return true;
		}
		if (obj instanceof String){
			String s = obj.toString();
			if (s == null || "".equals(s)) {
				return false;
			}
			return StringUtils.isNumeric(s);
		}
		else {
			return false;
		}
	}

	public static boolean isAlpha(Object obj) {
		if (obj == null){
			return false;
		}
		if (!(obj instanceof String)){
			return false;
		}
		String s = obj.toString();
		if ("".equals(s)) {
			return false;
		}
		return StringUtils.isAlpha(s);
	}

	public static Integer hashCode(String str){
		if (str == null) {
			return Integer.MIN_VALUE;
		}
		return Math.abs(str.hashCode());
	}

	public static Boolean regExp(String s, String regex){
		if (s == null || regex == null) {
			return false;
		}
		if (regex.length() == 0) {
			return false;
		}
		try {
			return (regexpPatternCache.get(regex)).matcher(s).find(0);
		} catch (Exception e) {
			LOG.error("Exception when compile and match regex:" +
					regex + " on: " + s, e);
			return false;
		}
	}

	public static Byte bitAnd(Byte a, Byte b) {
		if (a == null || b == null) {
			return 0;
		}
		return (byte) (a & b);
	}

	public static Short bitAnd(Short a, Short b) {
		if (a == null || b == null) {
			return 0;
		}
		return (short) (a & b);
	}

	public static Integer bitAnd(Integer a, Integer b) {
		if (a == null || b == null) {
			return 0;
		}

		return a & b;
	}

	public static Long bitAnd(Long a, Long b) {
		if (a == null || b == null) {
			return 0L;
		}
		return a & b;
	}

	public static Byte bitNot(Byte a) {
		if (a == null) {
			a = 0;
		}
		return (byte) (~a);
	}

	public static Short bitNot(Short a) {
		if (a == null) {
			a = 0;
		}
		return (short) (~a);
	}

	public static Integer bitNot(Integer a) {
		if (a == null) {
			a = 0;
		}
		return ~a;
	}

	public static Long bitNot(Long a) {
		if (a == null) {
			a = 0L;
		}
		return ~a;
	}

	public static Byte bitOr(Byte a, Byte b) {
		if (a == null || b == null) {
			if (a == null) {
				a = 0;
			}
			if (b == null) {
				b = 0;
			}
		}
		return (byte) (a | b);
	}

	public static Short bitOr(Short a, Short b) {
		if (a == null || b == null) {
			if (a == null) {
				a = 0;
			}
			if (b == null) {
				b = 0;
			}
		}
		return (short) (a | b);
	}

	public static Integer bitOr(Integer a, Integer b) {
		if (a == null || b == null) {
			if (a == null) {
				a = 0;
			}
			if (b == null) {
				b = 0;
			}
		}
		return a | b;
	}

	public static Long bitOr(Long a, Long b) {
		if (a == null || b == null) {
			if (a == null) {
				a = 0L;
			}
			if (b == null) {
				b = 0L;
			}
		}
		return a | b;
	}

	public static Byte bitXor(Byte a, Byte b) {
		if (a == null || b == null) {
			if (a == null) {
				a = 0;
			}
			if (b == null) {
				b = 0;
			}
		}
		return (byte) (a ^ b);
	}

	public static Short bitXor(Short a, Short b) {
		if (a == null || b == null) {
			if (a == null) {
				a = 0;
			}
			if (b == null) {
				b = 0;
			}
		}
		return (short) (a ^ b);
	}

	public static Integer bitXor(Integer a, Integer b) {
		if (a == null || b == null) {
			if (a == null) {
				a = 0;
			}
			if (b == null) {
				b = 0;
			}
		}
		return a ^ b;
	}

	public static Long bitXor(Long a, Long b) {
		if (a == null || b == null) {
			if (a == null) {
				a = 0L;
			}
			if (b == null) {
				b = 0L;
			}
		}
		return a ^ b;
	}

	public static byte divide(byte a, byte b) {
		return (byte) (a / b);
	}

	public static short divide(short a, short b) {
		return (short) (a / b);
	}

	public static int divide(int a, int b) {
		return (a / b);
	}

	public static long divide(long a, long b) {
		return (a / b);
	}

	public static float divide(float a, float b) {
		return (a / b);
	}

	public static double divide(double a, double b) {
		return (a / b);
	}

	public static String toBase64(byte[] b){
		if (b == null) {
			return null;
		}
		byte[] bytes = new byte[b.length];
		System.arraycopy(b, 0, bytes, 0, b.length);
		String result = null;
		try {
			result = new String(Base64.encodeBase64(bytes));
		} catch (Exception e) {
			LOG.error("Exception when encode base64:" + Hex.encodeHexString(b), e);
		}
		return result;
	}

	public static byte[] fromBase64(String value){
		if (value == null) {
			return null;
		}
		byte[] bytes = new byte[value.getBytes().length];
		System.arraycopy(value.getBytes(), 0, bytes, 0, bytes.length);
		byte[] result = null;
		try {
			result = Base64.decodeBase64(bytes);
		} catch (Exception e) {
			LOG.error("Exception when decode base64:" + value, e);
		}
		return result;
	}

	public static String uuid(){
		return UUID.randomUUID().toString();
	}

	public static String uuid(byte[] b){
		if (b == null){
			return null;
		}
		String result = null;
		try {
			result = UUID.nameUUIDFromBytes(b).toString();
		} catch (Exception e) {
			LOG.error("Exception when encode uuid:" + Hex.encodeHexString(b), e);
		}
		return result;
	}

	public static long toLong(Date v) {
		return v.getTime();
	}

	public static int toInt(java.sql.Time v) {
		return (int) (toLong(v) % DateTimeUtils.MILLIS_PER_DAY);
	}

	public static int toInt(java.util.Date v) {
		return (int) (toLong(v)  / DateTimeUtils.MILLIS_PER_DAY);
	}

	public static Long safeToLong(Date v) {
		if (v == null) {
			return null;
		} else {
			return toLong(v);
		}
	}

	public static Integer safeToInt(java.sql.Time v) {
		if (v == null) {
			return null;
		} else {
			return toInt(v);
		}
	}

	public static Integer safeToInt(java.util.Date v) {
		if (v == null) {
			return null;
		} else {
			return toInt(v);
		}
	}

	public static java.sql.Date internalToDate(int v) {
		final long t = v * DateTimeUtils.MILLIS_PER_DAY;
		return new java.sql.Date(t);
	}

	public static java.sql.Time internalToTime(int v) {
		return new java.sql.Time(v);
	}

	public static java.sql.Timestamp internalToTimestamp(long v) {
		return new java.sql.Timestamp(v);
	}

	public static java.sql.Date safeInternalToDate(Integer v) {
		if (v == null) {
			return null;
		} else {
			return internalToDate(v);
		}
	}

	public static java.sql.Time safeInternalToTime(Integer v) {
		if (v == null) {
			return null;
		} else {
			return internalToTime(v);
		}
	}

	public static java.sql.Timestamp safeInternalToTimestamp(Long v) {
		if (v == null) {
			return null;
		} else {
			return internalToTimestamp(v);
		}
	}

	/** Helper for CAST({timestamp} AS VARCHAR(n)). */
	public static String unixTimeToString(int time) {
		return unixTimeToString(time, 0);
	}

	public static String unixTimeToString(int time, int precision) {
		final StringBuilder buf = new StringBuilder(8);
		unixTimeToString(buf, time, precision);
		return buf.toString();
	}

	private static void unixTimeToString(StringBuilder buf, int time, int precision) {
		// time may be negative, means time milliSecs before 00:00:00
		// this is a bug in calcite avatica
		while (time < 0) {
			time += MILLIS_PER_DAY;
		}

		int h = time / 3600000;
		int time2 = time % 3600000;
		int m = time2 / 60000;
		int time3 = time2 % 60000;
		int s = time3 / 1000;
		int ms = time3 % 1000;
		int2(buf, h);
		buf.append(':');
		int2(buf, m);
		buf.append(':');
		int2(buf, s);
		if (precision > 0) {
			buf.append('.');
			while (precision > 0) {
				buf.append((char) ('0' + (ms / 100)));
				ms = ms % 100;
				ms = ms * 10;
				--precision;
			}
		}
	}

	private static void int2(StringBuilder buf, int i) {
		buf.append((char) ('0' + (i / 10) % 10));
		buf.append((char) ('0' + i % 10));
	}
}
