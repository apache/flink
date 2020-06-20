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

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.DecimalDataUtils;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.data.binary.BinaryStringDataUtil;
import org.apache.flink.table.runtime.util.JsonUtils;
import org.apache.flink.table.utils.EncodingUtils;
import org.apache.flink.table.utils.ThreadLocalCache;

import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.table.data.DecimalDataUtils.castFrom;
import static org.apache.flink.table.data.DecimalDataUtils.doubleValue;

/**
 * Built-in scalar runtime functions.
 *
 * <p>NOTE: Before you add functions here, check if Calcite provides it in
 * {@code org.apache.calcite.runtime.SqlFunctions}. Furthermore, make sure
 * to implement the function efficiently. Sometimes it makes sense to create a
 * {@code org.apache.flink.table.codegen.calls.CallGenerator} instead to avoid
 * massive object creation and reuse instances.
 */
public class SqlFunctionUtils {

	private static final Logger LOG = LoggerFactory.getLogger(SqlFunctionUtils.class);

	private static final ThreadLocalCache<String, Pattern> REGEXP_PATTERN_CACHE =
		new ThreadLocalCache<String, Pattern>() {
			@Override
			public Pattern getNewInstance(String regex) {
				return Pattern.compile(regex);
			}
		};

	private static final ThreadLocalCache<String, URL> URL_CACHE =
		new ThreadLocalCache<String, URL>() {
			public URL getNewInstance(String url) {
				try {
					return new URL(url);
				} catch (MalformedURLException e) {
					throw new RuntimeException(e);
				}
			}
		};

	private static final Map<String, String> EMPTY_MAP = new HashMap<>(0);

	public static double exp(DecimalData d) {
		return Math.exp(doubleValue(d));
	}

	public static double power(double base, DecimalData exponent) {
		return Math.pow(base, doubleValue(exponent));
	}

	public static double power(DecimalData base, DecimalData exponent) {
		return Math.pow(doubleValue(base), doubleValue(exponent));
	}

	public static double power(DecimalData base, double exponent) {
		return Math.pow(doubleValue(base), exponent);
	}

	public static double cosh(DecimalData x) {
		return Math.cosh(doubleValue(x));
	}

	public static double acos(DecimalData a) {
		return Math.acos(doubleValue(a));
	}

	public static double asin(DecimalData a) {
		return Math.asin(doubleValue(a));
	}

	public static double atan(DecimalData a) {
		return Math.atan(doubleValue(a));
	}

	public static double atan2(DecimalData y, DecimalData x) {
		return Math.atan2(doubleValue(y), doubleValue(x));
	}

	public static double sin(DecimalData a) {
		return Math.sin(doubleValue(a));
	}

	public static double sinh(DecimalData a) {
		return Math.sinh(doubleValue(a));
	}

	public static double cos(DecimalData a) {
		return Math.cos(doubleValue(a));
	}

	public static double tan(DecimalData a) {
		return Math.tan(doubleValue(a));
	}

	/**
	 * Calculates the hyperbolic tangent of a big decimal number.
	 */
	public static double tanh(DecimalData a) {
		return Math.tanh(doubleValue(a));
	}

	public static double cot(DecimalData a) {
		return 1.0d / Math.tan(doubleValue(a));
	}

	public static double degrees(DecimalData angrad) {
		return Math.toDegrees(doubleValue(angrad));
	}

	public static double radians(DecimalData angdeg) {
		return Math.toRadians(doubleValue(angdeg));
	}

	public static DecimalData abs(DecimalData a) {
		return DecimalDataUtils.abs(a);
	}

	public static DecimalData floor(DecimalData a) {
		return DecimalDataUtils.floor(a);
	}

	public static DecimalData ceil(DecimalData a) {
		return DecimalDataUtils.ceil(a);
	}

	// -------------------------- natural logarithm ------------------------

	/**
	 * Returns the natural logarithm of "x".
	 */
	public static double log(double x) {
		return Math.log(x);
	}

	public static double log(DecimalData x) {
		return Math.log(doubleValue(x));
	}

	/**
	 * Returns the logarithm of "x" with base "base".
	 */
	public static double log(double base, double x) {
		return Math.log(x) / Math.log(base);
	}

	public static double log(double base, DecimalData x) {
		return log(base, doubleValue(x));
	}

	public static double log(DecimalData base, double x) {
		return log(doubleValue(base), x);
	}

	public static double log(DecimalData base, DecimalData x) {
		return log(doubleValue(base), doubleValue(x));
	}

	/**
	 * Returns the logarithm of "a" with base 2.
	 */
	public static double log2(double x) {
		return Math.log(x) / Math.log(2);
	}

	public static double log2(DecimalData x) {
		return log2(doubleValue(x));
	}

	public static double log10(double x) {
		return Math.log10(x);
	}

	public static double log10(DecimalData x) {
		return log10(doubleValue(x));
	}

	// -------------------------- string functions ------------------------

	/**
	 * Returns the string str left-padded with the string pad to a length of len characters.
	 * If str is longer than len, the return value is shortened to len characters.
	 */
	public static String lpad(String base, int len, String pad) {
		if (len < 0 || "".equals(pad)) {
			return null;
		} else if (len == 0) {
			return "";
		}

		char[] data = new char[len];
		char[] baseChars = base.toCharArray();
		char[] padChars = pad.toCharArray();

		// the length of the padding needed
		int pos = Math.max(len - base.length(), 0);

		// copy the padding
		for (int i = 0; i < pos; i += pad.length()) {
			for (int j = 0; j < pad.length() && j < pos - i; j++) {
				data[i + j] = padChars[j];
			}
		}

		// copy the base
		int i = 0;
		while (pos + i < len && i < base.length()) {
			data[pos + i] = baseChars[i];
			i += 1;
		}

		return new String(data);
	}

	/**
	 * Returns the string str right-padded with the string pad to a length of len characters.
	 * If str is longer than len, the return value is shortened to len characters.
	 */
	public static String rpad(String base, int len, String pad) {
		if (len < 0 || "".equals(pad)) {
			return null;
		} else if (len == 0) {
			return "";
		}

		char[] data = new char[len];
		char[] baseChars = base.toCharArray();
		char[] padChars = pad.toCharArray();

		int pos = 0;

		// copy the base
		while (pos < base.length() && pos < len) {
			data[pos] = baseChars[pos];
			pos += 1;
		}

		// copy the padding
		while (pos < len) {
			int i = 0;
			while (i < pad.length() && i < len - pos) {
				data[pos + i] = padChars[i];
				i += 1;
			}
			pos += pad.length();
		}

		return new String(data);
	}

	/**
	 * Returns a string that repeats the base string n times.
	 */
	public static String repeat(String str, int repeat) {
		return EncodingUtils.repeat(str, repeat);
	}

	/**
	 * Replaces all the old strings with the replacement string.
	 */
	public static String replace(String str, String oldStr, String replacement) {
		return str.replace(oldStr, replacement);
	}

	/**
	 * Split target string with custom separator and pick the index-th(start with 0) result.
	 *
	 * @param str       target string.
	 * @param separator custom separator.
	 * @param index     index of the result which you want.
	 * @return the string at the index of split results.
	 */
	public static String splitIndex(String str, String separator, int index) {
		if (index < 0) {
			return null;
		}
		String[] values = StringUtils.splitByWholeSeparatorPreserveAllTokens(str, separator);
		if (index >= values.length) {
			return null;
		} else {
			return values[index];
		}
	}

	/**
	 * Split target string with custom separator and pick the index-th(start with 0) result.
	 *
	 * @param str   target string.
	 * @param character int value of the separator character
	 * @param index index of the result which you want.
	 * @return the string at the index of split results.
	 */
	public static String splitIndex(String str, int character, int index) {
		if (character > 255 || character < 1 || index < 0) {
			return null;
		}
		String[] values = StringUtils.splitPreserveAllTokens(str, (char) character);
		if (index >= values.length) {
			return null;
		} else {
			return values[index];
		}
	}

	/**
	 * Returns a string resulting from replacing all substrings
	 * that match the regular expression with replacement.
	 */
	public static String regexpReplace(String str, String regex, String replacement) {
		if (str == null || regex == null || replacement == null) {
			return null;
		}
		try {
			return str.replaceAll(regex, Matcher.quoteReplacement(replacement));
		} catch (Exception e) {
			LOG.error(
				String.format("Exception in regexpReplace('%s', '%s', '%s')", str, regex, replacement),
				e);
			// return null if exception in regex replace
			return null;
		}
	}

	/**
	 * Returns a string extracted with a specified regular expression and a regex match group index.
	 */
	public static String regexpExtract(String str, String regex, int extractIndex) {
		if (str == null || regex == null) {
			return null;
		}

		try {
			Matcher m = Pattern.compile(regex).matcher(str);
			if (m.find()) {
				MatchResult mr = m.toMatchResult();
				return mr.group(extractIndex);
			}
		} catch (Exception e) {
			LOG.error(
				String.format("Exception in regexpExtract('%s', '%s', '%d')", str, regex, extractIndex),
				e);
		}

		return null;
	}

	public static String regexpExtract(String str, String regex, long extractIndex) {
		return regexpExtract(str, regex, (int) extractIndex);
	}

	/**
	 * Returns the first string extracted with a specified regular expression.
	 */
	public static String regexpExtract(String str, String regex) {
		return regexpExtract(str, regex, 0);
	}

	/**
	 * Parse string as key-value string and return the value matches key name.
	 * example:
	 * keyvalue('k1=v1;k2=v2', ';', '=', 'k2') = 'v2'
	 * keyvalue('k1:v1,k2:v2', ',', ':', 'k3') = NULL
	 *
	 * @param str     target string.
	 * @param pairSeparator  separator between key-value tuple.
	 * @param kvSeparator  separator between key and value.
	 * @param keyName name of the key whose value you want return.
	 * @return target value.
	 */
	public static BinaryStringData keyValue(
			BinaryStringData str,
			BinaryStringData pairSeparator,
			BinaryStringData kvSeparator,
			BinaryStringData keyName) {
		if (str == null || str.getSizeInBytes() == 0) {
			return null;
		}
		if (pairSeparator != null && pairSeparator.getSizeInBytes() == 1 &&
			kvSeparator != null && kvSeparator.getSizeInBytes() == 1) {
			return BinaryStringDataUtil.keyValue(str, pairSeparator.byteAt(0), kvSeparator.byteAt(0), keyName);
		} else {
			return BinaryStringData.fromString(
				keyValue(
						BinaryStringDataUtil.safeToString(str),
						BinaryStringDataUtil.safeToString(pairSeparator),
						BinaryStringDataUtil.safeToString(kvSeparator),
						BinaryStringDataUtil.safeToString(keyName)));
		}
	}

	private static String keyValue(String str, String pairSeparator, String kvSeparator, String keyName) {
		try {
			if (StringUtils.isEmpty(str)) {
				return null;
			}
			String[] values = StringUtils.split(str, pairSeparator);
			for (String value : values) {
				if (!StringUtils.isEmpty(value)) {
					String[] kv = StringUtils.split(kvSeparator);
					if (kv != null && kv.length == 2 && kv[0].equals(keyName)) {
						return kv[1];
					}
				}
			}
			return null;
		} catch (Exception e) {
			LOG.error("Exception when parse key-value", e);
			return null;
		}
	}

	/**
	 * Calculate the hash value of a given string.
	 *
	 * @param algorithm  message digest algorithm.
	 * @param str        string to hash.
	 * @return           hash value of string.
	 */
	public static String hash(String algorithm, String str) {
		return hash(algorithm, str, "");
	}

	/**
	 * Calculate the hash value of a given string.
	 *
	 * @param algorithm    message digest algorithm.
	 * @param str          string to hash.
	 * @param charsetName  charset of string.
	 * @return           hash value of string.
	 */
	public static String hash(String algorithm, String str, String charsetName) {
		try {
			byte[] digest = MessageDigest
				.getInstance(algorithm)
				.digest(strToBytesWithCharset(str, charsetName));
			return EncodingUtils.hex(digest);
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalArgumentException("Unsupported algorithm: " + algorithm, e);
		}
	}

	private static byte[] strToBytesWithCharset(String str, String charsetName) {
		byte[] byteArray = null;
		if (!StringUtils.isEmpty(charsetName)) {
			try {
				byteArray = str.getBytes(charsetName);
			} catch (UnsupportedEncodingException e) {
				LOG.warn("Unsupported encoding: " + charsetName + ", fallback to system charset", e);
				byteArray = null;
			}
		}
		if (byteArray == null) {
			byteArray = str.getBytes(StandardCharsets.UTF_8);
		}
		return byteArray;
	}

	/**
	 * Parse url and return various components of the URL.
	 * If accept any null arguments, return null.
	 *
	 * @param urlStr        URL string.
	 * @param partToExtract determines which components would return.
	 *                      accept values:
	 *                      HOST,PATH,QUERY,REF,
	 *                      PROTOCOL,FILE,AUTHORITY,USERINFO
	 * @return target value.
	 */
	public static String parseUrl(String urlStr, String partToExtract) {
		URL url;
		try {
			url = URL_CACHE.get(urlStr);
		} catch (Exception e) {
			LOG.error("Parse URL error: " + urlStr, e);
			return null;
		}
		if ("HOST".equals(partToExtract)) {
			return url.getHost();
		}
		if ("PATH".equals(partToExtract)) {
			return url.getPath();
		}
		if ("QUERY".equals(partToExtract)) {
			return url.getQuery();
		}
		if ("REF".equals(partToExtract)) {
			return url.getRef();
		}
		if ("PROTOCOL".equals(partToExtract)) {
			return url.getProtocol();
		}
		if ("FILE".equals(partToExtract)) {
			return url.getFile();
		}
		if ("AUTHORITY".equals(partToExtract)) {
			return url.getAuthority();
		}
		if ("USERINFO".equals(partToExtract)) {
			return url.getUserInfo();
		}

		return null;
	}

	/**
	 * Parse url and return various parameter of the URL.
	 * If accept any null arguments, return null.
	 *
	 * @param urlStr        URL string.
	 * @param partToExtract must be QUERY, or return null.
	 * @param key           parameter name.
	 * @return target value.
	 */
	public static String parseUrl(String urlStr, String partToExtract, String key) {
		if (!"QUERY".equals(partToExtract)) {
			return null;
		}

		String query = parseUrl(urlStr, partToExtract);
		if (query == null) {
			return null;
		}

		Pattern p = Pattern.compile("(&|^)" + Pattern.quote(key) + "=([^&]*)");
		Matcher m = p.matcher(query);
		if (m.find()) {
			return m.group(2);
		}
		return null;
	}

	public static int divideInt(int a, int b) {
		return a / b;
	}

	public static String subString(String str, long start, long len) {
		if (len < 0) {
			LOG.error("len of 'substring(str, start, len)' must be >= 0 and Int type, but len = {}", len);
			return null;
		}
		if (len > Integer.MAX_VALUE || start > Integer.MAX_VALUE) {
			LOG.error("len or start of 'substring(str, start, len)' must be Int type, but len = {}, start = {}", len, start);
			return null;
		}
		int length = (int) len;
		int pos = (int) start;
		if (str.isEmpty()) {
			return "";
		}

		int startPos;
		int endPos;

		if (pos > 0) {
			startPos = pos - 1;
			if (startPos >= str.length()) {
				return "";
			}
		} else if (pos < 0) {
			startPos = str.length() + pos;
			if (startPos < 0) {
				return "";
			}
		} else {
			startPos = 0;
		}

		if ((str.length() - startPos) < length) {
			endPos = str.length();
		} else {
			endPos = startPos + length;
		}
		return str.substring(startPos, endPos);
	}

	public static String subString(String str, long start) {
		return subString(str, start, Integer.MAX_VALUE);
	}

	public static String chr(long chr) {
		if (chr < 0) {
			return "";
		} else if ((chr & 0xFF) == 0) {
			return String.valueOf(Character.MIN_VALUE);
		} else {
			return String.valueOf((char) (chr & 0xFF));
		}
	}

	public static String overlay(String s, String r, long start, long length) {
		if (start <= 0 || start > s.length()) {
			return s;
		} else {
			StringBuilder sb = new StringBuilder();
			int startPos = (int) start;
			int len = (int) length;
			sb.append(s, 0, startPos - 1);
			sb.append(r);
			if ((startPos + len) <= s.length() && len > 0) {
				sb.append(s.substring(startPos - 1 + len));
			}
			return sb.toString();
		}
	}

	public static String overlay(String s, String r, long start) {
		return overlay(s, r, start, r.length());
	}

	public static int position(BinaryStringData seek, BinaryStringData s) {
		return position(seek, s, 1);
	}

	public static int position(BinaryStringData seek, BinaryStringData s, int from) {
		return s.indexOf(seek, from - 1) + 1;
	}

	public static int instr(BinaryStringData str, BinaryStringData subString, int startPosition, int nthAppearance) {
		if (nthAppearance <= 0) {
			throw new IllegalArgumentException("nthAppearance must be positive!");
		}
		if (startPosition == 0) {
			return 0;
		} else if (startPosition > 0) {
			int startIndex = startPosition;
			int index = 0;
			for (int i = 0; i < nthAppearance; i++) {
				index = str.indexOf(subString, startIndex - 1) + 1;
				if (index == 0) {
					return 0;
				}
				startIndex = index + 1;
			}
			return index;
		} else {
			int pos = instr(
					BinaryStringDataUtil.reverse(str),
					BinaryStringDataUtil.reverse(subString),
					-startPosition,
					nthAppearance);
			if (pos == 0) {
				return 0;
			} else {
				return str.numChars() + 2 - pos - subString.numChars();
			}
		}
	}

	/**
	 * Returns the hex string of a long argument.
	 */
	public static String hex(long x) {
		return Long.toHexString(x).toUpperCase();
	}

	/**
	 * Returns the hex string of a string argument.
	 */
	public static String hex(String x) {
		return EncodingUtils.hex(x.getBytes(StandardCharsets.UTF_8)).toUpperCase();
	}

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

	public static String jsonValue(String jsonString, String pathString) {
		// TODO: refactor this to use jackson ?
		return JsonUtils.getInstance().getJsonObject(jsonString, pathString);
	}

	// SQL ROUND
	/** SQL <code>ROUND</code> operator applied to int values. */
	public static int sround(int b0) {
		return sround(b0, 0);
	}

	/** SQL <code>ROUND</code> operator applied to int values. */
	public static int sround(int b0, int b1) {
		return sround(BigDecimal.valueOf(b0), b1).intValue();
	}

	/** SQL <code>ROUND</code> operator applied to long values. */
	public static long sround(long b0) {
		return sround(b0, 0);
	}

	/** SQL <code>ROUND</code> operator applied to long values. */
	public static long sround(long b0, int b1) {
		return sround(BigDecimal.valueOf(b0), b1).longValue();
	}

	/** SQL <code>ROUND</code> operator applied to BigDecimal values. */
	public static BigDecimal sround(BigDecimal b0) {
		return sround(b0, 0);
	}

	/** SQL <code>ROUND</code> operator applied to BigDecimal values. */
	public static BigDecimal sround(BigDecimal b0, int b1) {
		return b0.movePointRight(b1)
			.setScale(0, RoundingMode.HALF_UP).movePointLeft(b1);
	}

	/** SQL <code>ROUND</code> operator applied to double values. */
	public static double sround(double b0) {
		return sround(b0, 0);
	}

	/** SQL <code>ROUND</code> operator applied to double values. */
	public static double sround(double b0, int b1) {
		return sround(BigDecimal.valueOf(b0), b1).doubleValue();
	}

	/** SQL <code>ROUND</code> operator applied to DecimalData values. */
	public static DecimalData sround(DecimalData b0) {
		return sround(b0, 0);
	}

	/** SQL <code>ROUND</code> operator applied to DecimalData values. */
	public static DecimalData sround(DecimalData b0, int b1) {
		return DecimalDataUtils.sround(b0, b1);
	}

	public static DecimalData sign(DecimalData b0) {
		return DecimalDataUtils.sign(b0);
	}

	public static boolean isDecimal(Object obj) {
		if ((obj instanceof Long)
			|| (obj instanceof Integer)
			|| (obj instanceof Short)
			|| (obj instanceof Byte)
			|| (obj instanceof Float)
			|| (obj instanceof Double)
			|| (obj instanceof BigDecimal)
			|| (obj instanceof BigInteger)){
			return true;
		}
		if (obj instanceof String || obj instanceof Character){
			String s = obj.toString();
			if (s.isEmpty()) {
				return false;
			}
			return isInteger(s) || isLong(s) || isDouble(s);
		} else {
			return false;
		}

	}

	private static boolean isInteger(String s) {
		boolean flag = true;
		try {
			Integer.parseInt(s);
		} catch (NumberFormatException e) {
			flag = false;
		}
		return flag;
	}

	private static boolean isLong(String s) {
		boolean flag = true;
		try {
			Long.parseLong(s);
		} catch (NumberFormatException e) {
			flag = false;
		}
		return flag;
	}

	private static boolean isDouble(String s) {
		boolean flag = true;
		try {
			Double.parseDouble(s);
		} catch (NumberFormatException e) {
			flag = false;
		}
		return flag;
	}

	public static boolean isDigit(Object obj) {
		if ((obj instanceof Long)
			|| (obj instanceof Integer)
			|| (obj instanceof Short)
			|| (obj instanceof Byte)){
			return true;
		}
		if (obj instanceof String){
			String s = obj.toString();
			if (s.isEmpty()) {
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
		if (regex.length() == 0) {
			return false;
		}
		try {
			return (REGEXP_PATTERN_CACHE.get(regex)).matcher(s).find(0);
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

	public static String toBase64(BinaryStringData bs){
		return toBase64(bs.toBytes());
	}

	public static String toBase64(byte[] bytes){
		return Base64.getEncoder().encodeToString(bytes);
	}

	public static BinaryStringData fromBase64(BinaryStringData bs) {
		return BinaryStringData.fromBytes(Base64.getDecoder().decode(bs.toBytes()));
	}

	public static BinaryStringData fromBase64(byte[] bytes) {
		return BinaryStringData.fromBytes(Base64.getDecoder().decode(bytes));
	}

	public static String uuid(){
		return UUID.randomUUID().toString();
	}

	public static String uuid(byte[] b){
		return UUID.nameUUIDFromBytes(b).toString();
	}

	/** SQL <code>TRUNCATE</code> operator applied to BigDecimal values. */
	public static DecimalData struncate(DecimalData b0) {
		return struncate(b0, 0);
	}

	public static DecimalData struncate(DecimalData b0, int b1) {
		if (b1 >= b0.scale()) {
			return b0;
		}

		BigDecimal b2 = b0.toBigDecimal().movePointRight(b1)
			.setScale(0, RoundingMode.DOWN).movePointLeft(b1);
		int p = b0.precision();
		int s = b0.scale();

		if (b1 < 0) {
			return DecimalData.fromBigDecimal(b2, Math.min(38, 1 + p - s), 0);
		} else {
			return DecimalData.fromBigDecimal(b2, 1 + p - s + b1, b1);
		}
	}

	/** SQL <code>TRUNCATE</code> operator applied to double values. */
	public static float struncate(float b0) {
		return struncate(b0, 0);
	}

	public static float struncate(float b0, int b1) {
		return (float) doubleValue(struncate(castFrom((double) b0, 38, 18), b1));
	}

	/** TODO: remove addMonths and subtractMonths if CALCITE-3881 fixed.
	 *   https://issues.apache.org/jira/browse/CALCITE-3881
	 **/
	public static long addMonths(long timestamp, int m) {
		final long millis =
			DateTimeUtils.floorMod(timestamp, DateTimeUtils.MILLIS_PER_DAY);
		timestamp -= millis;
		final long x =
			addMonths((int) (timestamp / DateTimeUtils.MILLIS_PER_DAY), m);
		return x * DateTimeUtils.MILLIS_PER_DAY + millis;
	}

	public static int addMonths(int date, int m) {
		int y0 = (int) DateTimeUtils.unixDateExtract(TimeUnitRange.YEAR, date);
		int m0 = (int) DateTimeUtils.unixDateExtract(TimeUnitRange.MONTH, date);
		int d0 = (int) DateTimeUtils.unixDateExtract(TimeUnitRange.DAY, date);
		m0 += m;
		int deltaYear = (int) DateTimeUtils.floorDiv(m0, 12);
		y0 += deltaYear;
		m0 = (int) DateTimeUtils.floorMod(m0, 12);
		if (m0 == 0) {
			y0 -= 1;
			m0 += 12;
		}
		int last = lastDay(y0, m0);
		if (d0 > last) {
			d0 = last;
		}
		return DateTimeUtils.ymdToUnixDate(y0, m0, d0);
	}

	private static int lastDay(int y, int m) {
		switch (m) {
			case 2:
				return y % 4 == 0
					&& (y % 100 != 0
					|| y % 400 == 0)
					? 29 : 28;
			case 4:
			case 6:
			case 9:
			case 11:
				return 30;
			default:
				return 31;
		}
	}

	public static int subtractMonths(int date0, int date1) {
		if (date0 < date1) {
			return -subtractMonths(date1, date0);
		}
		// Start with an estimate.
		// Since no month has more than 31 days, the estimate is <= the true value.
		int m = (date0 - date1) / 31;
		while (true) {
			int date2 = addMonths(date1, m);
			if (date2 >= date0) {
				return m;
			}
			int date3 = addMonths(date1, m + 1);
			if (date3 > date0) {
				return m;
			}
			++m;
		}
	}

	public static int subtractMonths(long t0, long t1) {
		final long millis0 =
			DateTimeUtils.floorMod(t0, DateTimeUtils.MILLIS_PER_DAY);
		final int d0 = (int) DateTimeUtils.floorDiv(t0 - millis0,
			DateTimeUtils.MILLIS_PER_DAY);
		final long millis1 =
			DateTimeUtils.floorMod(t1, DateTimeUtils.MILLIS_PER_DAY);
		final int d1 = (int) DateTimeUtils.floorDiv(t1 - millis1,
			DateTimeUtils.MILLIS_PER_DAY);
		int x = subtractMonths(d0, d1);
		final long d2 = addMonths(d1, x);
		if (d2 == d0 && millis0 < millis1) {
			--x;
		}
		return x;
	}
}
