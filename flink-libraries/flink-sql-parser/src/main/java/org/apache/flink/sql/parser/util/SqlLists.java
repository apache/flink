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

package org.apache.flink.sql.parser.util;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class SqlLists {

	private static final Pattern PATTERN_STATEMENT = Pattern.compile("[^\\\\];");
	private static final Pattern PATTERN_STRING = Pattern.compile("(\"|')([^\"^']*)(\"|')");
	private static final Pattern PATTERN_SINGLE_LINE = Pattern.compile("--.*");
	private static final Pattern PATTERN_MULTI_LINE = Pattern.compile("/\\*.*?\\*/", Pattern.DOTALL);

	public static List<SqlInfo> getSQLList(String context) {
		Map<Integer, Integer> enterMap = new TreeMap<Integer, Integer>();
		int enterCount = 1;
		for (int i = 0; i < context.length(); i++) {
			if (context.charAt(i) == '\n') {
				enterMap.put(i, enterCount++);
			}
		}
		enterMap.put(context.length(), enterCount++);
		//System.out.println(enterMap);
		List<SqlInfo> list = new ArrayList<SqlInfo>();

		Matcher match = PATTERN_STATEMENT.matcher(context);
		int index = 0;
		while (match.find()) {

			if (isInComment(context, match.start() + 1)
				|| !isMatch(context.substring(index, match.start() + 1), '\'')
				|| !isMatch(context.substring(index, match.start() + 1), '\"')) {
				continue;
			}

			String str = context.substring(index, match.start() + 1)
				.replaceAll("\\\\;", ";");
			str = str.replaceAll("^;", "");

			if (!"".equals(str) && !isCommentClause(str)) {
				int maxEnters = 0;
				int lastEnter = 0;
				int firstLineIndex = 0;
				int loc = index - 1;
				for (Integer i : enterMap.keySet()) {
					if (loc > i) {
						maxEnters = enterMap.get(i);
						lastEnter = i;
					}
					if (loc <= i) {
						if (loc == i) {
							firstLineIndex = 0;
						} else {
							firstLineIndex = loc - lastEnter;
						}
						break;
					}
				}
				SqlInfo sqlInfo = new SqlInfo();
				sqlInfo.setSqlContent(str);
				sqlInfo.setLine(maxEnters + 1);
				sqlInfo.setFirstLineIndex(firstLineIndex);
				list.add(sqlInfo);
			}
			index = match.start() + 2;
			//System.out.println(index);
		}
		if (context.substring(index) != null
			&& context.substring(index).trim().length() != 0) {
			String str = context.substring(index).replaceAll("\\\\;", ";");
			str = str.replaceAll("^;", "");
			if (!"".equals(str) && !isCommentClause(str)) {
				int loc = index - 1;
				int maxEnters = 0;
				int lastEnter = 0;
				int firstLineIndex = 0;
				for (Integer i : enterMap.keySet()) {
					if (index > i) {
						maxEnters = enterMap.get(i);
						lastEnter = i;
					}
					if (index <= i) {
						if (index == i) {
							firstLineIndex = 0;
						} else {
							firstLineIndex = index - lastEnter;
						}
						break;
					}
				}
				SqlInfo sqlInfo = new SqlInfo();
				sqlInfo.setSqlContent(str);
				sqlInfo.setLine(maxEnters + 1);
				sqlInfo.setFirstLineIndex(firstLineIndex);
				list.add(sqlInfo);
			}
		}

		return list;
	}

	public static String toLowCase(String str) {
		Matcher m = PATTERN_STRING.matcher(str);
		StringBuffer sb = new StringBuffer();
		int index = 0;
		while (m.find()) {
			sb.append(str.substring(index, m.start()).toLowerCase());
			sb.append(str.substring(m.start(), m.end()));
			index = m.end();
		}
		if (index != str.length()) {
			sb.append(str.substring(index, str.length()).toLowerCase());
		}
		return sb.toString();
	}

	private static boolean isCommentClause(String str) {
		String trimStr = str.trim();
		if (trimStr.startsWith("/*") && trimStr.endsWith("*/")) {
			return true;
		}

		boolean res = true;
		String[] lines = StringUtils.split(str, "\n");
		for (String line : lines) {
			String val = line.trim();
			if (StringUtils.isEmpty(val) || val.startsWith("--")) {
				res = true;
			} else {
				return false;
			}
		}
		return res;
	}

	private static boolean isMatch(String source, char pattern) {
		int count = 0;
		for (int i = 0; i < source.length(); i++) {

			if (source.charAt(i) == pattern) {
				count++;
			}
			if (source.charAt(i) == '\\' && i < source.length() - 1
				&& source.charAt(i + 1) == pattern) {
				i++;
			}
		}
		return count % 2 == 0;
	}

	private static boolean isInComment(String context, int index) {
		Matcher singleMatch = PATTERN_SINGLE_LINE.matcher(context);

		while (singleMatch.find()) {
			int start = singleMatch.start();
			int end = singleMatch.end() - 1;

			if (index > start && index <= end) {
				return true;
			}
		}

		Matcher multiMatch = PATTERN_MULTI_LINE.matcher(context);

		while (multiMatch.find()) {
			int start = multiMatch.start();
			int end = multiMatch.end() - 1;

			if (index > start && index < end) {
				return true;
			}
		}

		return false;
	}

	private static boolean isComment(String context) {
		return true;
	}
}
