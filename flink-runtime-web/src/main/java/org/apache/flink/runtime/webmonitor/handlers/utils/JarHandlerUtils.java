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

package org.apache.flink.runtime.webmonitor.handlers.utils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utils for jar handlers.
 *
 * @see org.apache.flink.runtime.webmonitor.handlers.JarRunHandler
 * @see org.apache.flink.runtime.webmonitor.handlers.JarPlanHandler
 */
public class JarHandlerUtils {

	private static final Pattern ARGUMENTS_TOKENIZE_PATTERN = Pattern.compile("([^\"\']\\S*|\".+?\"|\'.+?\')\\s*");

	/**
	 * Takes program arguments as a single string, and splits them into a list of string.
	 *
	 * <pre>
	 * tokenizeArguments("--foo bar")            = ["--foo" "bar"]
	 * tokenizeArguments("--foo \"bar baz\"")    = ["--foo" "bar baz"]
	 * tokenizeArguments("--foo 'bar baz'")      = ["--foo" "bar baz"]
	 * tokenizeArguments(null)                   = []
	 * </pre>
	 *
	 * <strong>WARNING: </strong>This method does not respect escaped quotes.
	 */
	public static List<String> tokenizeArguments(@Nullable final String args) {
		if (args == null) {
			return Collections.emptyList();
		}
		final Matcher matcher = ARGUMENTS_TOKENIZE_PATTERN.matcher(args);
		final List<String> tokens = new ArrayList<>();
		while (matcher.find()) {
			tokens.add(matcher.group()
				.trim()
				.replace("\"", "")
				.replace("\'", ""));
		}
		return tokens;
	}
}
