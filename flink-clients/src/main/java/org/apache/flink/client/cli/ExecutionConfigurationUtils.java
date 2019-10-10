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

package org.apache.flink.client.cli;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utilities for parsing parameters in the {@link ExecutionConfigAccessor}.
 */
@Internal
class ExecutionConfigurationUtils {

	/**
	 * Parses a list of {@link URL URLs} to a string and puts it in the provided {@code configuration} as the value of the provided {@code option}.
	 * @param urls the list of URLs to parse
	 * @param delimiter the delimiter to be used to separate the members of the list in the string
	 * @param configuration the configuration object to put the list
	 * @param option the {@link ConfigOption option} to serve as the key for the list in the configuration
	 * @return the produced string to be put in the configuration.
	 */
	static String urlListToConfig(
			final List<URL> urls,
			final String delimiter,
			final Configuration configuration,
			final ConfigOption<String> option) {

		checkNotNull(urls);
		checkNotNull(delimiter);
		checkNotNull(configuration);
		checkNotNull(option);

		final String str = urls.stream().map(URL::toString).collect(Collectors.joining(delimiter));
		configuration.setString(option, str);
		return str;
	}

	/**
	 * Parses a string into a list of {@link URL URLs} from a given {@link Configuration}.
	 * @param configuration the configuration containing the string-ified list of URLs
	 * @param option the {@link ConfigOption option} whose value is the list of URLs
	 * @param delimiter the delimiter used to separate the members of the list in the string
	 * @return the produced list of URLs.
	 */
	static List<URL> urlListFromConfig(
			final Configuration configuration,
			final ConfigOption<String> option,
			final String delimiter) {

		checkNotNull(configuration);
		checkNotNull(option);
		checkNotNull(delimiter);

		final String urls = configuration.getString(option);
		if (urls == null || urls.length() == 0) {
			return Collections.emptyList();
		}

		try (final Stream<String> urlTokens = Arrays.stream(urls.split(delimiter))) {
			return urlTokens.map(str -> {
				try {
					return new URL(str);
				} catch (MalformedURLException e) {
					throw new IllegalArgumentException("Invalid URL", e);
				}
			}).collect(Collectors.toList());
		}
	}
}
