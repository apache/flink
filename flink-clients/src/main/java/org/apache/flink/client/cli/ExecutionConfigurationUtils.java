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
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utilities for parsing parameters in the {@link ExecutionConfigAccessor}.
 */
@Internal
class ExecutionConfigurationUtils {

	/**
	 * Parses a list of {@link URL URLs} to a string and puts it in the provided {@code configuration} as the value of the provided {@code option}.
	 * @param urls the list of URLs to parse
	 * @param configuration the configuration object to put the list
	 * @param option the {@link ConfigOption option} to serve as the key for the list in the configuration
	 * @return the produced list of strings to be put in the configuration.
	 */
	static List<String> urlListToConfig(
			final List<URL> urls,
			final Configuration configuration,
			final ConfigOption<List<String>> option) {

		checkNotNull(urls);
		checkNotNull(configuration);
		checkNotNull(option);

		final List<String> str = urls.stream().map(URL::toString).collect(Collectors.toList());
		configuration.set(option, str);
		return str;
	}

	/**
	 * Parses a string into a list of {@link URL URLs} from a given {@link Configuration}.
	 * @param configuration the configuration containing the string-ified list of URLs
	 * @param option the {@link ConfigOption option} whose value is the list of URLs
	 * @return the produced list of URLs.
	 */
	static List<URL> urlListFromConfig(
			final Configuration configuration,
			final ConfigOption<List<String>> option) {

		checkNotNull(configuration);
		checkNotNull(option);

		final List<String> urls = configuration.get(option);
		if (urls == null || urls.isEmpty()) {
			return Collections.emptyList();
		}

		return urls.stream().map(str -> {
			try {
				return new URL(str);
			} catch (MalformedURLException e) {
				throw new IllegalArgumentException("Invalid URL", e);
			}
		}).collect(Collectors.toList());
	}
}
