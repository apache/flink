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

package org.apache.flink.yarn.cli;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utilities for parsing {@link org.apache.flink.configuration.ConfigOption configuration options}.
 */
public class YarnConfigUtils {

	public static <T> void encodeListToConfig(
			final Configuration configuration,
			final ConfigOption<List<String>> key,
			final Collection<T> value,
			final Function<T, String> mapper) {
		encodeListToConfig(configuration, key, value.stream(), mapper);
	}

	public static <T> void encodeListToConfig(
			final Configuration configuration,
			final ConfigOption<List<String>> key,
			final T[] value,
			final Function<T, String> mapper) {
		encodeListToConfig(configuration, key, Arrays.stream(value), mapper);
	}

	private static <T> void encodeListToConfig(
			final Configuration configuration,
			final ConfigOption<List<String>> key,
			final Stream<T> values,
			final Function<T, String> mapper) {

		checkNotNull(values);
		checkNotNull(key);
		checkNotNull(configuration);

		final List<String> encodedString = values.map(mapper).filter(Objects::nonNull).collect(Collectors.toList());
		if (!encodedString.isEmpty()) {
			configuration.set(key, encodedString);
		}
	}

	public static <R> List<R> decodeListFromConfig(
			final Configuration configuration,
			final ConfigOption<List<String>> key,
			final Function<String, R> mapper) {

		checkNotNull(configuration);
		checkNotNull(key);

		final List<String> encodedString = configuration.get(key);
		return encodedString != null
				? encodedString.stream().map(mapper).collect(Collectors.toList())
				: Collections.emptyList();
	}
}
