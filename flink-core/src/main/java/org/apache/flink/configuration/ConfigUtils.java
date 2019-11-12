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

package org.apache.flink.configuration;

import org.apache.flink.annotation.Internal;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * General utilities for parsing values to configuration options.
 */
@Internal
public class ConfigUtils {

	/**
	 * Puts an array of values of type {@code IN} in a {@link WritableConfig}
	 * as a {@link ConfigOption} of type {@link List} of type {@code OUT}.
	 *
	 * @param configuration the configuration object to put the list in
	 * @param key the {@link ConfigOption option} to serve as the key for the list in the configuration
	 * @param value the array of values to put as value for the {@code key}
	 * @param mapper the transformation function from {@code IN} to {@code OUT}.
	 */
	public static <IN, OUT> void encodeArrayToConfig(
			final WritableConfig configuration,
			final ConfigOption<List<OUT>> key,
			final IN[] value,
			final Function<IN, OUT> mapper) {
		if (value == null) {
			return;
		}
		encodeStreamToConfig(configuration, key, Arrays.stream(value), mapper);
	}

	/**
	 * Puts a {@link Stream} of values of type {@code IN} in a {@link WritableConfig}
	 * as a {@link ConfigOption} of type {@link List} of type {@code OUT}.
	 *
	 * @param configuration the configuration object to put the list in
	 * @param key the {@link ConfigOption option} to serve as the key for the list in the configuration
	 * @param values the stream of values to put as value for the {@code key}
	 * @param mapper the transformation function from {@code IN} to {@code OUT}.
	 */
	public static <IN, OUT> void encodeStreamToConfig(
			final WritableConfig configuration,
			final ConfigOption<List<OUT>> key,
			final Stream<IN> values,
			final Function<IN, OUT> mapper) {

		checkNotNull(configuration);
		checkNotNull(key);
		checkNotNull(mapper);

		if (values == null) {
			return;
		}

		final List<OUT> encodedOption = values
				.map(mapper)
				.filter(Objects::nonNull)
				.collect(Collectors.toList());

		if (!encodedOption.isEmpty()) {
			configuration.set(key, encodedOption);
		}
	}

	/**
	 * Gets a {@link List} of values of type {@code IN} from a {@link ReadableConfig}
	 * and transforms it to a {@link List} of type {@code OUT} based on the provided {@code mapper} function.
	 *
	 * @param configuration the configuration object to get the value out of
	 * @param key the {@link ConfigOption option} to serve as the key for the list in the configuration
	 * @param mapper the transformation function from {@code IN} to {@code OUT}.
	 * @return the transformed values in a list of type {@code OUT}.
	 */
	public static <IN, OUT> List<OUT> decodeListFromConfig(
			final ReadableConfig configuration,
			final ConfigOption<List<IN>> key,
			final Function<IN, OUT> mapper) {

		checkNotNull(configuration);
		checkNotNull(key);
		checkNotNull(mapper);

		final List<IN> encodedString = configuration.get(key);
		return encodedString != null
				? encodedString.stream().map(mapper).collect(Collectors.toList())
				: Collections.emptyList();
	}

	private ConfigUtils() {
	}
}
