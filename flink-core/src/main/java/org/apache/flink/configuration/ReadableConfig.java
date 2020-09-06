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

import org.apache.flink.annotation.PublicEvolving;

import java.util.Optional;

/**
 * Read access to a configuration object. Allows reading values described with meta information
 * included in {@link ConfigOption}.
 */
@PublicEvolving
public interface ReadableConfig {

	/**
	 * Reads a value using the metada included in {@link ConfigOption}. Returns the
	 * {@link ConfigOption#defaultValue()} if value key not present in the configuration.
	 *
	 * @param option metadata of the option to read
	 * @param <T> type of the value to read
	 * @return read value or {@link ConfigOption#defaultValue()} if not found
	 * @see #getOptional(ConfigOption)
	 */
	<T> T get(ConfigOption<T> option);

	/**
	 * Reads a value using the metada included in {@link ConfigOption}. In contrast to
	 * {@link #get(ConfigOption)} returns {@link Optional#empty()} if value not present.
	 *
	 * @param option metadata of the option to read
	 * @param <T> type of the value to read
	 * @return read value or {@link Optional#empty()} if not found
	 * @see #get(ConfigOption)
	 */
	<T> Optional<T> getOptional(ConfigOption<T> option);
}
