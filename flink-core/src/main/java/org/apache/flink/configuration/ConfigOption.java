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
import org.apache.flink.configuration.description.Description;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@code ConfigOption} describes a configuration parameter. It encapsulates
 * the configuration key, deprecated older versions of the key, and an optional
 * default value for the configuration parameter.
 *
 * <p>{@code ConfigOptions} are built via the {@link ConfigOptions} class.
 * Once created, a config option is immutable.
 *
 * @param <T> The type of value associated with the configuration option.
 */
@PublicEvolving
public class ConfigOption<T> {

	private static final FallbackKey[] EMPTY = new FallbackKey[0];

	// ------------------------------------------------------------------------

	/** The current key for that config option. */
	private final String key;

	/** The list of deprecated keys, in the order to be checked. */
	private final FallbackKey[] fallbackKeys;

	/** The default value for this option. */
	private final T defaultValue;

	/** The description for this option. */
	private final Description description;

	// ------------------------------------------------------------------------

	/**
	 * Creates a new config option with no fallback keys.
	 *
	 * @param key The current key for that config option
	 * @param defaultValue The default value for this option
	 */
	ConfigOption(String key, T defaultValue) {
		this.key = checkNotNull(key);
		this.description = Description.builder().text("").build();
		this.defaultValue = defaultValue;
		this.fallbackKeys = EMPTY;
	}

	/**
	 * Creates a new config option with fallback keys.
	 *
	 * @param key The current key for that config option
	 * @param description Description for that option
	 * @param defaultValue The default value for this option
	 * @param fallbackKeys The list of fallback keys, in the order to be checked
	 * @deprecated use version with {@link Description} instead
	 */
	@Deprecated
	ConfigOption(String key, String description, T defaultValue, FallbackKey... fallbackKeys) {
		this.key = checkNotNull(key);
		this.description = Description.builder().text(description).build();
		this.defaultValue = defaultValue;
		this.fallbackKeys = fallbackKeys == null || fallbackKeys.length == 0 ? EMPTY : fallbackKeys;
	}

	/**
	 * Creates a new config option with fallback keys.
	 *
	 * @param key The current key for that config option
	 * @param description Description for that option
	 * @param defaultValue The default value for this option
	 * @param fallbackKeys The list of fallback keys, in the order to be checked
	 */
	ConfigOption(String key, Description description, T defaultValue, FallbackKey... fallbackKeys) {
		this.key = checkNotNull(key);
		this.description = description;
		this.defaultValue = defaultValue;
		this.fallbackKeys = fallbackKeys == null || fallbackKeys.length == 0 ? EMPTY : fallbackKeys;
	}

	// ------------------------------------------------------------------------

	/**
	 * Creates a new config option, using this option's key and default value, and
	 * adding the given fallback keys.
	 *
	 * <p>When obtaining a value from the configuration via {@link Configuration#getValue(ConfigOption)},
	 * the fallback keys will be checked in the order provided to this method. The first key for which
	 * a value is found will be used - that value will be returned.
	 *
	 * @param fallbackKeys The fallback keys, in the order in which they should be checked.
	 * @return A new config options, with the given fallback keys.
	 */
	public ConfigOption<T> withFallbackKeys(String... fallbackKeys) {
		final Stream<FallbackKey> newFallbackKeys = Arrays.stream(fallbackKeys).map(FallbackKey::createFallbackKey);
		final Stream<FallbackKey> currentAlternativeKeys = Arrays.stream(this.fallbackKeys);

		// put fallback keys first so that they are prioritized
		final FallbackKey[] mergedAlternativeKeys = Stream.concat(newFallbackKeys, currentAlternativeKeys)
			.toArray(FallbackKey[]::new);
		return new ConfigOption<>(key, description, defaultValue, mergedAlternativeKeys);
	}

	/**
	 * Creates a new config option, using this option's key and default value, and
	 * adding the given deprecated keys.
	 *
	 * <p>When obtaining a value from the configuration via {@link Configuration#getValue(ConfigOption)},
	 * the deprecated keys will be checked in the order provided to this method. The first key for which
	 * a value is found will be used - that value will be returned.
	 *
	 * @param deprecatedKeys The deprecated keys, in the order in which they should be checked.
	 * @return A new config options, with the given deprecated keys.
	 */
	public ConfigOption<T> withDeprecatedKeys(String... deprecatedKeys) {
		final Stream<FallbackKey> newDeprecatedKeys = Arrays.stream(deprecatedKeys).map(FallbackKey::createDeprecatedKey);
		final Stream<FallbackKey> currentAlternativeKeys = Arrays.stream(this.fallbackKeys);

		// put deprecated keys last so that they are de-prioritized
		final FallbackKey[] mergedAlternativeKeys = Stream.concat(currentAlternativeKeys, newDeprecatedKeys)
			.toArray(FallbackKey[]::new);
		return new ConfigOption<>(key, description, defaultValue, mergedAlternativeKeys);
	}

	/**
	 * Creates a new config option, using this option's key and default value, and
	 * adding the given description. The given description is used when generation the configuration documention.
	 *
	 * @param description The description for this option.
	 * @return A new config option, with given description.
	 */
	public ConfigOption<T> withDescription(final String description) {
		return withDescription(Description.builder().text(description).build());
	}

	/**
	 * Creates a new config option, using this option's key and default value, and
	 * adding the given description. The given description is used when generation the configuration documention.
	 *
	 * @param description The description for this option.
	 * @return A new config option, with given description.
	 */
	public ConfigOption<T> withDescription(final Description description) {
		return new ConfigOption<>(key, description, defaultValue, fallbackKeys);
	}

	// ------------------------------------------------------------------------

	/**
	 * Gets the configuration key.
	 * @return The configuration key
	 */
	public String key() {
		return key;
	}

	/**
	 * Checks if this option has a default value.
	 * @return True if it has a default value, false if not.
	 */
	public boolean hasDefaultValue() {
		return defaultValue != null;
	}

	/**
	 * Returns the default value, or null, if there is no default value.
	 * @return The default value, or null.
	 */
	public T defaultValue() {
		return defaultValue;
	}

	/**
	 * Checks whether this option has deprecated keys.
	 * @return True if the option has deprecated keys, false if not.
	 * @deprecated Replaced by {@link #hasFallbackKeys()}
	 */
	@Deprecated
	public boolean hasDeprecatedKeys() {
		return fallbackKeys == EMPTY ? false :
			Arrays.stream(fallbackKeys).anyMatch(FallbackKey::isDeprecated);
	}

	/**
	 * Gets the deprecated keys, in the order to be checked.
	 * @return The option's deprecated keys.
	 * @deprecated Replaced by {@link #fallbackKeys()}
	 */
	@Deprecated
	public Iterable<String> deprecatedKeys() {
		return fallbackKeys == EMPTY ? Collections.<String>emptyList() :
			Arrays.stream(fallbackKeys)
				.filter(FallbackKey::isDeprecated)
				.map(FallbackKey::getKey)
				.collect(Collectors.toList());
	}

	/**
	 * Checks whether this option has fallback keys.
	 * @return True if the option has fallback keys, false if not.
	 */
	public boolean hasFallbackKeys() {
		return fallbackKeys != EMPTY;
	}

	/**
	 * Gets the fallback keys, in the order to be checked.
	 * @return The option's fallback keys.
	 */
	public Iterable<FallbackKey> fallbackKeys() {
		return (fallbackKeys == EMPTY) ? Collections.emptyList() : Arrays.asList(fallbackKeys);
	}

	/**
	 * Returns the description of this option.
	 * @return The option's description.
	 */
	public Description description() {
		return description;
	}

	// ------------------------------------------------------------------------

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		else if (o != null && o.getClass() == ConfigOption.class) {
			ConfigOption<?> that = (ConfigOption<?>) o;
			return this.key.equals(that.key) &&
					Arrays.equals(this.fallbackKeys, that.fallbackKeys) &&
					(this.defaultValue == null ? that.defaultValue == null :
							(that.defaultValue != null && this.defaultValue.equals(that.defaultValue)));
		}
		else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return 31 * key.hashCode() +
				17 * Arrays.hashCode(fallbackKeys) +
				(defaultValue != null ? defaultValue.hashCode() : 0);
	}

	@Override
	public String toString() {
		return String.format("Key: '%s' , default: %s (fallback keys: %s)",
				key, defaultValue, Arrays.toString(fallbackKeys));
	}
}
