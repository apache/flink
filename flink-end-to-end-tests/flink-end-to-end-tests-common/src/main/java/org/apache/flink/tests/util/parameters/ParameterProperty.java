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

package org.apache.flink.tests.util.parameters;

import java.util.Optional;
import java.util.function.Function;

/**
 * System-property based parameters for tests and resources.
 */
public class ParameterProperty<V> {

	private final String propertyName;
	private final Function<String, V> converter;

	public ParameterProperty(final String propertyName, final Function<String, V> converter) {
		this.propertyName = propertyName;
		this.converter = converter;
	}

	public String getPropertyName() {
		return propertyName;
	}

	/**
	 * Retrieves the value of this property.
	 *
	 * @return Optional containing the value of this property
	 */
	public Optional<V> get() {
		final String value = System.getProperty(propertyName);
		return value == null
			? Optional.empty()
			: Optional.of(converter.apply(value));
	}

	/**
	 * Retrieves the value of this property, or the given default if no value was set.
	 *
	 * @return the value of this property, or the given default if no value was set
	 */
	public V get(final V defaultValue) {
		final String value = System.getProperty(propertyName);
		return value == null
			? defaultValue
			: converter.apply(value);
	}
}
