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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * A single key-value pair entry in the {@link ClusterConfigurationInfo} response.
 */
public class ClusterConfigurationInfoEntry {

	public static final String FIELD_NAME_CONFIG_KEY = "key";
	public static final String FIELD_NAME_CONFIG_VALUE = "value";

	@JsonProperty(FIELD_NAME_CONFIG_KEY)
	private final String key;

	@JsonProperty(FIELD_NAME_CONFIG_VALUE)
	private final String value;

	@JsonCreator
	public ClusterConfigurationInfoEntry(
			@JsonProperty(FIELD_NAME_CONFIG_KEY) String key,
			@JsonProperty(FIELD_NAME_CONFIG_VALUE) String value) {
		this.key = Preconditions.checkNotNull(key);
		this.value = Preconditions.checkNotNull(value);
	}

	public String getKey() {
		return key;
	}

	public String getValue() {
		return value;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		ClusterConfigurationInfoEntry that = (ClusterConfigurationInfoEntry) o;
		return key.equals(that.key) && value.equals(that.value);
	}

	@Override
	public int hashCode() {
		return Objects.hash(key, value);
	}

	@Override
	public String toString() {
		return "(" + key + "," + value + ")";
	}
}
