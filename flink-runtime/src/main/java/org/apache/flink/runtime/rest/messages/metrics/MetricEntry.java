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

package org.apache.flink.runtime.rest.messages.metrics;

import org.apache.flink.util.Preconditions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


/**
 *
 */
public class MetricEntry {
	public static final String FIELD_NAME_ID = "id";
	public static final String FIELD_NAME_VALUE = "value";

	@JsonProperty(FIELD_NAME_ID)
	private final String id;

	@JsonProperty(FIELD_NAME_VALUE)
	private final String value;

	@JsonCreator
	public MetricEntry(
			@JsonProperty(FIELD_NAME_ID) String id,
			@JsonProperty(FIELD_NAME_VALUE) String value) {
		Preconditions.checkNotNull("id cannot be null", id);
		Preconditions.checkNotNull("value cannot be null", value);

		this.id = id;
		this.value = value;
	}

	public String getId() {
		return id;
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

		MetricEntry that = (MetricEntry) o;

		return id.equals(that.getId()) && value.equals(that.value);
	}

	@Override
	public int hashCode() {
		int result = id != null ? id.hashCode() : 0;
		result = 31 * result + (value != null ? value.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return "MetricEntry {" +
				"id=" + id +
				", value=" + value +
				"}";
	}
}
