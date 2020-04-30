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

package org.apache.flink.runtime.rest.messages.job.metrics;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * Response type for a Metric and Metric-Value-Pair.
 *
 * @see org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore
 */
public class Metric {

	private static final String FIELD_NAME_ID = "id";

	private static final String FIELD_NAME_VALUE = "value";

	@JsonProperty(value = FIELD_NAME_ID, required = true)
	private final String id;

	/**
	 * The value of the metric. If <code>null</code>, the field should not show up in the JSON
	 * representation.
	 */
	@JsonInclude(JsonInclude.Include.NON_NULL)
	@JsonProperty(value = FIELD_NAME_VALUE)
	private final String value;

	/**
	 * Creates a new {@link Metric} with a possible value.
	 *
	 * @param id    Name of the metric.
	 * @param value Value of the metric. Can be <code>null</code>.
	 */
	@JsonCreator
	public Metric(
			final @JsonProperty(value = FIELD_NAME_ID, required = true) String id,
			final @Nullable @JsonProperty(FIELD_NAME_VALUE) String value) {

		this.id = requireNonNull(id, "id must not be null");
		this.value = value;
	}

	/**
	 * Creates a new {@link Metric} without a value.
	 *
	 * @param id Name of the metric.
	 */
	public Metric(final String id) {
		this(id, null);
	}

	public String getId() {
		return id;
	}

	public String getValue() {
		return value;
	}

	@Override
	public String toString() {
		return "Metric{" +
			"id='" + id + '\'' +
			", value='" + value + '\'' +
			'}';
	}

}
