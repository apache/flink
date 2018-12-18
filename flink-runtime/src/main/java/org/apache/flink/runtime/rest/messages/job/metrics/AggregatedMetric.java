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
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * Response type for aggregated metrics. Contains the metric name and optionally the sum, average, minimum and maximum.
 */
public class AggregatedMetric {

	private static final String FIELD_NAME_ID = "id";

	private static final String FIELD_NAME_MIN = "min";

	private static final String FIELD_NAME_MAX = "max";

	private static final String FIELD_NAME_AVG = "avg";

	private static final String FIELD_NAME_SUM = "sum";

	@JsonProperty(value = FIELD_NAME_ID, required = true)
	private final String id;

	@JsonInclude(JsonInclude.Include.NON_NULL)
	@JsonProperty(FIELD_NAME_MIN)
	private final Double min;

	@JsonInclude(JsonInclude.Include.NON_NULL)
	@JsonProperty(FIELD_NAME_MAX)
	private final Double max;

	@JsonInclude(JsonInclude.Include.NON_NULL)
	@JsonProperty(FIELD_NAME_AVG)
	private final Double avg;

	@JsonInclude(JsonInclude.Include.NON_NULL)
	@JsonProperty(FIELD_NAME_SUM)
	private final Double sum;

	@JsonCreator
	public AggregatedMetric(
		final @JsonProperty(value = FIELD_NAME_ID, required = true) String id,
		final @Nullable @JsonProperty(FIELD_NAME_MIN) Double min,
		final @Nullable @JsonProperty(FIELD_NAME_MAX) Double max,
		final @Nullable @JsonProperty(FIELD_NAME_AVG) Double avg,
		final @Nullable @JsonProperty(FIELD_NAME_SUM) Double sum) {

		this.id = requireNonNull(id, "id must not be null");
		this.min = min;
		this.max = max;
		this.avg = avg;
		this.sum = sum;
	}

	public AggregatedMetric(final @JsonProperty(value = FIELD_NAME_ID, required = true) String id) {
		this(id, null, null, null, null);
	}

	@JsonIgnore
	public String getId() {
		return id;
	}

	@JsonIgnore
	public Double getMin() {
		return min;
	}

	@JsonIgnore
	public Double getMax() {
		return max;
	}

	@JsonIgnore
	public Double getSum() {
		return sum;
	}

	@JsonIgnore
	public Double getAvg() {
		return avg;
	}

	@Override
	public String toString() {
		return "AggregatedMetric{" +
			"id='" + id + '\'' +
			", mim='" + min + '\'' +
			", max='" + max + '\'' +
			", avg='" + avg + '\'' +
			", sum='" + sum + '\'' +
			'}';
	}
}
