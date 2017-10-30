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

package org.apache.flink.metrics.datadog;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.List;

/**
 * Abstract metric of Datadog for serialization.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class DMetric {
	private static final long MILLIS_TO_SEC = 1000L;

	/**
	 * Names of metric/type/tags field and their getters must not be changed
	 * since they are mapped to json objects in a Datadog-defined format.
	 */
	private final String metric; // Metric name
	private final MetricType type;
	private final String host;
	private final List<String> tags;

	public DMetric(MetricType metricType, String metric, String host, List<String> tags) {
		this.type = metricType;
		this.metric = metric;
		this.host = host;
		this.tags = tags;
	}

	public MetricType getType() {
		return type;
	}

	public String getMetric() {
		return metric;
	}

	public String getHost() {
		return host;
	}

	public List<String> getTags() {
		return tags;
	}

	public List<List<Number>> getPoints() {
		// One single data point
		List<Number> point = new ArrayList<>();
		point.add(getUnixEpochTimestamp());
		point.add(getMetricValue());

		List<List<Number>> points = new ArrayList<>();
		points.add(point);

		return points;
	}

	@JsonIgnore
	public abstract Number getMetricValue();

	public static long getUnixEpochTimestamp() {
		return (System.currentTimeMillis() / MILLIS_TO_SEC);
	}
}
