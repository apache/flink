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

import org.apache.flink.annotation.VisibleForTesting;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.List;

/**
 * Abstract metric of Datadog for serialization.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class DMetric {

	@VisibleForTesting
	static final String FIELD_NAME_TYPE = "type";
	@VisibleForTesting
	static final String FIELD_NAME_METRIC = "metric";
	@VisibleForTesting
	static final String FIELD_NAME_HOST = "host";
	@VisibleForTesting
	static final String FIELD_NAME_TAGS = "tags";
	@VisibleForTesting
	static final String FIELD_NAME_POINTS = "points";

	private final MetricMetaData metaData;

	public DMetric(MetricMetaData metaData) {
		this.metaData = metaData;
	}

	@JsonGetter(FIELD_NAME_TYPE)
	public MetricType getType() {
		return metaData.getType();
	}

	@JsonGetter(FIELD_NAME_METRIC)
	public String getMetricName() {
		return metaData.getMetricName();
	}

	@JsonGetter(FIELD_NAME_HOST)
	public String getHost() {
		return metaData.getHost();
	}

	@JsonGetter(FIELD_NAME_TAGS)
	public List<String> getTags() {
		return metaData.getTags();
	}

	@JsonGetter(FIELD_NAME_POINTS)
	public List<List<Number>> getPoints() {
		// One single data point
		List<Number> point = new ArrayList<>();
		point.add(metaData.getClock().getUnixEpochTimestamp());
		point.add(getMetricValue());

		List<List<Number>> points = new ArrayList<>();
		points.add(point);

		return points;
	}

	@JsonIgnore
	public abstract Number getMetricValue();

	public void ackReport() {
	}
}
