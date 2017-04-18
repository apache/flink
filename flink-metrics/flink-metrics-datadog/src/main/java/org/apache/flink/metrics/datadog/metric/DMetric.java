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

package org.apache.flink.metrics.datadog.metric;

import org.apache.flink.metrics.datadog.utils.TimestampUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Abstract metric of Datadog
 * */
public abstract class DMetric<T extends Number> {
	private final String metric;
	private final List<List<Number>> points;
	private final MetricType type;
	private final List<String> tags;

	public DMetric(String metric, T number, MetricType metricType, List<String> tags) {
		this.metric = metric;
		this.points = new ArrayList<>();
		List<Number> point = new ArrayList<>();
		point.add(TimestampUtils.getUnixEpochTimestamp());
		point.add(number);
		this.points.add(point);
		this.type = metricType;
		this.tags = tags;
	}

	public MetricType getType() {
		return type;
	}

	public String getMetric() {
		return metric;
	}

	public List<String> getTags() {
		return tags;
	}

	public List<List<Number>> getPoints() {
		return points;
	}
}
