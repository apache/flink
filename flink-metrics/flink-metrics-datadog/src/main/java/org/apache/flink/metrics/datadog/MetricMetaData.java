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

import java.util.List;

/**
 * All metadata associated with a given metric.
 */
public final class MetricMetaData {

	private final String metricName;
	private final MetricType type;
	private final String host;
	private final List<String> tags;
	private final Clock clock;

	public MetricMetaData(MetricType metricType, String metricName, String host, List<String> tags, Clock clock) {
		this.type = metricType;
		this.metricName = metricName;
		this.host = host;
		this.tags = tags;
		this.clock = clock;
	}

	public MetricType getType() {
		return type;
	}

	public String getMetricName() {
		return metricName;
	}

	public String getHost() {
		return host;
	}

	public List<String> getTags() {
		return tags;
	}

	public Clock getClock() {
		return clock;
	}
}
