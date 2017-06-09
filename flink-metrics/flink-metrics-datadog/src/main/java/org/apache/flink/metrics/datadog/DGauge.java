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

import org.apache.flink.metrics.Gauge;

import java.util.List;

/**
 * Mapping of gauge between Flink and Datadog.
 */
public class DGauge extends DMetric {
	private final Gauge<Number> gauge;

	public DGauge(Gauge<Number> g, String metricName, String host, List<String> tags) {
		super(MetricType.gauge, metricName, host, tags);
		gauge = g;
	}

	/**
	 * Visibility of this method must not be changed
	 * since we deliberately not map it to json object in a Datadog-defined format.
	 */
	@Override
	public Number getMetricValue() {
		return gauge.getValue();
	}
}
