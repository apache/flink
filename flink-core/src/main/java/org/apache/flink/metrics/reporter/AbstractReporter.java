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

package org.apache.flink.metrics.reporter;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractReporter implements MetricReporter {
	
	protected Map<String, Gauge<?>> gauges = new ConcurrentHashMap<>();
	protected Map<String, Counter> counters = new ConcurrentHashMap<>();

	@Override
	public void notifyOfAddedMetric(Metric metric, String name) {
		if (metric instanceof Counter) {
			counters.put(name, (Counter) metric);
		} else if (metric instanceof Gauge) {
			gauges.put(name, (Gauge<?>) metric);
		}
	}

	@Override
	public void notifyOfRemovedMetric(Metric metric, String name) {
		if (metric instanceof Counter) {
			counters.remove(name);
		} else if (metric instanceof Gauge) {
			gauges.remove(name);
		}
	}
}
