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

package org.apache.flink.runtime.metrics;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Metric;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.scope.ScopeFormats;

/**
 * Metric registry which does nothing.
 */
public class NoOpMetricRegistry implements MetricRegistry {
	private static final char delimiter = '.';
	private static final ScopeFormats scopeFormats = ScopeFormats.fromConfig(new Configuration());

	public static final MetricRegistry INSTANCE = new NoOpMetricRegistry();

	private NoOpMetricRegistry() {
	}

	@Override
	public char getDelimiter() {
		return delimiter;
	}

	@Override
	public int getNumberReporters() {
		return 0;
	}

	@Override
	public void register(Metric metric, String metricName, AbstractMetricGroup group) {
	}

	@Override
	public void unregister(Metric metric, String metricName, AbstractMetricGroup group) {
	}

	@Override
	public ScopeFormats getScopeFormats() {
		return scopeFormats;
	}
}
