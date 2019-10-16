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

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.MetricScope;
import org.apache.flink.runtime.metrics.scope.InternalMetricScope;

import java.util.Map;

/**
 * Metric group which forwards all registration calls to a variable parent metric group that injects a variable reporter
 * index into calls to {@link org.apache.flink.metrics.MetricGroup#getMetricIdentifier(String)}
 * or {@link org.apache.flink.metrics.MetricGroup#getMetricIdentifier(String, CharacterFilter)}.
 * This allows us to use reporter-specific delimiters, without requiring any action by the reporter.
 *
 * @param <P> parentMetricGroup to {@link AbstractMetricGroup AbstractMetricGroup}
 */
public class FrontMetricGroup<P extends AbstractMetricGroup<?>> extends ProxyMetricGroup<P> {

	private final ReporterIndexInjectingMetricScope scope;

	public FrontMetricGroup(int reporterIndex, P reference) {
		super(reference);
		this.scope = new ReporterIndexInjectingMetricScope(reporterIndex, this.parentMetricGroup.getScope());
	}

	@Override
	public String getMetricIdentifier(String metricName) {
		return scope.getMetricIdentifier(metricName);
	}

	@Override
	public String getMetricIdentifier(String metricName, CharacterFilter filter) {
		return scope.getMetricIdentifier(metricName, filter);
	}

	public String getLogicalScope(CharacterFilter filter) {
		return parentMetricGroup.getLogicalScope(filter);
	}

	public String getLogicalScope(CharacterFilter filter, char delimiter) {
		return parentMetricGroup.getLogicalScope(filter, delimiter, scope.reporterIndex);
	}

	private static final class ReporterIndexInjectingMetricScope implements MetricScope {

		private final int reporterIndex;
		private final InternalMetricScope scope;

		private ReporterIndexInjectingMetricScope(int reporterIndex, InternalMetricScope scope) {
			this.reporterIndex = reporterIndex;
			this.scope = scope;
		}

		@Override
		public Map<String, String> getAllVariables() {
			return scope.getAllVariables();
		}

		@Override
		public String getMetricIdentifier(String metricName) {
			return scope.getMetricIdentifier(metricName, s -> s, reporterIndex);
		}

		@Override
		public String getMetricIdentifier(String metricName, CharacterFilter filter) {
			return scope.getMetricIdentifier(metricName, filter, reporterIndex);
		}
	}
}
