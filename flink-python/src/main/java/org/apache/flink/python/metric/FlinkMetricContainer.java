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

package org.apache.flink.python.metric;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.metrics.MetricGroup;

import java.util.Arrays;

/**
 * Helper class for forwarding metric group information from Java to Python and forward Python
 * metrics to Java accumulators and metrics.
 */
@Internal
public class FlinkMetricContainer {

	private static final String METRIC_KEY_SEPARATOR =
		GlobalConfiguration.loadConfiguration().getString(MetricOptions.SCOPE_DELIMITER);

	private final MetricGroup baseMetricGroup;

	public FlinkMetricContainer(MetricGroup metricGroup) {
		this.baseMetricGroup = metricGroup;
	}

	public FlinkFnApi.MetricGroupInfo getBaseMetricGroupInfo() {
		FlinkFnApi.MetricGroupInfo.Builder builder = FlinkFnApi.MetricGroupInfo.newBuilder();
		// components
		builder.addAllScopeComponents(
			Arrays.asList(baseMetricGroup.getScopeComponents()));
		// variables
		builder.putAllVariables(baseMetricGroup.getAllVariables());
		// delimiter
		builder.setDelimiter(METRIC_KEY_SEPARATOR);
		return builder.build();
	}
}
