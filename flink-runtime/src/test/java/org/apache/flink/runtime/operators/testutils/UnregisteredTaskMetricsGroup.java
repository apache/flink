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

package org.apache.flink.runtime.operators.testutils;

import org.apache.flink.api.common.JobID;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskManagerJobMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.UUID;

public class UnregisteredTaskMetricsGroup extends TaskMetricGroup {
	
	private static final MetricRegistry EMPTY_REGISTRY = new MetricRegistry(MetricRegistryConfiguration.defaultMetricRegistryConfiguration());

	
	public UnregisteredTaskMetricsGroup() {
		super(EMPTY_REGISTRY, new DummyJobMetricGroup(),
				new JobVertexID(), new ExecutionAttemptID(), "testtask", 0, 0);
	}

	@Override
	protected void addMetric(String name, Metric metric) {}

	@Override
	public MetricGroup addGroup(String name) {
		return new UnregisteredMetricsGroup();
	}

	// ------------------------------------------------------------------------
	
	private static class DummyTaskManagerMetricsGroup extends TaskManagerMetricGroup {

		public DummyTaskManagerMetricsGroup() {
			super(EMPTY_REGISTRY, "localhost", UUID.randomUUID().toString());
		}
	}

	private static class DummyJobMetricGroup extends TaskManagerJobMetricGroup {
		
		public DummyJobMetricGroup() {
			super(EMPTY_REGISTRY, new DummyTaskManagerMetricsGroup(), new JobID(), "testjob");
		}
	}
	
	public static class DummyTaskIOMetricGroup extends TaskIOMetricGroup {
		public DummyTaskIOMetricGroup() {
			super(new UnregisteredTaskMetricsGroup());
		}
	}

	public static class DummyOperatorMetricGroup extends OperatorMetricGroup {
		public DummyOperatorMetricGroup() {
			super(EMPTY_REGISTRY, new UnregisteredTaskMetricsGroup(), "testoperator");
		}
	}
}
