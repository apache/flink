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

package org.apache.flink.table.runtime.utils;

import org.apache.flink.python.env.PythonDependencyInfo;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.python.env.beam.ProcessPythonEnvironmentManager;
import org.apache.flink.python.metric.FlinkMetricContainer;
import org.apache.flink.python.util.PythonEnvironmentManagerUtils;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.metrics.groups.GenericMetricGroup;
import org.apache.flink.runtime.metrics.groups.MetricGroupTest;

import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.util.WindowedValue;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Utilities for Python Tests.
 */
public final class PythonTestUtils {

	public static JobBundleFactory createMockJobBundleFactory() {
		JobBundleFactory jobBundleFactorySpy = spy(JobBundleFactory.class);
		StageBundleFactory stageBundleFactorySpy = spy(StageBundleFactory.class);
		when(jobBundleFactorySpy.forStage(any())).thenReturn(stageBundleFactorySpy);
		RemoteBundle remoteBundleSpy = spy(RemoteBundle.class);
		try {
			when(stageBundleFactorySpy.getBundle(any(), any(), any())).thenReturn(remoteBundleSpy);
		} catch (Exception e) {
			// ignore
		}
		Map<String, FnDataReceiver> inputReceivers = new HashMap<>();
		FnDataReceiver<WindowedValue<?>> windowedValueReceiverSpy = spy(FnDataReceiver.class);
		inputReceivers.put("input", windowedValueReceiverSpy);
		when(remoteBundleSpy.getInputReceivers()).thenReturn(inputReceivers);
		return jobBundleFactorySpy;
	}

	public static FlinkMetricContainer createMockFlinkMetricContainer() {
		return new FlinkMetricContainer(
			new GenericMetricGroup(
				NoOpMetricRegistry.INSTANCE,
				new MetricGroupTest.DummyAbstractMetricGroup(NoOpMetricRegistry.INSTANCE),
				"root"));
	}

	public static PythonEnvironmentManager createTestEnvironmentManager() {
		Map<String, String> env = new HashMap<>();
		env.put(PythonEnvironmentManagerUtils.PYFLINK_UDF_RUNNER_DIR, "");
		return new ProcessPythonEnvironmentManager(
			new PythonDependencyInfo(new HashMap<>(), null, null, new HashMap<>(), "python"),
			new String[] {System.getProperty("java.io.tmpdir")},
			env);
	}
}
