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

import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.python.metric.FlinkMetricContainer;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.runners.python.scalar.AbstractGeneralPythonScalarFunctionRunner;
import org.apache.flink.table.types.logical.RowType;

import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.Struct;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.runtime.utils.PythonTestUtils.createMockJobBundleFactory;

/**
 * A Python ScalarFunction runner that just return the input elements as the execution results.
 *
 * @param <IN> Type of the input elements.
 */
public abstract class PassThroughPythonScalarFunctionRunner<IN> extends AbstractGeneralPythonScalarFunctionRunner<IN> {

	private final JobBundleFactory jobBundleFactory;
	private final List<byte[]> bufferedInputs;

	public PassThroughPythonScalarFunctionRunner(
		String taskName,
		FnDataReceiver<byte[]> resultReceiver,
		PythonFunctionInfo[] scalarFunctions,
		PythonEnvironmentManager environmentManager,
		RowType inputType,
		RowType outputType,
		Map<String, String> jobOptions,
		FlinkMetricContainer flinkMetricContainer) {
		this(taskName, resultReceiver, scalarFunctions, environmentManager, inputType, outputType, jobOptions, createMockJobBundleFactory(), flinkMetricContainer);
	}

	public PassThroughPythonScalarFunctionRunner(
		String taskName,
		FnDataReceiver<byte[]> resultReceiver,
		PythonFunctionInfo[] scalarFunctions,
		PythonEnvironmentManager environmentManager,
		RowType inputType,
		RowType outputType,
		Map<String, String> jobOptions,
		JobBundleFactory jobBundleFactory,
		FlinkMetricContainer flinkMetricContainer) {
		super(taskName, resultReceiver, scalarFunctions, environmentManager, inputType, outputType, jobOptions, flinkMetricContainer);
		this.jobBundleFactory = jobBundleFactory;
		this.bufferedInputs = new ArrayList<>();
	}

	@Override
	public void startBundle() {
		super.startBundle();
		this.mainInputReceiver = input -> bufferedInputs.add(input.getValue());
	}

	@Override
	public void finishBundle() throws Exception {
		super.finishBundle();
		for (byte[] input : bufferedInputs) {
			resultReceiver.accept(input);
		}
		bufferedInputs.clear();
	}

	@Override
	public JobBundleFactory createJobBundleFactory(Struct pipelineOptions) {
		return jobBundleFactory;
	}
}
