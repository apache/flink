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

package org.apache.flink.streaming.api.operators.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.streaming.api.runners.python.beam.BeamDataStreamPythonFunctionRunner;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.streaming.api.utils.PythonOperatorUtils.getUserDefinedDataStreamFunctionProto;

/**
 * The {@link PythonPartitionCustomOperator} enables us to set the number of partitions for current
 * operator dynamically when generating the {@link org.apache.flink.streaming.api.graph.StreamGraph} before executing
 * the job. The number of partitions will be set in environment variables for python Worker, so that we can obtain the
 * number of partitions when executing user defined partitioner function.
 */
@Internal
public class PythonPartitionCustomOperator<IN, OUT> extends StatelessOneInputPythonFunctionOperator<IN, OUT> {

	private static final long serialVersionUID = 1L;

	private static final String NUM_PARTITIONS = "NUM_PARTITIONS";

	private int numPartitions = CoreOptions.DEFAULT_PARALLELISM.defaultValue();

	public PythonPartitionCustomOperator(
		Configuration config,
		TypeInformation<IN> inputTypeInfo,
		TypeInformation<OUT> outputTypeInfo,
		DataStreamPythonFunctionInfo pythonFunctionInfo) {
		super(config, inputTypeInfo, outputTypeInfo, pythonFunctionInfo);
	}

	@Override
	public PythonFunctionRunner createPythonFunctionRunner() throws Exception {
		PythonEnvironmentManager pythonEnvironmentManager = createPythonEnvironmentManager();
		Map<String, String> internalParameters = new HashMap<>();
		internalParameters.put(NUM_PARTITIONS, String.valueOf(this.numPartitions));
		return new BeamDataStreamPythonFunctionRunner(
			getRuntimeContext().getTaskName(),
			pythonEnvironmentManager,
			inputTypeInfo,
			outputTypeInfo,
			DATA_STREAM_STATELESS_FUNCTION_URN,
			getUserDefinedDataStreamFunctionProto(pythonFunctionInfo, getRuntimeContext(), internalParameters),
			MAP_CODER_URN,
			jobOptions,
			getFlinkMetricContainer(),
			null,
			null,
			getContainingTask().getEnvironment().getMemoryManager(),
			getOperatorConfig().getManagedMemoryFractionOperatorUseCaseOfSlot(
				ManagedMemoryUseCase.PYTHON,
				getContainingTask().getEnvironment().getTaskManagerInfo().getConfiguration(),
				getContainingTask().getEnvironment().getUserCodeClassLoader().asClassLoader())
		);
	}

	public void setNumPartitions(int numPartitions) {
		this.numPartitions = numPartitions;
	}
}
