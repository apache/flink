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

package org.apache.flink.table.runtime.runners.python.beam;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.python.metric.FlinkMetricContainer;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.streaming.api.runners.python.beam.BeamPythonFunctionRunner;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.beam.model.pipeline.v1.RunnerApi;

import java.util.Map;

import static org.apache.flink.table.runtime.typeutils.PythonTypeUtils.getRowCoderProto;

/**
 * A {@link BeamTableStatefulPythonFunctionRunner} used to execute Python stateful functions.
 */
public class BeamTableStatefulPythonFunctionRunner extends BeamPythonFunctionRunner {

	private final RowType inputType;
	private final RowType outputType;
	private final String inputCoderUrn;
	private final String outputCoderUrn;
	private final FlinkFnApi.UserDefinedAggregateFunctions userDefinedAggregateFunctions;

	public BeamTableStatefulPythonFunctionRunner(
			String taskName,
			PythonEnvironmentManager environmentManager,
			RowType inputType,
			RowType outputType,
			String functionUrn,
			FlinkFnApi.UserDefinedAggregateFunctions userDefinedFunctions,
			String inputCoderUrn,
			String outputCoderUrn,
			Map<String, String> jobOptions,
			FlinkMetricContainer flinkMetricContainer,
			KeyedStateBackend keyedStateBackend,
			TypeSerializer keySerializer) {
		super(
			taskName,
			environmentManager,
			functionUrn,
			jobOptions,
			flinkMetricContainer,
			keyedStateBackend,
			keySerializer);
		this.inputCoderUrn = Preconditions.checkNotNull(inputCoderUrn);
		this.outputCoderUrn = Preconditions.checkNotNull(outputCoderUrn);
		this.inputType = Preconditions.checkNotNull(inputType);
		this.outputType = Preconditions.checkNotNull(outputType);
		this.userDefinedAggregateFunctions = userDefinedFunctions;
	}

	@Override
	protected byte[] getUserDefinedFunctionsProtoBytes() {
		return this.userDefinedAggregateFunctions.toByteArray();
	}

	@Override
	protected RunnerApi.Coder getInputCoderProto() {
		return getRowCoderProto(inputType, inputCoderUrn);
	}

	@Override
	protected RunnerApi.Coder getOutputCoderProto() {
		return getRowCoderProto(outputType, outputCoderUrn);
	}
}
