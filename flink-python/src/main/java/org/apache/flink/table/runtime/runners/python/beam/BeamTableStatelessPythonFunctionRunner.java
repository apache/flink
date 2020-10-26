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

import org.apache.flink.annotation.Internal;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.python.metric.FlinkMetricContainer;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.streaming.api.runners.python.beam.BeamPythonFunctionRunner;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.beam.model.pipeline.v1.RunnerApi;

import java.util.Map;

import static org.apache.flink.table.runtime.typeutils.PythonTypeUtils.getRowCoderProto;

/**
 * A {@link BeamTableStatelessPythonFunctionRunner} used to execute Python stateless functions.
 */
@Internal
public class BeamTableStatelessPythonFunctionRunner extends BeamPythonFunctionRunner {

	private final RowType inputType;
	private final RowType outputType;
	private final String coderUrn;
	private final FlinkFnApi.UserDefinedFunctions userDefinedFunctions;

	public BeamTableStatelessPythonFunctionRunner(
		String taskName,
		PythonEnvironmentManager environmentManager,
		RowType inputType,
		RowType outputType,
		String functionUrn,
		FlinkFnApi.UserDefinedFunctions userDefinedFunctions,
		String coderUrn,
		Map<String, String> jobOptions,
		FlinkMetricContainer flinkMetricContainer,
		MemoryManager memoryManager,
		double managedMemoryFraction) {
		super(taskName, environmentManager, functionUrn, jobOptions, flinkMetricContainer, null, null, memoryManager, managedMemoryFraction);
		this.coderUrn = Preconditions.checkNotNull(coderUrn);
		this.inputType = Preconditions.checkNotNull(inputType);
		this.outputType = Preconditions.checkNotNull(outputType);
		this.userDefinedFunctions = userDefinedFunctions;
	}

	@Override
	protected byte[] getUserDefinedFunctionsProtoBytes() {
		return this.userDefinedFunctions.toByteArray();
	}

	@Override
	protected RunnerApi.Coder getInputCoderProto() {
		return getRowCoderProto(inputType, coderUrn);
	}

	@Override
	protected RunnerApi.Coder getOutputCoderProto() {
		return getRowCoderProto(outputType, coderUrn);
	}
}
