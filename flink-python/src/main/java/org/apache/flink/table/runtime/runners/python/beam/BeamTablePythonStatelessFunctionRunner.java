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
import org.apache.flink.streaming.api.runners.python.beam.BeamPythonStatelessFunctionRunner;
import org.apache.flink.table.runtime.typeutils.PythonTypeUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.beam.model.pipeline.v1.RunnerApi;

import java.util.Map;

/**
 * A {@link BeamTablePythonStatelessFunctionRunner} used to execute Python stateless functions.
 */
@Internal
public class BeamTablePythonStatelessFunctionRunner extends BeamPythonStatelessFunctionRunner {

	private final RowType inputType;
	private final RowType outputType;
	private final String coderUrn;
	private final FlinkFnApi.UserDefinedFunctions userDefinedFunctions;

	public BeamTablePythonStatelessFunctionRunner(
		String taskName,
		PythonEnvironmentManager environmentManager,
		RowType inputType,
		RowType outputType,
		String functionUrn,
		FlinkFnApi.UserDefinedFunctions userDefinedFunctions,
		String coderUrn,
		Map<String, String> jobOptions,
		FlinkMetricContainer flinkMetricContainer) {
		super(taskName, environmentManager, functionUrn, jobOptions, flinkMetricContainer);
		this.coderUrn = Preconditions.checkNotNull(coderUrn);
		this.inputType = Preconditions.checkNotNull(inputType);
		this.outputType = Preconditions.checkNotNull(outputType);
		this.userDefinedFunctions = userDefinedFunctions;
	}

	private RunnerApi.Coder getRowCoderProto(RowType rowType) {
		return RunnerApi.Coder.newBuilder()
			.setSpec(
				RunnerApi.FunctionSpec.newBuilder()
					.setUrn(coderUrn)
					.setPayload(org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.ByteString.copyFrom(
						toProtoType(rowType).getRowSchema().toByteArray()))
					.build())
			.build();
	}

	private FlinkFnApi.Schema.FieldType toProtoType(LogicalType logicalType) {
		return logicalType.accept(new PythonTypeUtils.LogicalTypeToProtoTypeConverter());
	}

	@Override
	protected byte[] getUserDefinedFunctionsProtoBytes() {
		return this.userDefinedFunctions.toByteArray();
	}

	@Override
	protected RunnerApi.Coder getInputCoderProto() {
		return getRowCoderProto(inputType);
	}

	@Override
	protected RunnerApi.Coder getOutputCoderProto() {
		return getRowCoderProto(outputType);
	}
}
