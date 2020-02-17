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

package org.apache.flink.table.runtime.runners.python.scalar;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.runners.python.AbstractPythonStatelessFunctionRunner;
import org.apache.flink.table.runtime.typeutils.PythonTypeUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.fn.data.FnDataReceiver;

/**
 * Abstract {@link PythonFunctionRunner} used to execute Python {@link ScalarFunction}s.
 *
 * @param <IN> Type of the input elements.
 */
@Internal
public abstract class AbstractPythonScalarFunctionRunner<IN> extends AbstractPythonStatelessFunctionRunner<IN> {

	private static final String SCALAR_FUNCTION_URN = "flink:transform:scalar_function:v1";

	private final PythonFunctionInfo[] scalarFunctions;

	public AbstractPythonScalarFunctionRunner(
		String taskName,
		FnDataReceiver<byte[]> resultReceiver,
		PythonFunctionInfo[] scalarFunctions,
		PythonEnvironmentManager environmentManager,
		RowType inputType,
		RowType outputType) {
		super(taskName, resultReceiver, environmentManager, inputType, outputType, SCALAR_FUNCTION_URN);
		this.scalarFunctions = Preconditions.checkNotNull(scalarFunctions);
	}

	/**
	 * Gets the proto representation of the Python user-defined functions to be executed.
	 */
	@VisibleForTesting
	public FlinkFnApi.UserDefinedFunctions getUserDefinedFunctionsProto() {
		FlinkFnApi.UserDefinedFunctions.Builder builder = FlinkFnApi.UserDefinedFunctions.newBuilder();
		for (PythonFunctionInfo pythonFunctionInfo : scalarFunctions) {
			builder.addUdfs(getUserDefinedFunctionProto(pythonFunctionInfo));
		}
		return builder.build();
	}

	/**
	 * Gets the proto representation of the input coder.
	 */
	@Override
	public RunnerApi.Coder getInputCoderProto() {
		return getRowCoderProto(getInputType());
	}

	/**
	 * Gets the proto representation of the output coder.
	 */
	@Override
	public RunnerApi.Coder getOutputCoderProto() {
		return getRowCoderProto(getOutputType());
	}

	private RunnerApi.Coder getRowCoderProto(RowType rowType) {
		return RunnerApi.Coder.newBuilder()
			.setSpec(
				RunnerApi.FunctionSpec.newBuilder()
					.setUrn(getInputOutputCoderUrn())
					.setPayload(org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.ByteString.copyFrom(
						toProtoType(rowType).getRowSchema().toByteArray()))
					.build())
			.build();
	}

	private FlinkFnApi.Schema.FieldType toProtoType(LogicalType logicalType) {
		return logicalType.accept(new PythonTypeUtils.LogicalTypeToProtoTypeConverter());
	}

	/**
	 * Returns the URN of the input/output coder.
	 */
	public abstract String getInputOutputCoderUrn();
}
