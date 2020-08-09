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

package org.apache.flink.datastream.runtime.runners.python.beam;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.datastream.runtime.typeutils.python.PythonTypeUtils;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.python.metric.FlinkMetricContainer;
import org.apache.flink.streaming.api.runners.python.beam.BeamPythonStatelessFunctionRunner;

import org.apache.beam.model.pipeline.v1.RunnerApi;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * {@link BeamDataStreamPythonStatelessFunctionRunner} is responsible for starting a beam python harness to execute user
 * defined python function.
 */
public class BeamDataStreamPythonStatelessFunctionRunner extends BeamPythonStatelessFunctionRunner {
	private static final long serialVersionUID = 1L;

	private final TypeInformation inputType;
	private final TypeInformation outputTupe;
	private final FlinkFnApi.UserDefinedDataStreamFunctions userDefinedDataStreamFunctions;
	private final String coderUrn;

	public BeamDataStreamPythonStatelessFunctionRunner(
		String taskName,
		PythonEnvironmentManager environmentManager,
		TypeInformation inputType,
		TypeInformation outputType,
		String functionUrn,
		FlinkFnApi.UserDefinedDataStreamFunctions userDefinedDataStreamFunctions,
		String coderUrn,
		Map<String, String> jobOptions,
		@Nullable FlinkMetricContainer flinkMetricContainer) {
		super(taskName, environmentManager, functionUrn, jobOptions, flinkMetricContainer);
		this.inputType = inputType;
		this.outputTupe = outputType;
		this.userDefinedDataStreamFunctions = userDefinedDataStreamFunctions;
		this.coderUrn = coderUrn;
	}

	@Override
	protected byte[] getUserDefinedFunctionsProtoBytes() {
		return this.userDefinedDataStreamFunctions.toByteArray();
	}

	@Override
	protected RunnerApi.Coder getInputCoderProto() {
		return getInputOutputCoderProto(inputType);
	}

	@Override
	protected RunnerApi.Coder getOutputCoderProto() {
		return getInputOutputCoderProto(outputTupe);
	}

	private RunnerApi.Coder getInputOutputCoderProto(TypeInformation typeInformation) {
		FlinkFnApi.TypeInfo.FieldType builtFieldType = PythonTypeUtils.TypeInfoToProtoConverter
			.getFieldType(typeInformation);
		return RunnerApi.Coder.newBuilder()
			.setSpec(
		RunnerApi.FunctionSpec.newBuilder()
					.setUrn(this.coderUrn)
					.setPayload(org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.ByteString.copyFrom(
		PythonTypeUtils.TypeInfoToProtoConverter.toTypeInfoProto(builtFieldType).toByteArray()
					)).build()
			).build();
	}
}
