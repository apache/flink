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

package org.apache.flink.streaming.api.runners.python.beam;

import org.apache.flink.annotation.Internal;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.python.metric.FlinkMetricContainer;
import org.apache.flink.util.Preconditions;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.ModelCoders;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.ImmutableExecutableStage;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.SideInputReference;
import org.apache.beam.runners.core.construction.graph.TimerReference;
import org.apache.beam.runners.core.construction.graph.UserStateReference;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.beam.runners.core.construction.BeamUrns.getUrn;

/**
 * A {@link BeamPythonFunctionRunner} used to execute Python stateless functions.
 */
@Internal
public abstract class BeamPythonStatelessFunctionRunner extends BeamPythonFunctionRunner {
	private static final String INPUT_ID = "input";
	private static final String OUTPUT_ID = "output";
	private static final String TRANSFORM_ID = "transform";

	private static final String MAIN_INPUT_NAME = "input";
	private static final String MAIN_OUTPUT_NAME = "output";

	private static final String INPUT_CODER_ID = "input_coder";
	private static final String OUTPUT_CODER_ID = "output_coder";
	private static final String WINDOW_CODER_ID = "window_coder";

	private static final String WINDOW_STRATEGY = "windowing_strategy";

	private final String functionUrn;

	public BeamPythonStatelessFunctionRunner(
		String taskName,
		PythonEnvironmentManager environmentManager,
		String functionUrn,
		Map<String, String> jobOptions,
		FlinkMetricContainer flinkMetricContainer) {
		super(taskName, environmentManager, StateRequestHandler.unsupported(), jobOptions, flinkMetricContainer);
		this.functionUrn = Preconditions.checkNotNull(functionUrn);
	}

	@Override
	@SuppressWarnings("unchecked")
	public ExecutableStage createExecutableStage() throws Exception {
		RunnerApi.Components components =
			RunnerApi.Components.newBuilder()
				.putPcollections(
					INPUT_ID,
					RunnerApi.PCollection.newBuilder()
						.setWindowingStrategyId(WINDOW_STRATEGY)
						.setCoderId(INPUT_CODER_ID)
						.build())
				.putPcollections(
					OUTPUT_ID,
					RunnerApi.PCollection.newBuilder()
						.setWindowingStrategyId(WINDOW_STRATEGY)
						.setCoderId(OUTPUT_CODER_ID)
						.build())
				.putTransforms(
					TRANSFORM_ID,
					RunnerApi.PTransform.newBuilder()
						.setUniqueName(TRANSFORM_ID)
						.setSpec(RunnerApi.FunctionSpec.newBuilder()
							.setUrn(functionUrn)
							.setPayload(
								org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.ByteString.copyFrom(
									getUserDefinedFunctionsProtoBytes()))
							.build())
						.putInputs(MAIN_INPUT_NAME, INPUT_ID)
						.putOutputs(MAIN_OUTPUT_NAME, OUTPUT_ID)
						.build())
				.putWindowingStrategies(
					WINDOW_STRATEGY,
					RunnerApi.WindowingStrategy.newBuilder()
						.setWindowCoderId(WINDOW_CODER_ID)
						.build())
				.putCoders(
					INPUT_CODER_ID,
					getInputCoderProto())
				.putCoders(
					OUTPUT_CODER_ID,
					getOutputCoderProto())
				.putCoders(
					WINDOW_CODER_ID,
					getWindowCoderProto())
				.build();

		PipelineNode.PCollectionNode input =
			PipelineNode.pCollection(INPUT_ID, components.getPcollectionsOrThrow(INPUT_ID));
		List<SideInputReference> sideInputs = Collections.EMPTY_LIST;
		List<UserStateReference> userStates = Collections.EMPTY_LIST;
		List<TimerReference> timers = Collections.EMPTY_LIST;
		List<PipelineNode.PTransformNode> transforms =
			Collections.singletonList(
				PipelineNode.pTransform(TRANSFORM_ID, components.getTransformsOrThrow(TRANSFORM_ID)));
		List<PipelineNode.PCollectionNode> outputs =
			Collections.singletonList(
				PipelineNode.pCollection(OUTPUT_ID, components.getPcollectionsOrThrow(OUTPUT_ID)));
		return ImmutableExecutableStage.of(
			components, createPythonExecutionEnvironment(), input, sideInputs, userStates, timers, transforms, outputs, createValueOnlyWireCoderSetting());
	}

	private RunnerApi.WireCoderSetting createValueOnlyWireCoderSetting() throws IOException {
		WindowedValue<byte[]> value = WindowedValue.valueInGlobalWindow(new byte[0]);
		Coder<? extends BoundedWindow> windowCoder = GlobalWindow.Coder.INSTANCE;
		WindowedValue.FullWindowedValueCoder<byte[]> windowedValueCoder =
			WindowedValue.FullWindowedValueCoder.of(ByteArrayCoder.of(), windowCoder);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		windowedValueCoder.encode(value, baos);
		return RunnerApi.WireCoderSetting.newBuilder()
			.setUrn(getUrn(RunnerApi.StandardCoders.Enum.PARAM_WINDOWED_VALUE))
			.setPayload(
				org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.ByteString.copyFrom(baos.toByteArray()))
			.build();
	}

	protected abstract byte[] getUserDefinedFunctionsProtoBytes();

	protected abstract RunnerApi.Coder getInputCoderProto();

	protected abstract RunnerApi.Coder getOutputCoderProto();

	/**
	 * Gets the proto representation of the window coder.
	 */
	private RunnerApi.Coder getWindowCoderProto() {
		return RunnerApi.Coder.newBuilder()
			.setSpec(
				RunnerApi.FunctionSpec.newBuilder()
					.setUrn(ModelCoders.GLOBAL_WINDOW_CODER_URN)
					.build())
			.build();
	}
}
