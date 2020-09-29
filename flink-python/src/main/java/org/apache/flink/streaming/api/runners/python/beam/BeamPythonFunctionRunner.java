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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.python.PythonConfig;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.PythonOptions;
import org.apache.flink.python.env.ProcessPythonEnvironment;
import org.apache.flink.python.env.PythonEnvironment;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.python.metric.FlinkMetricContainer;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.util.Preconditions;

import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.ModelCoders;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.ImmutableExecutableStage;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.SideInputReference;
import org.apache.beam.runners.core.construction.graph.TimerReference;
import org.apache.beam.runners.core.construction.graph.UserStateReference;
import org.apache.beam.runners.fnexecution.control.BundleProgressHandler;
import org.apache.beam.runners.fnexecution.control.DefaultJobBundleFactory;
import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.runners.fnexecution.control.OutputReceiverFactory;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.Struct;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.beam.runners.core.construction.BeamUrns.getUrn;

/**
 * A {@link BeamPythonFunctionRunner} used to execute Python functions.
 */
@Internal
public abstract class BeamPythonFunctionRunner implements PythonFunctionRunner {
	protected static final Logger LOG = LoggerFactory.getLogger(BeamPythonFunctionRunner.class);

	private static final String PYTHON_STATE_PREFIX = "python-state-";

	private static final String INPUT_ID = "input";
	private static final String OUTPUT_ID = "output";
	private static final String TRANSFORM_ID = "transform";

	private static final String MAIN_INPUT_NAME = "input";
	private static final String MAIN_OUTPUT_NAME = "output";

	private static final String INPUT_CODER_ID = "input_coder";
	private static final String OUTPUT_CODER_ID = "output_coder";
	private static final String WINDOW_CODER_ID = "window_coder";

	private static final String WINDOW_STRATEGY = "windowing_strategy";

	private transient boolean bundleStarted;

	private final String taskName;

	/**
	 * The Python execution environment manager.
	 */
	private final PythonEnvironmentManager environmentManager;

	/**
	 * The options used to configure the Python worker process.
	 */
	private final Map<String, String> jobOptions;

	/**
	 * The Python function execution result tuple.
	 */
	protected final Tuple2<byte[], Integer> resultTuple;

	/**
	 * The bundle factory which has all job-scoped information and can be used to create a {@link StageBundleFactory}.
	 */
	private transient JobBundleFactory jobBundleFactory;

	/**
	 * The bundle factory which has all of the resources it needs to provide new {@link RemoteBundle}.
	 */
	private transient StageBundleFactory stageBundleFactory;

	/**
	 * Handler for state requests.
	 */
	private final StateRequestHandler stateRequestHandler;

	/**
	 * Handler for bundle progress messages, both during bundle execution and on its completion.
	 */
	private transient BundleProgressHandler progressHandler;

	/**
	 * A bundle handler for handling input elements by forwarding them to a remote environment for processing.
	 * It holds a collection of {@link FnDataReceiver}s which actually perform the data forwarding work.
	 *
	 * <p>When a RemoteBundle is closed, it will block until bundle processing is finished on remote resources,
	 * and throw an exception if bundle processing has failed.
	 */
	private transient RemoteBundle remoteBundle;

	/**
	 * The Python function execution result receiver.
	 */
	protected transient LinkedBlockingQueue<byte[]> resultBuffer;

	/**
	 * The receiver which forwards the input elements to a remote environment for processing.
	 */
	protected transient FnDataReceiver<WindowedValue<byte[]>> mainInputReceiver;

	/**
	 * The flinkMetricContainer will be set to null if metric is configured to be turned off.
	 */
	@Nullable
	private FlinkMetricContainer flinkMetricContainer;

	private final String functionUrn;

	public BeamPythonFunctionRunner(
			String taskName,
			PythonEnvironmentManager environmentManager,
			String functionUrn,
			Map<String, String> jobOptions,
			FlinkMetricContainer flinkMetricContainer,
			@Nullable KeyedStateBackend keyedStateBackend,
			@Nullable TypeSerializer keySerializer) {
		this.taskName = Preconditions.checkNotNull(taskName);
		this.environmentManager = Preconditions.checkNotNull(environmentManager);
		this.functionUrn = functionUrn;
		this.jobOptions = Preconditions.checkNotNull(jobOptions);
		this.flinkMetricContainer = flinkMetricContainer;
		this.stateRequestHandler = getStateRequestHandler(keyedStateBackend, keySerializer);
		this.resultTuple = new Tuple2<>();
	}

	@Override
	public void open(PythonConfig config) throws Exception {
		this.bundleStarted = false;
		this.resultBuffer = new LinkedBlockingQueue<>();

		// The creation of stageBundleFactory depends on the initialized environment manager.
		environmentManager.open();

		PortablePipelineOptions portableOptions =
			PipelineOptionsFactory.as(PortablePipelineOptions.class);
		// one operator has one Python SDK harness
		portableOptions.setSdkWorkerParallelism(1);

		if (jobOptions.containsKey(PythonOptions.STATE_CACHE_SIZE.key())) {
			portableOptions.as(ExperimentalOptions.class).setExperiments(
				Collections.singletonList(
					ExperimentalOptions.STATE_CACHE_SIZE + "=" +
						jobOptions.get(PythonOptions.STATE_CACHE_SIZE.key())));
		}

		Struct pipelineOptions = PipelineOptionsTranslation.toProto(portableOptions);

		jobBundleFactory = createJobBundleFactory(pipelineOptions);
		stageBundleFactory = createStageBundleFactory();
		progressHandler = getProgressHandler(flinkMetricContainer);
	}

	@Override
	public void close() throws Exception {
		try {
			if (jobBundleFactory != null) {
				jobBundleFactory.close();
			}
		} finally {
			jobBundleFactory = null;
		}

		environmentManager.close();
	}

	@Override
	public void process(byte[] data) throws Exception {
		checkInvokeStartBundle();
		mainInputReceiver.accept(WindowedValue.valueInGlobalWindow(data));
	}

	@Override
	public Tuple2<byte[], Integer> pollResult() throws Exception {
		byte[] result = resultBuffer.poll();
		if (result == null) {
			return null;
		} else {
			this.resultTuple.f0 = result;
			this.resultTuple.f1 = result.length;
			return this.resultTuple;
		}
	}

	@Override
	public void flush() throws Exception {
		if (bundleStarted) {
			finishBundle();
			bundleStarted = false;
		}
	}

	public JobBundleFactory createJobBundleFactory(Struct pipelineOptions) throws Exception {
		return DefaultJobBundleFactory.create(
			JobInfo.create(taskName, taskName, environmentManager.createRetrievalToken(), pipelineOptions));
	}

	/**
	 * Creates a specification which specifies the portability Python execution environment.
	 * It's used by Beam's portability framework to creates the actual Python execution environment.
	 */
	protected RunnerApi.Environment createPythonExecutionEnvironment() throws Exception {
		PythonEnvironment environment = environmentManager.createEnvironment();
		if (environment instanceof ProcessPythonEnvironment) {
			ProcessPythonEnvironment processEnvironment = (ProcessPythonEnvironment) environment;
			Map<String, String> env = processEnvironment.getEnv();
			env.putAll(jobOptions);
			return Environments.createProcessEnvironment(
				"",
				"",
				processEnvironment.getCommand(),
				env);
		}
		throw new RuntimeException("Currently only ProcessPythonEnvironment is supported.");
	}

	protected void startBundle() {
		try {
			remoteBundle = stageBundleFactory.getBundle(createOutputReceiverFactory(), stateRequestHandler, progressHandler);
			mainInputReceiver =
				Preconditions.checkNotNull(
					Iterables.getOnlyElement(remoteBundle.getInputReceivers().values()),
					"Failed to retrieve main input receiver.");
		} catch (Throwable t) {
			throw new RuntimeException("Failed to start remote bundle", t);
		}
	}

	private void finishBundle() {
		try {
			remoteBundle.close();
		} catch (Throwable t) {
			throw new RuntimeException("Failed to close remote bundle", t);
		} finally {
			remoteBundle = null;
		}
	}

	private OutputReceiverFactory createOutputReceiverFactory() {
		return new OutputReceiverFactory() {

			// the input value type is always byte array
			@SuppressWarnings("unchecked")
			@Override
			public FnDataReceiver<WindowedValue<byte[]>> create(String pCollectionId) {
				return input -> resultBuffer.add(input.getValue());
			}
		};
	}

	/**
	 * Ignore bundle progress if flinkMetricContainer is null. The flinkMetricContainer will be set
	 * to null if metric is configured to be turned off.
	 */
	private BundleProgressHandler getProgressHandler(FlinkMetricContainer flinkMetricContainer) {
		if (flinkMetricContainer == null) {
			return BundleProgressHandler.ignored();
		} else {
			return new BundleProgressHandler() {
				@Override
				public void onProgress(BeamFnApi.ProcessBundleProgressResponse progress) {
					flinkMetricContainer.updateMetrics(taskName, progress.getMonitoringInfosList());
				}

				@Override
				public void onCompleted(BeamFnApi.ProcessBundleResponse response) {
					flinkMetricContainer.updateMetrics(taskName, response.getMonitoringInfosList());
				}
			};
		}
	}

	/**
	 * To make the error messages more user friendly, throws an exception with the boot logs.
	 */
	private StageBundleFactory createStageBundleFactory() throws Exception {
		try {
			return jobBundleFactory.forStage(createExecutableStage());
		} catch (Throwable e) {
			throw new RuntimeException(environmentManager.getBootLog(), e);
		}
	}

	/**
	 * Checks whether to invoke startBundle.
	 */
	private void checkInvokeStartBundle() throws Exception {
		if (!bundleStarted) {
			startBundle();
			bundleStarted = true;
		}
	}

	/**
	 * Creates a {@link ExecutableStage} which contains the Python user-defined functions to be executed
	 * and all the other information needed to execute them, such as the execution environment, the input
	 * and output coder, etc.
	 */
	@SuppressWarnings("unchecked")
	private ExecutableStage createExecutableStage() throws Exception {
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
								org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString.copyFrom(
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

	private Collection<RunnerApi.ExecutableStagePayload.WireCoderSetting> createValueOnlyWireCoderSetting() throws
		IOException {
		WindowedValue<byte[]> value = WindowedValue.valueInGlobalWindow(new byte[0]);
		Coder<? extends BoundedWindow> windowCoder = GlobalWindow.Coder.INSTANCE;
		WindowedValue.FullWindowedValueCoder<byte[]> windowedValueCoder =
			WindowedValue.FullWindowedValueCoder.of(ByteArrayCoder.of(), windowCoder);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		windowedValueCoder.encode(value, baos);

		return Arrays.asList(
			RunnerApi.ExecutableStagePayload.WireCoderSetting.newBuilder()
				.setUrn(getUrn(RunnerApi.StandardCoders.Enum.PARAM_WINDOWED_VALUE))
				.setPayload(
					org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString.copyFrom(baos.toByteArray()))
				.setInputOrOutputId(INPUT_ID)
				.build(),
			RunnerApi.ExecutableStagePayload.WireCoderSetting.newBuilder()
				.setUrn(getUrn(RunnerApi.StandardCoders.Enum.PARAM_WINDOWED_VALUE))
				.setPayload(
					org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString.copyFrom(baos.toByteArray()))
				.setInputOrOutputId(OUTPUT_ID)
				.build()
		);
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

	private static StateRequestHandler getStateRequestHandler(
			KeyedStateBackend keyedStateBackend,
			TypeSerializer keySerializer) {
		if (keyedStateBackend == null) {
			return StateRequestHandler.unsupported();
		} else {
			assert keySerializer != null;
			return new SimpleStateRequestHandler(keyedStateBackend, keySerializer);
		}
	}

	/**
	 * A state request handler which handles the state request from Python side.
	 */
	private static class SimpleStateRequestHandler implements StateRequestHandler {

		private final TypeSerializer keySerializer;
		private final TypeSerializer<byte[]> valueSerializer;
		private final KeyedStateBackend keyedStateBackend;

		/**
		 * Reusable InputStream used to holding the elements to be deserialized.
		 */
		private final ByteArrayInputStreamWithPos bais;

		/**
		 * InputStream Wrapper.
		 */
		private final DataInputViewStreamWrapper baisWrapper;

		/**
		 * The cache of the stateDescriptors.
		 */
		final Map<String, StateDescriptor> stateDescriptorCache;

		SimpleStateRequestHandler(
			KeyedStateBackend keyedStateBackend,
			TypeSerializer keySerializer) {
			this.keyedStateBackend = keyedStateBackend;
			this.keySerializer = keySerializer;
			this.valueSerializer = PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO
				.createSerializer(new ExecutionConfig());
			bais = new ByteArrayInputStreamWithPos();
			baisWrapper = new DataInputViewStreamWrapper(bais);
			stateDescriptorCache = new HashMap<>();
		}

		@Override
		public CompletionStage<BeamFnApi.StateResponse.Builder> handle(
				BeamFnApi.StateRequest request) throws Exception {
			BeamFnApi.StateKey.TypeCase typeCase = request.getStateKey().getTypeCase();
			synchronized (keyedStateBackend) {
				if (typeCase.equals(BeamFnApi.StateKey.TypeCase.BAG_USER_STATE)) {
					return handleBagState(request);
				} else {
					throw new RuntimeException("Unsupported state type: " + typeCase);
				}
			}
		}

		private CompletionStage<BeamFnApi.StateResponse.Builder> handleBagState(
				BeamFnApi.StateRequest request) throws Exception {
			if (request.getStateKey().hasBagUserState()) {
				BeamFnApi.StateKey.BagUserState bagUserState = request.getStateKey().getBagUserState();
				// get key
				byte[] keyBytes = bagUserState.getKey().toByteArray();
				bais.setBuffer(keyBytes, 0, keyBytes.length);
				Object key = keySerializer.deserialize(baisWrapper);
				keyedStateBackend.setCurrentKey(
					((RowDataSerializer) keyedStateBackend.getKeySerializer()).toBinaryRow((RowData) key));
			} else {
				throw new RuntimeException("Unsupported bag state request: " + request);
			}

			switch (request.getRequestCase()) {
				case GET:
					return handleGetRequest(request);
				case APPEND:
					return handleAppendRequest(request);
				case CLEAR:
					return handleClearRequest(request);
				default:
					throw new RuntimeException(
						String.format(
							"Unsupported request type %s for user state.", request.getRequestCase()));
			}
		}

		private List<ByteString> convertToByteString(ListState<byte[]> listState) throws Exception {
			List<ByteString> ret = new LinkedList<>();
			if (listState.get() == null) {
				return ret;
			}
			for (byte[] v: listState.get()) {
				ret.add(ByteString.copyFrom(v));
			}
			return ret;
		}

		private CompletionStage<BeamFnApi.StateResponse.Builder> handleGetRequest(
			BeamFnApi.StateRequest request) throws Exception {

			ListState<byte[]> partitionedState = getListState(request);
			List<ByteString> byteStrings = convertToByteString(partitionedState);

			return CompletableFuture.completedFuture(
				BeamFnApi.StateResponse.newBuilder()
					.setId(request.getId())
					.setGet(
						BeamFnApi.StateGetResponse.newBuilder()
							.setData(ByteString.copyFrom(byteStrings))));
		}

		private CompletionStage<BeamFnApi.StateResponse.Builder> handleAppendRequest(
			BeamFnApi.StateRequest request) throws Exception {

			ListState<byte[]> partitionedState = getListState(request);
			// get values
			byte[] valueBytes = request.getAppend().getData().toByteArray();
			partitionedState.add(valueBytes);

			return CompletableFuture.completedFuture(
				BeamFnApi.StateResponse.newBuilder()
					.setId(request.getId())
					.setAppend(BeamFnApi.StateAppendResponse.getDefaultInstance()));
		}

		private CompletionStage<BeamFnApi.StateResponse.Builder> handleClearRequest(
			BeamFnApi.StateRequest request) throws Exception {

			ListState<byte[]> partitionedState = getListState(request);

			partitionedState.clear();
			return CompletableFuture.completedFuture(
				BeamFnApi.StateResponse.newBuilder()
					.setId(request.getId())
					.setClear(BeamFnApi.StateClearResponse.getDefaultInstance()));
		}

		private ListState<byte[]> getListState(BeamFnApi.StateRequest request) throws Exception {
			BeamFnApi.StateKey.BagUserState bagUserState = request.getStateKey().getBagUserState();
			String stateName = PYTHON_STATE_PREFIX + bagUserState.getUserStateId();
			ListStateDescriptor<byte[]> listStateDescriptor;
			StateDescriptor cachedStateDescriptor = stateDescriptorCache.get(stateName);
			if (cachedStateDescriptor instanceof ListStateDescriptor) {
				listStateDescriptor = (ListStateDescriptor<byte[]>) cachedStateDescriptor;
			} else if (cachedStateDescriptor == null) {
				listStateDescriptor = new ListStateDescriptor<>(stateName, valueSerializer);
				stateDescriptorCache.put(stateName, listStateDescriptor);
			} else {
				throw new RuntimeException(
					String.format(
						"State name corrupt detected: " +
						"'%s' is used both as LIST state and '%s' state at the same time.",
						stateName,
						cachedStateDescriptor.getType()));
			}

			return (ListState<byte[]>) keyedStateBackend.getPartitionedState(
				VoidNamespace.INSTANCE,
				VoidNamespaceSerializer.INSTANCE,
				listStateDescriptor);
		}
	}
}
