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
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.python.PythonConfig;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.PythonOptions;
import org.apache.flink.python.env.ProcessPythonEnvironment;
import org.apache.flink.python.env.PythonEnvironment;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.python.metric.FlinkMetricContainer;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.utils.ByteArrayWrapper;
import org.apache.flink.streaming.api.utils.ByteArrayWrapperSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.LongFunctionWithException;

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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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

	private static final String MANAGED_MEMORY_RESOURCE_ID = "python-process-managed-memory";
	private static final String PYTHON_WORKER_MEMORY_LIMIT = "_PYTHON_WORKER_MEMORY_LIMIT";

	private transient boolean bundleStarted;

	private final String taskName;

	/**
	 * The Python execution environment manager.
	 */
	private final PythonEnvironmentManager environmentManager;

	private final String functionUrn;

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

	@Nullable
	private final MemoryManager memoryManager;

	/**
	 * The fraction of total managed memory in the slot that the Python worker could use.
	 */
	private final double managedMemoryFraction;

	/**
	 * The shared resource among Python operators of the same slot.
	 */
	private OpaqueMemoryResource<PythonSharedResources> sharedResources;

	public BeamPythonFunctionRunner(
			String taskName,
			PythonEnvironmentManager environmentManager,
			String functionUrn,
			Map<String, String> jobOptions,
			FlinkMetricContainer flinkMetricContainer,
			@Nullable KeyedStateBackend keyedStateBackend,
			@Nullable TypeSerializer keySerializer,
			@Nullable MemoryManager memoryManager,
			double managedMemoryFraction) {
		this.taskName = Preconditions.checkNotNull(taskName);
		this.environmentManager = Preconditions.checkNotNull(environmentManager);
		this.functionUrn = Preconditions.checkNotNull(functionUrn);
		this.jobOptions = Preconditions.checkNotNull(jobOptions);
		this.flinkMetricContainer = flinkMetricContainer;
		this.stateRequestHandler = getStateRequestHandler(keyedStateBackend, keySerializer, jobOptions);
		this.memoryManager = memoryManager;
		this.managedMemoryFraction = managedMemoryFraction;
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

		if (jobOptions.containsKey(PythonOptions.STATE_CACHE_SIZE.key())) {
			portableOptions.as(ExperimentalOptions.class).setExperiments(
				Collections.singletonList(
					ExperimentalOptions.STATE_CACHE_SIZE + "=" +
						jobOptions.get(PythonOptions.STATE_CACHE_SIZE.key())));
		}

		Struct pipelineOptions = PipelineOptionsTranslation.toProto(portableOptions);

		if (memoryManager != null && config.isUsingManagedMemory()) {
			Preconditions.checkArgument(managedMemoryFraction > 0 && managedMemoryFraction <= 1.0,
				"The configured managed memory fraction for Python worker process must be within (0, 1], was: %s. " +
				"It may be because the consumer type \"Python\" was missing or set to 0 for the config option \"taskmanager.memory.managed.consumer-weights\"." +
				managedMemoryFraction);

			final LongFunctionWithException<PythonSharedResources, Exception> initializer = (size) ->
				new PythonSharedResources(createJobBundleFactory(pipelineOptions), createPythonExecutionEnvironment(size));

			sharedResources =
				memoryManager.getSharedMemoryResourceForManagedMemory(MANAGED_MEMORY_RESOURCE_ID, initializer, managedMemoryFraction);
			LOG.info("Obtained shared Python process of size {} bytes", sharedResources.getSize());
			sharedResources.getResourceHandle().addPythonEnvironmentManager(environmentManager);

			JobBundleFactory jobBundleFactory = sharedResources.getResourceHandle().getJobBundleFactory();
			RunnerApi.Environment environment = sharedResources.getResourceHandle().getEnvironment();
			stageBundleFactory = createStageBundleFactory(jobBundleFactory, environment);
		} else {
			// there is no way to access the MemoryManager for the batch job of old planner,
			// fallback to the way that spawning a Python process for each Python operator
			jobBundleFactory = createJobBundleFactory(pipelineOptions);
			stageBundleFactory = createStageBundleFactory(jobBundleFactory, createPythonExecutionEnvironment(-1));
		}
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

		try {
			if (sharedResources != null) {
				if (sharedResources.getResourceHandle().release()) {
					// release sharedResources iff there are no more Python operators sharing it
					sharedResources.close();
				}
			} else {
				// if sharedResources is not null, the close of environmentManager will be managed in sharedResources,
				// otherwise, we need to close the environmentManager explicitly
				environmentManager.close();
			}
		} finally {
			sharedResources = null;
		}
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
	private RunnerApi.Environment createPythonExecutionEnvironment(long memoryLimitBytes) throws Exception {
		PythonEnvironment environment = environmentManager.createEnvironment();
		if (environment instanceof ProcessPythonEnvironment) {
			ProcessPythonEnvironment processEnvironment = (ProcessPythonEnvironment) environment;
			Map<String, String> env = processEnvironment.getEnv();
			env.putAll(jobOptions);
			env.put(PYTHON_WORKER_MEMORY_LIMIT, String.valueOf(memoryLimitBytes));
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
	private StageBundleFactory createStageBundleFactory(
			JobBundleFactory jobBundleFactory, RunnerApi.Environment environment) throws Exception {
		try {
			return jobBundleFactory.forStage(createExecutableStage(environment));
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
	private ExecutableStage createExecutableStage(RunnerApi.Environment environment) throws Exception {
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
			components, environment, input, sideInputs, userStates, timers, transforms, outputs, createValueOnlyWireCoderSetting());
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
			TypeSerializer keySerializer,
			Map<String, String> jobOptions) {
		if (keyedStateBackend == null) {
			return StateRequestHandler.unsupported();
		} else {
			assert keySerializer != null;
			return new SimpleStateRequestHandler(keyedStateBackend, keySerializer, jobOptions);
		}
	}

	/**
	 * The type of the Python map state iterate request.
	 */
	private enum IterateType {

		/**
		 * Equivalent to iterate {@link Map#entrySet() }.
		 */
		ITEMS((byte) 0),

		/**
		 * Equivalent to iterate {@link Map#keySet() }.
		 */
		KEYS((byte) 1),

		/**
		 * Equivalent to iterate {@link Map#values() }.
		 */
		VALUES((byte) 2);

		private final byte ord;

		IterateType(byte ord) {
			this.ord = ord;
		}

		public byte getOrd() {
			return ord;
		}

		public static IterateType fromOrd(byte ord) {
			switch (ord) {
				case 0:
					return ITEMS;
				case 1:
					return KEYS;
				case 2:
					return VALUES;
				default:
					throw new RuntimeException("Unsupported ordinal: " + ord);
			}
		}
	}

	/**
	 * A state request handler which handles the state request from Python side.
	 */
	private static class SimpleStateRequestHandler implements StateRequestHandler {

		private static final String CLEAR_CACHED_ITERATOR_MARK = "clear_iterators";

		// map state GET request flags
		private static final byte GET_FLAG = 0;
		private static final byte ITERATE_FLAG = 1;
		private static final byte CHECK_EMPTY_FLAG = 2;

		// map state GET response flags
		private static final byte EXIST_FLAG = 0;
		private static final byte IS_NONE_FLAG = 1;
		private static final byte NOT_EXIST_FLAG = 2;
		private static final byte IS_EMPTY_FLAG = 3;
		private static final byte NOT_EMPTY_FLAG = 4;

		// map state APPEND request flags
		private static final byte DELETE = 0;
		private static final byte SET_NONE = 1;
		private static final byte SET_VALUE = 2;

		private static final BeamFnApi.StateGetResponse.Builder NOT_EXIST_RESPONSE =
			BeamFnApi.StateGetResponse.newBuilder().setData(ByteString.copyFrom(new byte[]{NOT_EXIST_FLAG}));
		private static final BeamFnApi.StateGetResponse.Builder IS_NONE_RESPONSE =
			BeamFnApi.StateGetResponse.newBuilder().setData(ByteString.copyFrom(new byte[]{IS_NONE_FLAG}));
		private static final BeamFnApi.StateGetResponse.Builder IS_EMPTY_RESPONSE =
			BeamFnApi.StateGetResponse.newBuilder().setData(ByteString.copyFrom(new byte[]{IS_EMPTY_FLAG}));
		private static final BeamFnApi.StateGetResponse.Builder NOT_EMPTY_RESPONSE =
			BeamFnApi.StateGetResponse.newBuilder().setData(ByteString.copyFrom(new byte[]{NOT_EMPTY_FLAG}));

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
		 * Reusable OutputStream used to holding the serialized input elements.
		 */
		private final ByteArrayOutputStreamWithPos baos;

		/**
		 * OutputStream Wrapper.
		 */
		private final DataOutputViewStreamWrapper baosWrapper;

		/**
		 * The cache of the stateDescriptors.
		 */
		private final Map<String, StateDescriptor> stateDescriptorCache;

		/**
		 * The cache of the map state iterators.
		 */
		private final Map<ByteArrayWrapper, Iterator> mapStateIteratorCache;

		private final int mapStateIterateResponseBatchSize;

		private final ByteArrayWrapper reuseByteArrayWrapper = new ByteArrayWrapper(new byte[0]);

		SimpleStateRequestHandler(
			KeyedStateBackend keyedStateBackend,
			TypeSerializer keySerializer,
			Map<String, String> config) {
			this.keyedStateBackend = keyedStateBackend;
			this.keySerializer = keySerializer;
			this.valueSerializer = PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO
				.createSerializer(new ExecutionConfig());
			bais = new ByteArrayInputStreamWithPos();
			baisWrapper = new DataInputViewStreamWrapper(bais);
			baos = new ByteArrayOutputStreamWithPos();
			baosWrapper = new DataOutputViewStreamWrapper(baos);
			stateDescriptorCache = new HashMap<>();
			mapStateIteratorCache = new HashMap<>();
			mapStateIterateResponseBatchSize = Integer.valueOf(config.getOrDefault(
				PythonOptions.MAP_STATE_ITERATE_RESPONSE_BATCH_SIZE.key(),
				PythonOptions.MAP_STATE_ITERATE_RESPONSE_BATCH_SIZE.defaultValue().toString()));
			if (mapStateIterateResponseBatchSize <= 0) {
				throw new RuntimeException(String.format(
					"The value of '%s' must be greater than 0!",
					PythonOptions.MAP_STATE_ITERATE_RESPONSE_BATCH_SIZE.key()));
			}
		}

		@Override
		public CompletionStage<BeamFnApi.StateResponse.Builder> handle(
				BeamFnApi.StateRequest request) throws Exception {
			BeamFnApi.StateKey.TypeCase typeCase = request.getStateKey().getTypeCase();
			synchronized (keyedStateBackend) {
				if (typeCase.equals(BeamFnApi.StateKey.TypeCase.BAG_USER_STATE)) {
					return handleBagState(request);
				} else if (typeCase.equals(BeamFnApi.StateKey.TypeCase.MULTIMAP_SIDE_INPUT)) {
					return handleMapState(request);
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
				if (keyedStateBackend.getKeySerializer() instanceof RowDataSerializer) {
					keyedStateBackend.setCurrentKey(
						((RowDataSerializer) keyedStateBackend.getKeySerializer()).toBinaryRow((RowData) key));
				} else {
					keyedStateBackend.setCurrentKey(key);
				}
			} else {
				throw new RuntimeException("Unsupported bag state request: " + request);
			}

			switch (request.getRequestCase()) {
				case GET:
					return handleBagGetRequest(request);
				case APPEND:
					return handleBagAppendRequest(request);
				case CLEAR:
					return handleBagClearRequest(request);
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

		private CompletionStage<BeamFnApi.StateResponse.Builder> handleBagGetRequest(
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

		private CompletionStage<BeamFnApi.StateResponse.Builder> handleBagAppendRequest(
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

		private CompletionStage<BeamFnApi.StateResponse.Builder> handleBagClearRequest(
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

		private CompletionStage<BeamFnApi.StateResponse.Builder> handleMapState(
			BeamFnApi.StateRequest request) throws Exception {
			// Currently the `beam_fn_api.proto` does not support MapState, so we use the
			// the `MultimapSideInput` message to mark the state as a MapState for now.
			if (request.getStateKey().hasMultimapSideInput()) {
				BeamFnApi.StateKey.MultimapSideInput mapUserState = request.getStateKey().getMultimapSideInput();
				// get key
				byte[] keyBytes = mapUserState.getKey().toByteArray();
				bais.setBuffer(keyBytes, 0, keyBytes.length);
				Object key = keySerializer.deserialize(baisWrapper);
				keyedStateBackend.setCurrentKey(
					((RowDataSerializer) keyedStateBackend.getKeySerializer()).toBinaryRow((RowData) key));
			} else {
				throw new RuntimeException("Unsupported bag state request: " + request);
			}

			switch (request.getRequestCase()) {
				case GET:
					return handleMapGetRequest(request);
				case APPEND:
					return handleMapAppendRequest(request);
				case CLEAR:
					return handleMapClearRequest(request);
				default:
					throw new RuntimeException(
						String.format(
							"Unsupported request type %s for user state.", request.getRequestCase()));
			}
		}

		private CompletionStage<BeamFnApi.StateResponse.Builder> handleMapGetRequest(BeamFnApi.StateRequest request)
				throws Exception {
			MapState<ByteArrayWrapper, byte[]> mapState = getMapState(request);
			// The continuation token structure of GET request is:
			// [flag (1 byte)][serialized map key]
			// The continuation token structure of CHECK_EMPTY request is:
			// [flag (1 byte)]
			// The continuation token structure of ITERATE request is:
			// [flag (1 byte)][iterate type (1 byte)][iterator token length (int32)][iterator token]
			byte[] getRequest = request.getGet().getContinuationToken().toByteArray();
			byte getFlag = getRequest[0];
			BeamFnApi.StateGetResponse.Builder response;
			switch (getFlag) {
				case GET_FLAG:
					reuseByteArrayWrapper.setData(getRequest);
					reuseByteArrayWrapper.setOffset(1);
					reuseByteArrayWrapper.setLimit(getRequest.length);
					response = handleMapGetValueRequest(reuseByteArrayWrapper, mapState);
					break;
				case CHECK_EMPTY_FLAG:
					response = handleMapCheckEmptyRequest(mapState);
					break;
				case ITERATE_FLAG:
					bais.setBuffer(getRequest, 1, getRequest.length - 1);
					IterateType iterateType = IterateType.fromOrd(baisWrapper.readByte());
					int iterateTokenLength = baisWrapper.readInt();
					ByteArrayWrapper iterateToken;
					if (iterateTokenLength > 0) {
						reuseByteArrayWrapper.setData(getRequest);
						reuseByteArrayWrapper.setOffset(bais.getPosition());
						reuseByteArrayWrapper.setLimit(bais.getPosition() + iterateTokenLength);
						iterateToken = reuseByteArrayWrapper;
					} else {
						iterateToken = null;
					}
					response = handleMapIterateRequest(mapState, iterateType, iterateToken);
					break;
				default:
					throw new RuntimeException(String.format(
						"Unsupported get request type: '%d' for map state.", getFlag));
			}

			return CompletableFuture.completedFuture(
				BeamFnApi.StateResponse.newBuilder()
					.setId(request.getId())
					.setGet(response));
		}

		private CompletionStage<BeamFnApi.StateResponse.Builder> handleMapAppendRequest(BeamFnApi.StateRequest request)
				throws Exception {
			// The structure of append request bytes is:
			// [number of requests (int32)][append request flag (1 byte)][map key length (int32)][map key]
			// [map value length (int32)][map value][append request flag (1 byte)][map key length (int32)][map key]
			// ...
			byte[] appendBytes = request.getAppend().getData().toByteArray();
			bais.setBuffer(appendBytes, 0, appendBytes.length);
			MapState<ByteArrayWrapper, byte[]> mapState = getMapState(request);
			int subRequestNum = baisWrapper.readInt();
			for (int i = 0; i < subRequestNum; i++) {
				byte requestFlag = baisWrapper.readByte();
				int keyLength = baisWrapper.readInt();
				reuseByteArrayWrapper.setData(appendBytes);
				reuseByteArrayWrapper.setOffset(bais.getPosition());
				reuseByteArrayWrapper.setLimit(bais.getPosition() + keyLength);
				baisWrapper.skipBytesToRead(keyLength);
				switch (requestFlag) {
					case DELETE:
						mapState.remove(reuseByteArrayWrapper);
						break;
					case SET_NONE:
						mapState.put(reuseByteArrayWrapper.copy(), null);
						break;
					case SET_VALUE:
						int valueLength = baisWrapper.readInt();
						byte[] valueBytes = new byte[valueLength];
						int readLength = baisWrapper.read(valueBytes);
						assert valueLength == readLength;
						mapState.put(reuseByteArrayWrapper.copy(), valueBytes);
						break;
					default:
						throw new RuntimeException(String.format(
							"Unsupported append request type: '%d' for map state.", requestFlag));
				}
			}
			return CompletableFuture.completedFuture(
				BeamFnApi.StateResponse.newBuilder()
					.setId(request.getId())
					.setAppend(BeamFnApi.StateAppendResponse.getDefaultInstance()));
		}

		private CompletionStage<BeamFnApi.StateResponse.Builder> handleMapClearRequest(BeamFnApi.StateRequest request)
			throws Exception {
			if (request.getStateKey().getMultimapSideInput().getTransformId().equals(CLEAR_CACHED_ITERATOR_MARK)) {
				mapStateIteratorCache.clear();
			} else {
				MapState<ByteArrayWrapper, byte[]> partitionedState = getMapState(request);
				partitionedState.clear();
			}
			return CompletableFuture.completedFuture(
				BeamFnApi.StateResponse.newBuilder()
					.setId(request.getId())
					.setClear(BeamFnApi.StateClearResponse.getDefaultInstance()));
		}

		private BeamFnApi.StateGetResponse.Builder handleMapGetValueRequest(
				ByteArrayWrapper key, MapState<ByteArrayWrapper, byte[]> mapState) throws Exception {
			if (mapState.contains(key)) {
				byte[] value = mapState.get(key);
				if (value == null) {
					return IS_NONE_RESPONSE;
				} else {
					baos.reset();
					baosWrapper.writeByte(EXIST_FLAG);
					baosWrapper.write(value);
					return BeamFnApi.StateGetResponse.newBuilder().setData(
						ByteString.copyFrom(baos.toByteArray()));
				}
			} else {
				return NOT_EXIST_RESPONSE;
			}
		}

		private BeamFnApi.StateGetResponse.Builder handleMapCheckEmptyRequest(
				MapState<ByteArrayWrapper, byte[]> mapState) throws Exception {
			if (mapState.isEmpty()) {
				return IS_EMPTY_RESPONSE;
			} else {
				return NOT_EMPTY_RESPONSE;
			}
		}

		private BeamFnApi.StateGetResponse.Builder handleMapIterateRequest(
				MapState<ByteArrayWrapper, byte[]> mapState,
				IterateType iterateType,
				ByteArrayWrapper iteratorToken) throws Exception {
			final Iterator iterator;
			if (iteratorToken == null) {
				switch (iterateType) {
					case ITEMS:
					case VALUES:
						iterator = mapState.iterator();
						break;
					case KEYS:
						iterator = mapState.keys().iterator();
						break;
					default:
						throw new RuntimeException("Unsupported iterate type: " + iterateType);
				}
			} else {
				iterator = mapStateIteratorCache.get(iteratorToken);
				if (iterator == null) {
					throw new RuntimeException("The cached iterator does not exist!");
				}
			}
			baos.reset();
			switch (iterateType) {
				case ITEMS:
				case VALUES:
					Iterator<Map.Entry<ByteArrayWrapper, byte[]>> entryIterator = iterator;
					for (int i = 0; i < mapStateIterateResponseBatchSize; i++) {
						if (entryIterator.hasNext()) {
							Map.Entry<ByteArrayWrapper, byte[]> entry = entryIterator.next();
							ByteArrayWrapper key = entry.getKey();
							baosWrapper.write(key.getData(), key.getOffset(), key.getLimit() - key.getOffset());
							baosWrapper.writeBoolean(entry.getValue() != null);
							if (entry.getValue() != null) {
								baosWrapper.write(entry.getValue());
							}
						} else {
							break;
						}
					}
					break;
				case KEYS:
					Iterator<ByteArrayWrapper> keyIterator = iterator;
					for (int i = 0; i < mapStateIterateResponseBatchSize; i++) {
						if (keyIterator.hasNext()) {
							ByteArrayWrapper key = keyIterator.next();
							baosWrapper.write(key.getData(), key.getOffset(), key.getLimit() - key.getOffset());
						} else {
							break;
						}
					}
					break;
				default:
					throw new RuntimeException("Unsupported iterate type: " + iterateType);
			}
			if (!iterator.hasNext()) {
				if (iteratorToken != null) {
					mapStateIteratorCache.remove(iteratorToken);
				}
				iteratorToken = null;
			} else {
				if (iteratorToken == null) {
					iteratorToken = new ByteArrayWrapper(UUID.randomUUID().toString().getBytes());
				}
				mapStateIteratorCache.put(iteratorToken, iterator);
			}
			BeamFnApi.StateGetResponse.Builder responseBuilder =
				BeamFnApi.StateGetResponse.newBuilder().setData(ByteString.copyFrom(baos.toByteArray()));
			if (iteratorToken != null) {
				responseBuilder.setContinuationToken(ByteString.copyFrom(
					iteratorToken.getData(),
					iteratorToken.getOffset(),
					iteratorToken.getLimit() - iteratorToken.getOffset()));
			}
			return responseBuilder;
		}

		private MapState<ByteArrayWrapper, byte[]> getMapState(BeamFnApi.StateRequest request) throws Exception {
			BeamFnApi.StateKey.MultimapSideInput mapUserState = request.getStateKey().getMultimapSideInput();
			String stateName = PYTHON_STATE_PREFIX + mapUserState.getSideInputId();
			StateDescriptor cachedStateDescriptor = stateDescriptorCache.get(stateName);
			MapStateDescriptor<ByteArrayWrapper, byte[]> mapStateDescriptor;
			if (cachedStateDescriptor instanceof MapStateDescriptor) {
				mapStateDescriptor = (MapStateDescriptor<ByteArrayWrapper, byte[]>) cachedStateDescriptor;
			} else if (cachedStateDescriptor == null) {
				mapStateDescriptor = new MapStateDescriptor<>(
					stateName, ByteArrayWrapperSerializer.INSTANCE, valueSerializer);
				stateDescriptorCache.put(stateName, mapStateDescriptor);
			} else {
				throw new RuntimeException(
					String.format(
						"State name corrupt detected: " +
							"'%s' is used both as MAP state and '%s' state at the same time.",
						stateName,
						cachedStateDescriptor.getType()));
			}

			return (MapState<ByteArrayWrapper, byte[]>) keyedStateBackend.getPartitionedState(
				VoidNamespace.INSTANCE,
				VoidNamespaceSerializer.INSTANCE,
				mapStateDescriptor);
		}
	}
}
