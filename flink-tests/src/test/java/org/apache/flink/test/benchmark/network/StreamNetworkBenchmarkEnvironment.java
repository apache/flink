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

package org.apache.flink.test.benchmark.network;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.InputChannelDeploymentDescriptor;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionLocation;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManager.IOMode;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.api.writer.ChannelSelector;
import org.apache.flink.runtime.io.network.api.writer.RoundRobinChannelSelector;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.io.network.netty.NettyConnectionManager;
import org.apache.flink.runtime.io.network.partition.BlockingShuffleType;
import org.apache.flink.runtime.io.network.partition.InternalResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.PartitionRequestManager;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.external.ExternalResultPartition;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.taskmanager.TaskActions;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.streaming.runtime.io.StreamRecordWriter;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.util.ExceptionUtils.suppressExceptions;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;
import static org.mockito.Mockito.mock;

/**
 * Context for network benchmarks executed by the external
 * <a href="https://github.com/dataArtisans/flink-benchmarks">flink-benchmarks</a> project.
 */
public class StreamNetworkBenchmarkEnvironment<T extends IOReadableWritable> {

	private static final int NUM_SLOTS_AND_THREADS = 1;

	private static final InetAddress LOCAL_ADDRESS;

	static {
		try {
			LOCAL_ADDRESS = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			throw new Error(e);
		}
	}

	private int bufferSize;

	protected final JobID jobId = new JobID();
	protected final IntermediateDataSetID dataSetID = new IntermediateDataSetID();
	protected final ExecutionAttemptID executionAttemptID = new ExecutionAttemptID();

	protected NetworkEnvironment senderEnv;
	protected NetworkEnvironment receiverEnv;
	protected IOManager ioManager;
	protected MemoryManager memoryManager;

	protected int channels;
	protected boolean broadcastMode = false;
	protected boolean localMode = false;

	protected ResultPartitionID[] partitionIds;

	private final ExecutorService executor = Executors.newFixedThreadPool(1);

	protected final AbstractInvokable parentTask = new MockInvokable();

	private Configuration configuration;

	/**
	 * Sets up the environment including buffer pools and netty threads.
	 *
	 * @param writers
	 * 		number of writers
	 * @param channels
	 * 		outgoing channels per writer
	 * @param broadcastMode
	 * 		writers broadcast records to all channels?
	 * @param localMode
	 * 		only local channels?
	 * @param senderBufferPoolSize
	 * 		buffer pool size for the sender (set to <tt>-1</tt> for default)
	 * @param receiverBufferPoolSize
	 * 		buffer pool size for the receiver (set to <tt>-1</tt> for default)
	 * @param configuration The configuration
	 */
	public void setUp(
		int writers,
		int channels,
		boolean broadcastMode,
		boolean localMode,
		int senderBufferPoolSize,
		int receiverBufferPoolSize,
		Configuration configuration) throws Exception {
		this.broadcastMode = broadcastMode;
		this.localMode = localMode;
		this.channels = channels;
		this.configuration = checkNotNull(configuration);
		this.partitionIds = new ResultPartitionID[writers];
		this.bufferSize = configuration.getInteger(TaskManagerOptions.MEMORY_SEGMENT_SIZE);

		if (senderBufferPoolSize == -1) {
			senderBufferPoolSize = Math.max(4096, writers * channels * (
				configuration.getInteger(TaskManagerOptions.NETWORK_BUFFERS_PER_SUBPARTITION)));
		}
		if (receiverBufferPoolSize == -1) {
			receiverBufferPoolSize = Math.max(4096, writers * channels * (
				configuration.getInteger(TaskManagerOptions.NETWORK_BUFFERS_PER_CHANNEL) +
					configuration.getInteger(TaskManagerOptions.NETWORK_EXTRA_BUFFERS_PER_GATE)));
		}

		memoryManager = new MemoryManager(
			10240 * bufferSize, 0, 1, bufferSize, MemoryType.HEAP, true);

		ioManager = new IOManagerAsync();

		senderEnv = createNettyNetworkEnvironment(senderBufferPoolSize);
		senderEnv.start();
		if (localMode && senderBufferPoolSize == receiverBufferPoolSize) {
			receiverEnv = senderEnv;
		}
		else {
			receiverEnv = createNettyNetworkEnvironment(receiverBufferPoolSize);
			receiverEnv.start();
		}

		generatePartitionIds();
	}

	public void tearDown() throws InterruptedException {
		suppressExceptions(senderEnv::shutdown);
		suppressExceptions(receiverEnv::shutdown);
		suppressExceptions(ioManager::shutdown);
		suppressExceptions(memoryManager::shutdown);
		// shutdown the executor and wait the thread to finish
		// (jmh will check if there is any unfinished thread, it
		// is particularly import for external shuffle mode)
		suppressExceptions(executor::shutdownNow);
		while (!executor.isTerminated()) {
			Thread.sleep(1000);
		}
	}

	public StreamRecordWriter<T> createRecordWriter(ResultPartition resultPartition, long flushTimeout) throws Exception {
		ChannelSelector channelSelector = broadcastMode ? new BroadcastPartitioner<>() : new RoundRobinChannelSelector<>();

		return new StreamRecordWriter<T>(resultPartition, channelSelector , flushTimeout);
	}

	private void generatePartitionIds() throws Exception {
		for (int writer = 0; writer < partitionIds.length; writer++) {
			partitionIds[writer] = new ResultPartitionID();
		}
	}

	private NetworkEnvironment createNettyNetworkEnvironment(
			@SuppressWarnings("SameParameterValue") int bufferPoolSize) throws Exception {
		final NetworkBufferPool bufferPool = new NetworkBufferPool(bufferPoolSize, bufferSize);

		final NettyConnectionManager nettyConnectionManager = new NettyConnectionManager(
			new NettyConfig(LOCAL_ADDRESS, 0, bufferSize, NUM_SLOTS_AND_THREADS, new Configuration()));

		return new NetworkEnvironment(
			bufferPool,
			nettyConnectionManager,
			new ResultPartitionManager(),
			new TaskEventDispatcher(),
			new KvStateRegistry(),
			null,
			null,
			IOMode.SYNC,
			configuration.getInteger(TaskManagerOptions.NETWORK_REQUEST_BACKOFF_INITIAL),
			configuration.getInteger(TaskManagerOptions.NETWORK_REQUEST_BACKOFF_MAX),
			configuration.getInteger(TaskManagerOptions.NETWORK_BUFFERS_PER_CHANNEL),
			configuration.getInteger(TaskManagerOptions.NETWORK_EXTRA_BUFFERS_PER_GATE),
			configuration.getInteger(TaskManagerOptions.NETWORK_BUFFERS_PER_EXTERNAL_BLOCKING_CHANNEL),
			configuration.getInteger(TaskManagerOptions.NETWORK_EXTRA_BUFFERS_PER_EXTERNAL_BLOCKING_GATE),
			configuration.getInteger(TaskManagerOptions.NETWORK_BUFFERS_PER_SUBPARTITION),
			true);
	}

	protected ResultPartition<T> createInternalResultPartition(int subTaskIndex, TypeSerializer serializer) throws Exception {

		InternalResultPartition<T> resultPartition = new InternalResultPartition<T>(
			"sender task",
			new NoOpTaskActions(),
			jobId,
			partitionIds[subTaskIndex],
			ResultPartitionType.PIPELINED,
			channels,
			1,
			senderEnv.getResultPartitionManager(),
			new NoOpResultPartitionConsumableNotifier(),
			ioManager,
			false);

		resultPartition.setTypeSerializer(serializer);
		senderEnv.setupPartition(resultPartition);

		return resultPartition;
	}

	protected ResultPartition<T> createExternalResultPartition(int subTaskIndex, TypeSerializer<T> serializer) throws Exception {
		generatePartitionIds();

		ResultPartition<T> resultPartition = new ExternalResultPartition<>(
			configuration,
			"sender task",
			jobId,
			partitionIds[subTaskIndex],
			ResultPartitionType.BLOCKING,
			channels,
			1,
			memoryManager,
			ioManager);

		resultPartition.setTypeSerializer(serializer);
		resultPartition.setParentTask(parentTask);

		return resultPartition;
	}

	public SingleInputGate[] createInputGate(
		int dataPort,
		ResultPartitionType resultPartitionType,
		int channelIndexStart,
		int channelIndexEnd) throws IOException {
		TaskManagerLocation senderLocation = new TaskManagerLocation(
			ResourceID.generate(), LOCAL_ADDRESS, dataPort);

		return createInputGate(senderLocation, receiverEnv, resultPartitionType, channelIndexStart, channelIndexEnd);
	}

	public SingleInputGate[] createInputGate(
		ResultPartitionType resultPartitionType,
		int channelIndexStart,
		int channelIndexEnd) throws Exception {
		TaskManagerLocation senderLocation = new TaskManagerLocation(
			ResourceID.generate(),
			LOCAL_ADDRESS,
			senderEnv.getConnectionManager().getDataPort());

		return createInputGate(senderLocation, receiverEnv, resultPartitionType, channelIndexStart, channelIndexEnd);
	}

	private SingleInputGate[] createInputGate(
			final TaskManagerLocation senderLocation,
			NetworkEnvironment environment,
			ResultPartitionType resultPartitionType,
			int subpartitionStart,
			int subpartitionEnd) throws IOException {
		checkState(environment != null, "setup should be called before creating input gate");

		SingleInputGate[] gates = new SingleInputGate[subpartitionEnd - subpartitionStart];
		// the subpartitions range from subpartitionStart to subpartitionEnd will be assigned to one receiver thread,
		// each subpartition a single input gate and each input gate with (number of writer thread) input channels.
		for (int subpartitionIndex = subpartitionStart; subpartitionIndex < subpartitionEnd; ++subpartitionIndex) {
			int finalChannel = subpartitionIndex;
			InputChannelDeploymentDescriptor[] channelDescriptors = Arrays.stream(partitionIds)
				.map(partitionId -> new InputChannelDeploymentDescriptor(
					partitionId,
					localMode ? ResultPartitionLocation.createLocal() : ResultPartitionLocation.createRemote(new ConnectionID(senderLocation, finalChannel))))
				.toArray(InputChannelDeploymentDescriptor[]::new);

			final InputGateDeploymentDescriptor gateDescriptor = new InputGateDeploymentDescriptor(
				dataSetID,
				resultPartitionType,
				subpartitionIndex,
				channelDescriptors);

			SingleInputGate gate = SingleInputGate.create(
				"receiving task[" + subpartitionIndex + "]",
				jobId,
				executionAttemptID,
				gateDescriptor,
				environment,
				new NoOpTaskActions(),
				UnregisteredMetricGroups.createUnregisteredTaskMetricGroup().getIOMetricGroup(),
				new PartitionRequestManager(configuration.getInteger(TaskManagerOptions.TASK_EXTERNAL_SHUFFLE_MAX_CONCURRENT_REQUESTS), channelDescriptors.length),
				BlockingShuffleType.valueOf(configuration.getString(TaskManagerOptions.TASK_BLOCKING_SHUFFLE_TYPE)),
				executor);

			environment.setupInputGate(gate);
			gates[subpartitionIndex - subpartitionStart] = gate;
		}

		return gates;
	}

	// ------------------------------------------------------------------------
	//  Mocks
	// ------------------------------------------------------------------------

	/**
	 * A dummy implementation of the {@link TaskActions}. We implement this here rather than using Mockito
	 * to avoid using mockito in this benchmark class.
	 */
	private static final class NoOpTaskActions implements TaskActions {

		@Override
		public void triggerPartitionProducerStateCheck(JobID jobId, IntermediateDataSetID intermediateDataSetId, ResultPartitionID resultPartitionId) {

		}

		@Override
		public void failExternally(Throwable cause) {

		}
	}

	private static final class NoOpResultPartitionConsumableNotifier implements ResultPartitionConsumableNotifier {

		@Override
		public void notifyPartitionConsumable(JobID j, ResultPartitionID p, TaskActions t) {}
	}

	/**
	 * A mock invokable which is used by external shuffle.
	 */
	public final class MockInvokable extends AbstractInvokable {

		public MockInvokable() {
			super(mock(Environment.class));
		}

		@Override
		public void invoke() {}

		@Override
		public Configuration getTaskConfiguration() {
			return configuration;
		}
	}
}
