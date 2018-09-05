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

package org.apache.flink.streaming.runtime.io.benchmark;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.InputChannelDeploymentDescriptor;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionLocation;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManager.IOMode;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.api.writer.RoundRobinChannelSelector;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.io.network.netty.NettyConnectionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.UnionInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.taskmanager.TaskActions;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.streaming.runtime.io.StreamRecordWriter;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

import static org.apache.flink.util.ExceptionUtils.suppressExceptions;
import static org.apache.flink.util.MathUtils.checkedDownCast;

/**
 * Context for network benchmarks executed by the external
 * <a href="https://github.com/dataArtisans/flink-benchmarks">flink-benchmarks</a> project.
 */
public class StreamNetworkBenchmarkEnvironment<T extends IOReadableWritable> {

	private static final int BUFFER_SIZE =
		checkedDownCast(MemorySize.parse(TaskManagerOptions.MEMORY_SEGMENT_SIZE.defaultValue()).getBytes());

	private static final int NUM_SLOTS_AND_THREADS = 1;

	private static final InetAddress LOCAL_ADDRESS;

	static {
		try {
			LOCAL_ADDRESS = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			throw new Error(e);
		}
	}

	protected final JobID jobId = new JobID();
	protected final IntermediateDataSetID dataSetID = new IntermediateDataSetID();
	protected final ExecutionAttemptID executionAttemptID = new ExecutionAttemptID();

	protected NetworkEnvironment senderEnv;
	protected NetworkEnvironment receiverEnv;
	protected IOManager ioManager;

	protected int channels;
	protected boolean localMode = false;

	protected ResultPartitionID[] partitionIds;

	/**
	 * Sets up the environment including buffer pools and netty threads.
	 *
	 * @param writers
	 * 		number of writers
	 * @param channels
	 * 		outgoing channels per writer
	 * @param localMode
	 * 		only local channels?
	 * @param senderBufferPoolSize
	 * 		buffer pool size for the sender (set to <tt>-1</tt> for default)
	 * @param receiverBufferPoolSize
	 * 		buffer pool size for the receiver (set to <tt>-1</tt> for default)
	 */
	public void setUp(
			int writers,
			int channels,
			boolean localMode,
			int senderBufferPoolSize,
			int receiverBufferPoolSize) throws Exception {
		this.localMode = localMode;
		this.channels = channels;
		this.partitionIds = new ResultPartitionID[writers];
		if (senderBufferPoolSize == -1) {
			senderBufferPoolSize = Math.max(2048, writers * channels * 4);
		}
		if (receiverBufferPoolSize == -1) {
			receiverBufferPoolSize = Math.max(2048, writers * channels * 4);
		}

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

	public void tearDown() {
		suppressExceptions(senderEnv::shutdown);
		suppressExceptions(receiverEnv::shutdown);
		suppressExceptions(ioManager::shutdown);
	}

	public SerializingLongReceiver createReceiver() throws Exception {
		TaskManagerLocation senderLocation = new TaskManagerLocation(
			ResourceID.generate(),
			LOCAL_ADDRESS,
			senderEnv.getConnectionManager().getDataPort());

		InputGate receiverGate = createInputGate(
			jobId,
			dataSetID,
			executionAttemptID,
			senderLocation,
			receiverEnv,
			channels);

		SerializingLongReceiver receiver = new SerializingLongReceiver(receiverGate, channels * partitionIds.length);

		receiver.start();
		return receiver;
	}

	public StreamRecordWriter<T> createRecordWriter(int partitionIndex, long flushTimeout) throws Exception {
		ResultPartitionWriter sender = createResultPartition(jobId, partitionIds[partitionIndex], senderEnv, channels);
		return new StreamRecordWriter<>(sender,  new RoundRobinChannelSelector<T>(), flushTimeout);
	}

	private void generatePartitionIds() throws Exception {
		for (int writer = 0; writer < partitionIds.length; writer++) {
			partitionIds[writer] = new ResultPartitionID();
		}
	}

	private NetworkEnvironment createNettyNetworkEnvironment(
			@SuppressWarnings("SameParameterValue") int bufferPoolSize) throws Exception {

		final NetworkBufferPool bufferPool = new NetworkBufferPool(bufferPoolSize, BUFFER_SIZE);

		final NettyConnectionManager nettyConnectionManager = new NettyConnectionManager(
			new NettyConfig(LOCAL_ADDRESS, 0, BUFFER_SIZE, NUM_SLOTS_AND_THREADS, new Configuration()));

		return new NetworkEnvironment(
			bufferPool,
			nettyConnectionManager,
			new ResultPartitionManager(),
			new TaskEventDispatcher(),
			new KvStateRegistry(),
			null,
			null,
			IOMode.SYNC,
			TaskManagerOptions.NETWORK_REQUEST_BACKOFF_INITIAL.defaultValue(),
			TaskManagerOptions.NETWORK_REQUEST_BACKOFF_MAX.defaultValue(),
			TaskManagerOptions.NETWORK_BUFFERS_PER_CHANNEL.defaultValue(),
			TaskManagerOptions.NETWORK_EXTRA_BUFFERS_PER_GATE.defaultValue(),
			true);
	}

	protected ResultPartitionWriter createResultPartition(
			JobID jobId,
			ResultPartitionID partitionId,
			NetworkEnvironment environment,
			int channels) throws Exception {

		ResultPartition resultPartition = new ResultPartition(
			"sender task",
			new NoOpTaskActions(),
			jobId,
			partitionId,
			ResultPartitionType.PIPELINED_BOUNDED,
			channels,
			1,
			environment.getResultPartitionManager(),
			new NoOpResultPartitionConsumableNotifier(),
			ioManager,
			false);

		environment.setupPartition(resultPartition);

		return resultPartition;
	}

	private InputGate createInputGate(
			JobID jobId,
			IntermediateDataSetID dataSetID,
			ExecutionAttemptID executionAttemptID,
			final TaskManagerLocation senderLocation,
			NetworkEnvironment environment,
			final int channels) throws IOException {

		InputGate[] gates = new InputGate[channels];
		for (int channel = 0; channel < channels; ++channel) {
			int finalChannel = channel;
			InputChannelDeploymentDescriptor[] channelDescriptors = Arrays.stream(partitionIds)
				.map(partitionId -> new InputChannelDeploymentDescriptor(
					partitionId,
					localMode ? ResultPartitionLocation.createLocal() : ResultPartitionLocation.createRemote(new ConnectionID(senderLocation, finalChannel))))
				.toArray(InputChannelDeploymentDescriptor[]::new);

			final InputGateDeploymentDescriptor gateDescriptor = new InputGateDeploymentDescriptor(
				dataSetID,
				ResultPartitionType.PIPELINED_BOUNDED,
				channel,
				channelDescriptors);

			SingleInputGate gate = SingleInputGate.create(
				"receiving task[" + channel + "]",
				jobId,
				executionAttemptID,
				gateDescriptor,
				environment,
				new NoOpTaskActions(),
				UnregisteredMetricGroups.createUnregisteredTaskMetricGroup().getIOMetricGroup());

			environment.setupInputGate(gate);
			gates[channel] = gate;
		}

		if (channels > 1) {
			return new UnionInputGate(gates);
		} else {
			return gates[0];
		}
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
		public void triggerPartitionProducerStateCheck(
			JobID jobId,
			IntermediateDataSetID intermediateDataSetId,
			ResultPartitionID resultPartitionId) {}

		@Override
		public void failExternally(Throwable cause) {}
	}

	private static final class NoOpResultPartitionConsumableNotifier implements ResultPartitionConsumableNotifier {

		@Override
		public void notifyPartitionConsumable(JobID j, ResultPartitionID p, TaskActions t) {}
	}
}
