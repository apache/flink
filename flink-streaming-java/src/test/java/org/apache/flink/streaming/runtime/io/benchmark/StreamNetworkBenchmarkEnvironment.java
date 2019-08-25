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
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterBuilder;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.io.network.partition.InputChannelTestUtils;
import org.apache.flink.runtime.io.network.partition.NoOpResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionBuilder;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateFactory;
import org.apache.flink.runtime.io.network.partition.consumer.UnionInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.taskmanager.ConsumableNotifyingResultPartitionWriterDecorator;
import org.apache.flink.runtime.taskmanager.InputGateWithMetrics;
import org.apache.flink.runtime.taskmanager.NoOpTaskActions;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.ConfigurationParserUtils;
import org.apache.flink.runtime.util.NettyShuffleDescriptorBuilder;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

import static org.apache.flink.util.ExceptionUtils.suppressExceptions;

/**
 * Context for network benchmarks executed by the external
 * <a href="https://github.com/dataArtisans/flink-benchmarks">flink-benchmarks</a> project.
 */
public class StreamNetworkBenchmarkEnvironment<T extends IOReadableWritable> {

	private static final InetAddress LOCAL_ADDRESS;

	static {
		try {
			LOCAL_ADDRESS = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			throw new Error(e);
		}
	}

	private final ResourceID location = ResourceID.generate();
	protected final JobID jobId = new JobID();
	protected final IntermediateDataSetID dataSetID = new IntermediateDataSetID();

	protected NettyShuffleEnvironment senderEnv;
	protected NettyShuffleEnvironment receiverEnv;

	protected int channels;
	protected boolean broadcastMode = false;
	protected boolean localMode = false;

	protected ResultPartitionID[] partitionIds;

	private int dataPort;

	private SingleInputGateFactory gateFactory;

	public void setUp(
			int writers,
			int channels,
			boolean localMode,
			int senderBufferPoolSize,
			int receiverBufferPoolSize) throws Exception {
		setUp(
			writers,
			channels,
			false,
			localMode,
			senderBufferPoolSize,
			receiverBufferPoolSize,
			new Configuration());
	}

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
			boolean broadcastMode,
			boolean localMode,
			int senderBufferPoolSize,
			int receiverBufferPoolSize,
			Configuration config) throws Exception {
		this.broadcastMode = broadcastMode;
		this.localMode = localMode;
		this.channels = channels;
		this.partitionIds = new ResultPartitionID[writers];
		if (senderBufferPoolSize == -1) {
			senderBufferPoolSize = Math.max(2048, writers * channels * 4);
		}
		if (receiverBufferPoolSize == -1) {
			receiverBufferPoolSize = Math.max(2048, writers * channels * 4);
		}

		senderEnv = createShuffleEnvironment(senderBufferPoolSize, config);
		this.dataPort = senderEnv.start();
		if (localMode && senderBufferPoolSize == receiverBufferPoolSize) {
			receiverEnv = senderEnv;
		}
		else {
			receiverEnv = createShuffleEnvironment(receiverBufferPoolSize, config);
			receiverEnv.start();
		}

		gateFactory = new SingleInputGateFactory(
			location,
			receiverEnv.getConfiguration(),
			receiverEnv.getConnectionManager(),
			receiverEnv.getResultPartitionManager(),
			new TaskEventDispatcher(),
			receiverEnv.getNetworkBufferPool());

		generatePartitionIds();
	}

	public void tearDown() {
		suppressExceptions(senderEnv::close);
		suppressExceptions(receiverEnv::close);
	}

	public SerializingLongReceiver createReceiver() throws Exception {
		TaskManagerLocation senderLocation = new TaskManagerLocation(
			ResourceID.generate(),
			LOCAL_ADDRESS,
			dataPort);

		InputGate receiverGate = createInputGate(senderLocation);

		SerializingLongReceiver receiver = new SerializingLongReceiver(receiverGate, channels * partitionIds.length);

		receiver.start();
		return receiver;
	}

	public RecordWriter<T> createRecordWriter(int partitionIndex, long flushTimeout) throws Exception {
		ResultPartitionWriter sender = createResultPartition(jobId, partitionIds[partitionIndex], senderEnv, channels);
		return new RecordWriterBuilder().setTimeout(flushTimeout).build(sender);
	}

	private void generatePartitionIds() throws Exception {
		for (int writer = 0; writer < partitionIds.length; writer++) {
			partitionIds[writer] = new ResultPartitionID();
		}
	}

	private NettyShuffleEnvironment createShuffleEnvironment(
			@SuppressWarnings("SameParameterValue") int bufferPoolSize, Configuration config) throws Exception {

		final NettyConfig nettyConfig = new NettyConfig(
			LOCAL_ADDRESS,
			0,
			ConfigurationParserUtils.getPageSize(config),
			// please note that the number of slots directly influences the number of netty threads!
			ConfigurationParserUtils.getSlot(config),
			config);
		return new NettyShuffleEnvironmentBuilder()
			.setNumNetworkBuffers(bufferPoolSize)
			.setNettyConfig(nettyConfig)
			.build();
	}

	protected ResultPartitionWriter createResultPartition(
			JobID jobId,
			ResultPartitionID partitionId,
			NettyShuffleEnvironment environment,
			int channels) throws Exception {

		ResultPartitionWriter resultPartitionWriter = new ResultPartitionBuilder()
			.setResultPartitionId(partitionId)
			.setResultPartitionType(ResultPartitionType.PIPELINED_BOUNDED)
			.setNumberOfSubpartitions(channels)
			.setResultPartitionManager(environment.getResultPartitionManager())
			.setupBufferPoolFactoryFromNettyShuffleEnvironment(environment)
			.build();

		ResultPartitionWriter consumableNotifyingPartitionWriter = new ConsumableNotifyingResultPartitionWriterDecorator(
			new NoOpTaskActions(),
			jobId,
			resultPartitionWriter,
			new NoOpResultPartitionConsumableNotifier());

		consumableNotifyingPartitionWriter.setup();

		return consumableNotifyingPartitionWriter;
	}

	private InputGate createInputGate(TaskManagerLocation senderLocation) throws Exception {
		InputGate[] gates = new InputGate[channels];
		for (int channel = 0; channel < channels; ++channel) {
			final InputGateDeploymentDescriptor gateDescriptor = createInputGateDeploymentDescriptor(
				senderLocation,
				channel,
				location);

			final InputGate gate = createInputGateWithMetrics(gateFactory, gateDescriptor, channel);

			gate.setup();
			gates[channel] = gate;
		}

		if (channels > 1) {
			return new UnionInputGate(gates);
		} else {
			return gates[0];
		}
	}

	private InputGateDeploymentDescriptor createInputGateDeploymentDescriptor(
			TaskManagerLocation senderLocation,
			int consumedSubpartitionIndex,
			ResourceID localLocation) {

		final ShuffleDescriptor[] channelDescriptors = Arrays.stream(partitionIds)
			.map(partitionId ->
				createShuffleDescriptor(localMode, partitionId, localLocation, senderLocation, consumedSubpartitionIndex))
			.toArray(ShuffleDescriptor[]::new);

		return new InputGateDeploymentDescriptor(
			dataSetID,
			ResultPartitionType.PIPELINED_BOUNDED,
			consumedSubpartitionIndex,
			channelDescriptors);
	}

	private InputGate createInputGateWithMetrics(
		SingleInputGateFactory gateFactory,
		InputGateDeploymentDescriptor gateDescriptor,
		int channelIndex) {

		final SingleInputGate singleGate = gateFactory.create(
			"receiving task[" + channelIndex + "]",
			gateDescriptor,
			SingleInputGateBuilder.NO_OP_PRODUCER_CHECKER,
			InputChannelTestUtils.newUnregisteredInputChannelMetrics());

		return new InputGateWithMetrics(singleGate, new SimpleCounter());
	}

	private static ShuffleDescriptor createShuffleDescriptor(
			boolean localMode,
			ResultPartitionID resultPartitionID,
			ResourceID location,
			TaskManagerLocation senderLocation,
			int channel) {
		final NettyShuffleDescriptorBuilder builder = NettyShuffleDescriptorBuilder.newBuilder()
			.setId(resultPartitionID)
			.setProducerInfoFromTaskManagerLocation(senderLocation)
			.setConnectionIndex(channel);
		return localMode ? builder.setProducerLocation(location).buildLocal() : builder.buildRemote();
	}
}
