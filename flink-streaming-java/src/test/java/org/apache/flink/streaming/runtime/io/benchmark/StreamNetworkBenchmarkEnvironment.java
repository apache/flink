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
import org.apache.flink.runtime.deployment.InputChannelDeploymentDescriptor;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionLocation;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.NetworkEnvironmentBuilder;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterBuilder;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.io.network.partition.InputChannelTestUtils;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionBuilder;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateFactory;
import org.apache.flink.runtime.io.network.partition.consumer.UnionInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.taskmanager.InputGateWithMetrics;
import org.apache.flink.runtime.taskmanager.NetworkEnvironmentConfiguration;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.ConfigurationParserUtils;

import java.io.IOException;
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

	protected final JobID jobId = new JobID();
	protected final IntermediateDataSetID dataSetID = new IntermediateDataSetID();

	protected NetworkEnvironment senderEnv;
	protected NetworkEnvironment receiverEnv;
	protected IOManager ioManager;

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

		ioManager = new IOManagerAsync();

		senderEnv = createNettyNetworkEnvironment(senderBufferPoolSize, config);
		this.dataPort = senderEnv.start();
		if (localMode && senderBufferPoolSize == receiverBufferPoolSize) {
			receiverEnv = senderEnv;
		}
		else {
			receiverEnv = createNettyNetworkEnvironment(receiverBufferPoolSize, config);
			receiverEnv.start();
		}

		gateFactory = new SingleInputGateFactory(
			receiverEnv.getConfiguration(),
			receiverEnv.getConnectionManager(),
			receiverEnv.getResultPartitionManager(),
			new TaskEventDispatcher(),
			receiverEnv.getNetworkBufferPool());

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

	private NetworkEnvironment createNettyNetworkEnvironment(
			@SuppressWarnings("SameParameterValue") int bufferPoolSize, Configuration config) throws Exception {

		final NettyConfig nettyConfig = new NettyConfig(
			LOCAL_ADDRESS,
			0,
			NetworkEnvironmentConfiguration.getPageSize(config),
			// please note that the number of slots directly influences the number of netty threads!
			ConfigurationParserUtils.getSlot(config),
			config);
		return new NetworkEnvironmentBuilder()
			.setNumNetworkBuffers(bufferPoolSize)
			.setNettyConfig(nettyConfig)
			.build();
	}

	protected ResultPartitionWriter createResultPartition(
			JobID jobId,
			ResultPartitionID partitionId,
			NetworkEnvironment environment,
			int channels) throws Exception {

		ResultPartition resultPartition = new ResultPartitionBuilder()
			.setJobId(jobId)
			.setResultPartitionId(partitionId)
			.setResultPartitionType(ResultPartitionType.PIPELINED_BOUNDED)
			.setNumberOfSubpartitions(channels)
			.setResultPartitionManager(environment.getResultPartitionManager())
			.setIOManager(ioManager)
			.setupBufferPoolFactoryFromNetworkEnvironment(environment)
			.build();

		resultPartition.setup();

		return resultPartition;
	}

	private InputGate createInputGate(TaskManagerLocation senderLocation) throws IOException {
		InputGate[] gates = new InputGate[channels];
		for (int channel = 0; channel < channels; ++channel) {
			final InputGateDeploymentDescriptor gateDescriptor = createInputGateDeploymentDescriptor(
				senderLocation,
				channel);

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
			int consumedSubpartitionIndex) {

		final InputChannelDeploymentDescriptor[] channelDescriptors = Arrays.stream(partitionIds)
			.map(partitionId -> new InputChannelDeploymentDescriptor(
				partitionId,
				localMode ? ResultPartitionLocation.createLocal() : ResultPartitionLocation.createRemote(
					new ConnectionID(senderLocation, consumedSubpartitionIndex))))
			.toArray(InputChannelDeploymentDescriptor[]::new);

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
}
