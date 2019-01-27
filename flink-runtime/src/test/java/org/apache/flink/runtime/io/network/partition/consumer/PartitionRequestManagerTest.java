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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.deployment.InputChannelDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionLocation;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.netty.PartitionRequestClient;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.taskmanager.TaskActions;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link PartitionRequestManager}.
 */
public class PartitionRequestManagerTest {

	@Test
	public void testOneInputGateWithAdequatePartitionRequestQuota() throws Exception {
		final ResultSubpartitionView iterator = mock(ResultSubpartitionView.class);

		final ResultPartitionManager partitionManager = mock(ResultPartitionManager.class);
		when(partitionManager.createSubpartitionView(
			any(ResultPartitionID.class),
			anyInt(),
			any(BufferAvailabilityListener.class))).thenReturn(iterator);

		PartitionRequestClient partitionRequestClient = mock(PartitionRequestClient.class);
		ConnectionManager connectionManager = mock(ConnectionManager.class);
		when(connectionManager.createPartitionRequestClient(any(ConnectionID.class))).thenReturn(partitionRequestClient);

		NetworkBufferPool networkBufferPool = new NetworkBufferPool(100, 32);

		int buffersPerChannel = 2;
		int extraNetworkBuffersPerGate = 8;
		int buffersPerBlockingChannel = 128;
		int extraNetworkBuffersPerBlockingGate = 0;
		int buffersPerSubpartition = 2;
		final NetworkEnvironment network = createNetworkEnvironment(networkBufferPool, partitionManager, connectionManager,
			buffersPerChannel, extraNetworkBuffersPerGate, buffersPerBlockingChannel, extraNetworkBuffersPerBlockingGate, buffersPerSubpartition, 0, 0);

		PartitionRequestManager partitionRequestManager = new PartitionRequestManager(2, 1);
		final SingleInputGate inputGate = createInputGate(network, partitionRequestManager);

		ResultPartitionID localPartitionId = new ResultPartitionID(new IntermediateResultPartitionID(), new ExecutionAttemptID());
		ResultPartitionID unknownPartitionId = new ResultPartitionID(new IntermediateResultPartitionID(), new ExecutionAttemptID());
		ResultPartitionID remotePartitionId = new ResultPartitionID(new IntermediateResultPartitionID(), new ExecutionAttemptID());
		final ConnectionID connectionId = new ConnectionID(new InetSocketAddress("localhost", 5000), 0);

		InputChannel remote = addRemoteInputChannel(network, inputGate, connectionId, remotePartitionId, 0);
		inputGate.setInputChannel(remotePartitionId.getPartitionId(), remote);
		InputChannel unknown = addUnknownInputChannel(network, inputGate, unknownPartitionId, 1);
		inputGate.setInputChannel(unknownPartitionId.getPartitionId(), unknown);
		InputChannel local = spy(addLocalInputChannel(network, inputGate, localPartitionId, 2));
		inputGate.setInputChannel(localPartitionId.getPartitionId(), local);

		// Request partitions
		inputGate.requestPartitions();

		verify(local, times(1)).requestSubpartition(anyInt());

		verify(partitionRequestClient, times(1)).requestSubpartition(eq(remotePartitionId), eq(0), eq((RemoteInputChannel)remote), anyInt());

		inputGate.updateInputChannel(new InputChannelDeploymentDescriptor(unknownPartitionId, ResultPartitionLocation.createRemote(connectionId)));

		verify(partitionRequestClient, times(2)).requestSubpartition(any(ResultPartitionID.class), anyInt(), any(RemoteInputChannel.class), anyInt());
	}

	@Test
	public void testOneInputGateWithInadequatePartitionRequestQuotaForRemoteChannel() throws Exception {
		final ResultSubpartitionView iterator = mock(ResultSubpartitionView.class);

		final ResultPartitionManager partitionManager = mock(ResultPartitionManager.class);
		when(partitionManager.createSubpartitionView(
			any(ResultPartitionID.class),
			anyInt(),
			any(BufferAvailabilityListener.class))).thenReturn(iterator);

		PartitionRequestClient partitionRequestClient = mock(PartitionRequestClient.class);
		ConnectionManager connectionManager = mock(ConnectionManager.class);
		when(connectionManager.createPartitionRequestClient(any(ConnectionID.class))).thenReturn(partitionRequestClient);

		NetworkBufferPool networkBufferPool = new NetworkBufferPool(100, 32);

		int buffersPerChannel = 2;
		int extraNetworkBuffersPerGate = 8;
		int buffersPerBlockingChannel = 128;
		int extraNetworkBuffersPerBlockingGate = 0;
		int buffersPerSubpartition = 2;
		final NetworkEnvironment network = createNetworkEnvironment(networkBufferPool, partitionManager, connectionManager,
			buffersPerChannel, extraNetworkBuffersPerGate, buffersPerBlockingChannel, extraNetworkBuffersPerBlockingGate, buffersPerSubpartition, 0, 0);

		PartitionRequestManager partitionRequestManager = new PartitionRequestManager(1, 1);
		final SingleInputGate inputGate = createInputGate(network, partitionRequestManager);

		ResultPartitionID localPartitionId = new ResultPartitionID(new IntermediateResultPartitionID(), new ExecutionAttemptID());
		ResultPartitionID remotePartitionId1 = new ResultPartitionID(new IntermediateResultPartitionID(), new ExecutionAttemptID());
		ResultPartitionID remotePartitionId2 = new ResultPartitionID(new IntermediateResultPartitionID(), new ExecutionAttemptID());
		final ConnectionID connectionId = new ConnectionID(new InetSocketAddress("localhost", 5000), 0);

		InputChannel remote1 = mock(RemoteInputChannel.class);
		Optional endOfPartitionEvent = Optional.of(
			new InputChannel.BufferAndAvailability(
				EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE), false, 0));
		when(remote1.getNextBuffer()).thenReturn(endOfPartitionEvent);
		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
				partitionRequestClient.requestSubpartition(remotePartitionId1, 0, (RemoteInputChannel) remote1, 0);
				return null;
			}
		}).when(remote1).requestSubpartition(anyInt());
		inputGate.setInputChannel(remotePartitionId1.getPartitionId(), remote1);

		InputChannel remote2 = spy(addRemoteInputChannel(network, inputGate, connectionId, remotePartitionId2, 1));
		inputGate.setInputChannel(remotePartitionId2.getPartitionId(), remote2);
		InputChannel local = spy(addLocalInputChannel(network, inputGate, localPartitionId, 2));
		inputGate.setInputChannel(localPartitionId.getPartitionId(), local);

		// Request partitions
		inputGate.requestPartitions();

		verify(local, times(1)).requestSubpartition(anyInt());

		verify(partitionRequestClient, times(1)).requestSubpartition(any(ResultPartitionID.class), anyInt(), any(RemoteInputChannel.class), anyInt());

		inputGate.notifyChannelNonEmpty(remote1);
		inputGate.getNextBufferOrEvent();

		verify(partitionRequestClient, times(2)).requestSubpartition(any(ResultPartitionID.class), anyInt(), any(RemoteInputChannel.class), anyInt());
	}

	@Test
	public void testOneInputGateWithInadequatePartitionRequestQuotaForUnknownChannel() throws Exception {
		final ResultSubpartitionView iterator = mock(ResultSubpartitionView.class);
		when(iterator.getNextBuffer()).thenReturn(
			new ResultSubpartition.BufferAndBacklog(new NetworkBuffer(MemorySegmentFactory.allocateUnpooledSegment(1024), FreeingBufferRecycler.INSTANCE), false,0, false));

		final ResultPartitionManager partitionManager = mock(ResultPartitionManager.class);
		when(partitionManager.createSubpartitionView(
			any(ResultPartitionID.class),
			anyInt(),
			any(BufferAvailabilityListener.class))).thenReturn(iterator);

		PartitionRequestClient partitionRequestClient = mock(PartitionRequestClient.class);
		ConnectionManager connectionManager = mock(ConnectionManager.class);
		when(connectionManager.createPartitionRequestClient(any(ConnectionID.class))).thenReturn(partitionRequestClient);

		NetworkBufferPool networkBufferPool = new NetworkBufferPool(100, 32);

		int buffersPerChannel = 2;
		int extraNetworkBuffersPerGate = 8;
		int buffersPerBlockingChannel = 128;
		int extraNetworkBuffersPerBlockingGate = 0;
		int buffersPerSubpartition = 2;
		final NetworkEnvironment network = createNetworkEnvironment(networkBufferPool, partitionManager, connectionManager,
			buffersPerChannel, extraNetworkBuffersPerGate, buffersPerBlockingChannel, extraNetworkBuffersPerBlockingGate, buffersPerSubpartition, 0, 0);

		PartitionRequestManager partitionRequestManager = new PartitionRequestManager(1, 1);
		final SingleInputGate inputGate = createInputGate(network, partitionRequestManager);

		ResultPartitionID localPartitionId = new ResultPartitionID(new IntermediateResultPartitionID(), new ExecutionAttemptID());
		ResultPartitionID unknownPartitionId = new ResultPartitionID(new IntermediateResultPartitionID(), new ExecutionAttemptID());
		ResultPartitionID remotePartitionId = new ResultPartitionID(new IntermediateResultPartitionID(), new ExecutionAttemptID());
		final ConnectionID connectionId = new ConnectionID(new InetSocketAddress("localhost", 5000), 0);

		InputChannel remote = mock(RemoteInputChannel.class);
		Optional endOfPartitionEvent = Optional.of(
			new InputChannel.BufferAndAvailability(
				EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE), false, 0));
		when(remote.getNextBuffer()).thenReturn(endOfPartitionEvent);
		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
				partitionRequestClient.requestSubpartition(remotePartitionId, 0, (RemoteInputChannel) remote, 0);
				return null;
			}
		}).when(remote).requestSubpartition(anyInt());
		inputGate.setInputChannel(remotePartitionId.getPartitionId(), remote);

		InputChannel unknown = addUnknownInputChannel(network, inputGate, unknownPartitionId, 1);
		inputGate.setInputChannel(unknownPartitionId.getPartitionId(), unknown);
		InputChannel local = spy(addLocalInputChannel(network, inputGate, localPartitionId, 2));
		inputGate.setInputChannel(localPartitionId.getPartitionId(), local);

		// Request partitions
		inputGate.requestPartitions();

		verify(local, times(1)).requestSubpartition(anyInt());

		verify(partitionRequestClient, times(1)).requestSubpartition(eq(remotePartitionId), eq(0), eq((RemoteInputChannel)remote), anyInt());

		inputGate.updateInputChannel(new InputChannelDeploymentDescriptor(unknownPartitionId, ResultPartitionLocation.createRemote(connectionId)));

		verify(partitionRequestClient, times(1)).requestSubpartition(any(ResultPartitionID.class), anyInt(), any(RemoteInputChannel.class), anyInt());

		inputGate.notifyChannelNonEmpty(remote);
		inputGate.getNextBufferOrEvent();

		verify(partitionRequestClient, times(2)).requestSubpartition(any(ResultPartitionID.class), anyInt(), any(RemoteInputChannel.class), anyInt());
	}

	@Test
	public void testMultiInputGateWithAdequatePartitionRequestQuota() throws Exception {
		final ResultSubpartitionView iterator = mock(ResultSubpartitionView.class);

		final ResultPartitionManager partitionManager = mock(ResultPartitionManager.class);
		when(partitionManager.createSubpartitionView(
			any(ResultPartitionID.class),
			anyInt(),
			any(BufferAvailabilityListener.class))).thenReturn(iterator);

		PartitionRequestClient partitionRequestClient = mock(PartitionRequestClient.class);
		ConnectionManager connectionManager = mock(ConnectionManager.class);
		when(connectionManager.createPartitionRequestClient(any(ConnectionID.class))).thenReturn(partitionRequestClient);

		NetworkBufferPool networkBufferPool = new NetworkBufferPool(100, 32);

		int buffersPerChannel = 2;
		int extraNetworkBuffersPerGate = 8;
		int buffersPerBlockingChannel = 128;
		int extraNetworkBuffersPerBlockingGate = 0;
		int buffersPerSubpartition = 2;
		final NetworkEnvironment network = createNetworkEnvironment(networkBufferPool, partitionManager, connectionManager,
			buffersPerChannel, extraNetworkBuffersPerGate, buffersPerBlockingChannel, extraNetworkBuffersPerBlockingGate, buffersPerSubpartition, 0, 0);

		PartitionRequestManager partitionRequestManager = new PartitionRequestManager(4, 2);
		final SingleInputGate inputGate1 = createInputGate(network, partitionRequestManager);
		final SingleInputGate inputGate2 = createInputGate(network, partitionRequestManager);

		ResultPartitionID localPartitionId = new ResultPartitionID(new IntermediateResultPartitionID(), new ExecutionAttemptID());
		ResultPartitionID unknownPartitionId = new ResultPartitionID(new IntermediateResultPartitionID(), new ExecutionAttemptID());
		ResultPartitionID remotePartitionId = new ResultPartitionID(new IntermediateResultPartitionID(), new ExecutionAttemptID());
		final ConnectionID connectionId = new ConnectionID(new InetSocketAddress("localhost", 5000), 0);

		InputChannel remote1 = addRemoteInputChannel(network, inputGate1, connectionId, remotePartitionId, 0);
		inputGate1.setInputChannel(remotePartitionId.getPartitionId(), remote1);
		InputChannel unknown1 = addUnknownInputChannel(network, inputGate1, unknownPartitionId, 1);
		inputGate1.setInputChannel(unknownPartitionId.getPartitionId(), unknown1);
		InputChannel local1 = spy(addLocalInputChannel(network, inputGate1, localPartitionId, 2));
		inputGate1.setInputChannel(localPartitionId.getPartitionId(), local1);

		InputChannel remote2 = addRemoteInputChannel(network, inputGate2, connectionId, remotePartitionId, 0);
		inputGate2.setInputChannel(remotePartitionId.getPartitionId(), remote2);
		InputChannel unknown2 = addUnknownInputChannel(network, inputGate2, unknownPartitionId, 1);
		inputGate2.setInputChannel(unknownPartitionId.getPartitionId(), unknown2);
		InputChannel local2 = spy(addLocalInputChannel(network, inputGate2, localPartitionId, 2));
		inputGate2.setInputChannel(localPartitionId.getPartitionId(), local2);

		// Request partitions
		inputGate1.requestPartitions();

		verify(local1, times(1)).requestSubpartition(anyInt());

		verify(partitionRequestClient, times(1)).requestSubpartition(eq(remotePartitionId), eq(0), eq((RemoteInputChannel)remote1), anyInt());

		inputGate1.updateInputChannel(new InputChannelDeploymentDescriptor(unknownPartitionId, ResultPartitionLocation.createRemote(connectionId)));

		verify(partitionRequestClient, times(2)).requestSubpartition(any(ResultPartitionID.class), anyInt(), any(RemoteInputChannel.class), anyInt());

		inputGate2.requestPartitions();

		verify(local2, times(1)).requestSubpartition(anyInt());

		verify(partitionRequestClient, times(1)).requestSubpartition(eq(remotePartitionId), eq(0), eq((RemoteInputChannel)remote2), anyInt());

		inputGate2.updateInputChannel(new InputChannelDeploymentDescriptor(unknownPartitionId, ResultPartitionLocation.createRemote(connectionId)));

		verify(partitionRequestClient, times(4)).requestSubpartition(any(ResultPartitionID.class), anyInt(), any(RemoteInputChannel.class), anyInt());

	}

	@Test
	public void testMultiInputGateWithInadequatePartitionRequestQuota() throws Exception {
		final ResultSubpartitionView iterator = mock(ResultSubpartitionView.class);

		final ResultPartitionManager partitionManager = mock(ResultPartitionManager.class);
		when(partitionManager.createSubpartitionView(
			any(ResultPartitionID.class),
			anyInt(),
			any(BufferAvailabilityListener.class))).thenReturn(iterator);

		PartitionRequestClient partitionRequestClient = mock(PartitionRequestClient.class);
		ConnectionManager connectionManager = mock(ConnectionManager.class);
		when(connectionManager.createPartitionRequestClient(any(ConnectionID.class))).thenReturn(partitionRequestClient);

		NetworkBufferPool networkBufferPool = new NetworkBufferPool(100, 32);

		int buffersPerChannel = 2;
		int extraNetworkBuffersPerGate = 8;
		int buffersPerBlockingChannel = 128;
		int extraNetworkBuffersPerBlockingGate = 0;
		int buffersPerSubpartition = 2;
		final NetworkEnvironment network = createNetworkEnvironment(networkBufferPool, partitionManager, connectionManager,
			buffersPerChannel, extraNetworkBuffersPerGate, buffersPerBlockingChannel, extraNetworkBuffersPerBlockingGate, buffersPerSubpartition, 0, 0);

		PartitionRequestManager partitionRequestManager = new PartitionRequestManager(2, 2);
		final SingleInputGate inputGate1 = createInputGate(
			2, ResultPartitionType.BLOCKING, network, partitionRequestManager);
		final SingleInputGate inputGate2 = createInputGate(network, partitionRequestManager);

		ResultPartitionID localPartitionId = new ResultPartitionID(new IntermediateResultPartitionID(), new ExecutionAttemptID());
		ResultPartitionID unknownPartitionId = new ResultPartitionID(new IntermediateResultPartitionID(), new ExecutionAttemptID());
		ResultPartitionID remotePartitionId1 = new ResultPartitionID(new IntermediateResultPartitionID(), new ExecutionAttemptID());
		ResultPartitionID remotePartitionId2 = new ResultPartitionID(new IntermediateResultPartitionID(), new ExecutionAttemptID());
		final ConnectionID connectionId = new ConnectionID(new InetSocketAddress("localhost", 5000), 0);

		final boolean[] isRemote1Request = {true};
		InputChannel remote1 = mock(RemoteInputChannel.class);
		Optional endOfPartitionEvent = Optional.of(
			new InputChannel.BufferAndAvailability(
				EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE), false, 0));
		when(remote1.getNextBuffer()).thenReturn(endOfPartitionEvent);
		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
				partitionRequestClient.requestSubpartition(remotePartitionId1, 0, (RemoteInputChannel) remote1, 0);
				isRemote1Request[0] = true;
				return null;
			}
		}).when(remote1).requestSubpartition(anyInt());
		when(remote1.getChannelIndex()).thenReturn(0);
		inputGate1.setInputChannel(remotePartitionId1.getPartitionId(), remote1);

		InputChannel remote2 = mock(RemoteInputChannel.class);
		when(remote2.getNextBuffer()).thenReturn(endOfPartitionEvent);
		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
				partitionRequestClient.requestSubpartition(remotePartitionId2, 0, (RemoteInputChannel) remote2, 0);
				isRemote1Request[0] = false;
				return null;
			}
		}).when(remote2).requestSubpartition(anyInt());
		when(remote2.getChannelIndex()).thenReturn(1);
		inputGate1.setInputChannel(remotePartitionId2.getPartitionId(), remote2);

		InputChannel remote3 = mock(RemoteInputChannel.class);
		when(remote3.getNextBuffer()).thenReturn(endOfPartitionEvent);
		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
				partitionRequestClient.requestSubpartition(remotePartitionId1, 0, (RemoteInputChannel) remote3, 0);
				return null;
			}
		}).when(remote3).requestSubpartition(anyInt());
		inputGate2.setInputChannel(remotePartitionId1.getPartitionId(), remote3);

		InputChannel unknown2 = addUnknownInputChannel(network, inputGate2, unknownPartitionId, 1);
		inputGate2.setInputChannel(unknownPartitionId.getPartitionId(), unknown2);
		InputChannel local2 = spy(addLocalInputChannel(network, inputGate2, localPartitionId, 2));
		inputGate2.setInputChannel(localPartitionId.getPartitionId(), local2);

		// Request partitions
		inputGate1.requestPartitions();
		inputGate2.requestPartitions();

		verify(partitionRequestClient, times(2)).requestSubpartition(any(ResultPartitionID.class), anyInt(), any(RemoteInputChannel.class), anyInt());

		verify(local2, times(1)).requestSubpartition(anyInt());

		verify(partitionRequestClient, times(1)).requestSubpartition(eq(remotePartitionId1), eq(0), eq((RemoteInputChannel)remote3), anyInt());

		if (isRemote1Request[0]) {
			verify(partitionRequestClient, times(1)).requestSubpartition(eq(remotePartitionId1), eq(0), eq((RemoteInputChannel) remote1), anyInt());
			verify(remote1, times(1)).requestSubpartition(anyInt());
			inputGate1.notifyChannelNonEmpty(remote1);
			inputGate1.getNextBufferOrEvent();
		} else {
			verify(partitionRequestClient, times(1)).requestSubpartition(eq(remotePartitionId2), eq(0), eq((RemoteInputChannel) remote2), anyInt());
			verify(remote2, times(1)).requestSubpartition(anyInt());
			inputGate1.notifyChannelNonEmpty(remote2);
			inputGate1.getNextBufferOrEvent();
		}

		verify(partitionRequestClient, times(3)).requestSubpartition(any(ResultPartitionID.class), anyInt(), any(RemoteInputChannel.class), anyInt());

		if (isRemote1Request[0]) {
			inputGate1.notifyChannelNonEmpty(remote1);
			inputGate1.getNextBufferOrEvent();
		} else {
			inputGate1.notifyChannelNonEmpty(remote2);
			inputGate1.getNextBufferOrEvent();
		}

		inputGate2.updateInputChannel(new InputChannelDeploymentDescriptor(unknownPartitionId, ResultPartitionLocation.createRemote(connectionId)));

		verify(partitionRequestClient, times(4)).requestSubpartition(any(ResultPartitionID.class), anyInt(), any(RemoteInputChannel.class), anyInt());

		inputGate2.notifyChannelNonEmpty(remote3);
		inputGate2.getNextBufferOrEvent();

		verify(partitionRequestClient, times(4)).requestSubpartition(any(ResultPartitionID.class), anyInt(), any(RemoteInputChannel.class), anyInt());
	}

	private static List<int[]> generateParameters() {
		List<int[]> parameters = new LinkedList<>();
		for (int numberOfGate = 2; numberOfGate <= 5; ++numberOfGate) {
			for (int numberOfChannel = 2; numberOfChannel <= 5; ++numberOfChannel) {
				for (int numberOfUnknownChannel = 0; numberOfUnknownChannel <= numberOfChannel; ++numberOfUnknownChannel) {
					for (int quota = numberOfGate; quota <= numberOfGate * numberOfChannel; ++quota) {
						parameters.add(new int[] {numberOfGate, numberOfChannel, numberOfUnknownChannel, quota});
					}
					parameters.add(new int[] {numberOfGate, numberOfChannel, numberOfUnknownChannel, Integer.MAX_VALUE});
				}
			}
		}
		return parameters;
	}

	@Test
	public void testMultiInputGateMultiThreadWithInadequatePartitionRequestQuota() throws Exception {
		List<int[]> parameters = generateParameters();
		for (int [] parameter: parameters) {
			runTestMultiInputGateMultiThreadWithInadequatePartitionRequestQuota(parameter);
		}
	}

	private void runTestMultiInputGateMultiThreadWithInadequatePartitionRequestQuota(final int[] parameter) throws Exception {
		final ResultSubpartitionView iterator = mock(ResultSubpartitionView.class);

		final ResultPartitionManager partitionManager = mock(ResultPartitionManager.class);
		when(partitionManager.createSubpartitionView(
			any(ResultPartitionID.class),
			anyInt(),
			any(BufferAvailabilityListener.class))).thenReturn(iterator);

		PartitionRequestClient partitionRequestClient = mock(PartitionRequestClient.class);
		ConnectionManager connectionManager = mock(ConnectionManager.class);
		when(connectionManager.createPartitionRequestClient(any(ConnectionID.class))).thenReturn(partitionRequestClient);

		NetworkBufferPool networkBufferPool = new NetworkBufferPool(1000, 32);

		int buffersPerChannel = 2;
		int extraNetworkBuffersPerGate = 8;
		int buffersPerBlockingChannel = 128;
		int extraNetworkBuffersPerBlockingGate = 0;
		int buffersPerSubpartition = 2;
		final NetworkEnvironment network = createNetworkEnvironment(networkBufferPool, partitionManager, connectionManager,
			buffersPerChannel, extraNetworkBuffersPerGate, buffersPerBlockingChannel, extraNetworkBuffersPerBlockingGate, buffersPerSubpartition, 0, 0);

		PartitionRequestManager partitionRequestManager = new PartitionRequestManager(
			parameter[3], parameter[0]);

		List<SingleInputGate> inputGates = new ArrayList<>(parameter[0]);
		Map<InputChannel, SingleInputGate> requestedChannels = new ConcurrentHashMap<>();
		Map<InputChannel, Tuple2<SingleInputGate,ResultPartitionID>> unknownChannels = new ConcurrentHashMap<>();

		Optional endOfPartitionEvent = Optional.of(
			new InputChannel.BufferAndAvailability(
				EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE), false, 0));

		final ConnectionID connectionId = new ConnectionID(new InetSocketAddress("localhost", 5000), 0);

		final int numberOfChannels = parameter[0] * parameter[1];
		for (int i = 0; i < parameter[0]; ++i) {
			InputChannel[] channels = new InputChannel[parameter[1]];
			final SingleInputGate inputGate = createInputGate(
				parameter[1],
				ResultPartitionType.BLOCKING,
				network,
				partitionRequestManager);
			inputGates.add(inputGate);
			for (int j = 0; j < parameter[1] - parameter[2]; ++j) {
				ResultPartitionID resultPartitionID = new ResultPartitionID(new IntermediateResultPartitionID(), new ExecutionAttemptID());
				InputChannel remoteInputChannel = mock(RemoteInputChannel.class);
				when(remoteInputChannel.getNextBuffer()).thenReturn(endOfPartitionEvent);
				int finalJ = j;
				doAnswer(new Answer<Void>() {
					@Override
					public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
						partitionRequestClient.requestSubpartition(
							resultPartitionID,
							0,
							(RemoteInputChannel) remoteInputChannel,
							0);
						inputGate.notifyChannelNonEmpty(remoteInputChannel);
						requestedChannels.put(remoteInputChannel, inputGate);
						return null;
					}
				}).when(remoteInputChannel).requestSubpartition(anyInt());
				inputGate.setInputChannel(resultPartitionID.getPartitionId(), remoteInputChannel);
				when(remoteInputChannel.getChannelIndex()).thenReturn(finalJ);
				channels[finalJ] = remoteInputChannel;
			}

			for (int j = 0; j < parameter[2]; ++j) {
				ResultPartitionID resultPartitionID = new ResultPartitionID(new IntermediateResultPartitionID(), new ExecutionAttemptID());
				InputChannel remoteInputChannel = mock(RemoteInputChannel.class);
				when(remoteInputChannel.getNextBuffer()).thenReturn(endOfPartitionEvent);
				int finalJ = j + parameter[1] - parameter[2];
				doAnswer(new Answer<Void>() {
					@Override
					public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
						partitionRequestClient.requestSubpartition(
							resultPartitionID,
							0,
							(RemoteInputChannel) remoteInputChannel,
							0);
						inputGate.notifyChannelNonEmpty(remoteInputChannel);
						requestedChannels.put(remoteInputChannel, inputGate);
						return null;
					}
				}).when(remoteInputChannel).requestSubpartition(anyInt());
				when(remoteInputChannel.getChannelIndex()).thenReturn(finalJ);

				InputChannel unknownInputChannel = mock(UnknownInputChannel.class);
				when(((UnknownInputChannel) unknownInputChannel).toRemoteInputChannel(any(ConnectionID.class))).thenReturn((RemoteInputChannel) remoteInputChannel);
				inputGate.setInputChannel(resultPartitionID.getPartitionId(), unknownInputChannel);
				unknownChannels.put(unknownInputChannel, new Tuple2(inputGate, resultPartitionID));
				channels[finalJ] = unknownInputChannel;
			}
		}

		Collections.shuffle(inputGates);

		Thread taskThread = new Thread(new Runnable() {
			@Override
			public void run() {
				for (SingleInputGate inputGate: inputGates) {
					try {
						inputGate.requestPartitions();
					} catch (IOException e) {
						fail();
					} catch (InterruptedException e) {
						fail();
					}
				}
				int count = 0;
				while (count < numberOfChannels) {
					for (InputChannel inputChannel: requestedChannels.keySet()) {
						SingleInputGate singleInputGate = requestedChannels.remove(inputChannel);
						++count;
						try {
							singleInputGate.getNextBufferOrEvent();
						} catch (IOException e) {
							fail();
						} catch (InterruptedException e) {
							fail();
						}
					}
				}
			}
		});
		taskThread.start();

		Thread rpcThread = new Thread(new Runnable() {
			@Override
			public void run() {
				for (InputChannel unknownChannel: unknownChannels.keySet()) {
					try {
						unknownChannels.get(unknownChannel).f0.updateInputChannel(
							new InputChannelDeploymentDescriptor(unknownChannels.get(unknownChannel).f1,
								ResultPartitionLocation.createRemote(connectionId)));
					} catch (IOException e) {
						fail();
					} catch (InterruptedException e) {
						fail();
					}
				}
			}
		});
		rpcThread.start();

		taskThread.join();
		rpcThread.join();

		verify(partitionRequestClient, times(numberOfChannels)).
			requestSubpartition(any(ResultPartitionID.class), anyInt(), any(RemoteInputChannel.class), anyInt());
}

	private NetworkEnvironment createNetworkEnvironment(
		NetworkBufferPool networkBufferPool,
		ResultPartitionManager resultPartitionManager,
		ConnectionManager connectionManager,
		int buffersPerChannel,
		int extraNetworkBuffersPerGate,
		int buffersPerBlockingChannel,
		int extraNetworkBuffersPerBlockingGate,
		int buffersPerSubpartition,
		int initialBackoff,
		int maxBackoff) {
		return new NetworkEnvironment(
			networkBufferPool,
			connectionManager,
			resultPartitionManager,
			new TaskEventDispatcher(),
			new KvStateRegistry(),
			null,
			null,
			IOManager.IOMode.SYNC,
			initialBackoff,
			maxBackoff,
			buffersPerChannel,
			extraNetworkBuffersPerGate,
			buffersPerBlockingChannel,
			extraNetworkBuffersPerBlockingGate,
			buffersPerSubpartition,
			true);
	}

	private SingleInputGate createInputGate(
		NetworkEnvironment networkEnvironment,
		PartitionRequestManager partitionRequestManager) throws IOException {
		return createInputGate(3, networkEnvironment, partitionRequestManager);
	}

	private SingleInputGate createInputGate(
		int numberOfInputChannels,
		NetworkEnvironment networkEnvironment,
		PartitionRequestManager partitionRequestManager) throws IOException {
		return createInputGate(
			numberOfInputChannels,
			ResultPartitionType.BLOCKING,
			networkEnvironment,
			partitionRequestManager);
	}

	private SingleInputGate createInputGate(
		int numberOfInputChannels,
		ResultPartitionType partitionType,
		NetworkEnvironment networkEnvironment,
		PartitionRequestManager partitionRequestManager) throws IOException {
		SingleInputGate inputGate = new SingleInputGate(
			"Test Task Name",
			new JobID(),
			new IntermediateDataSetID(),
			partitionType,
			0,
			numberOfInputChannels,
			mock(TaskActions.class),
			UnregisteredMetricGroups.createUnregisteredTaskMetricGroup().getIOMetricGroup(),
			partitionRequestManager,
			Executors.newSingleThreadExecutor(),
			true,
			true);

		inputGate.setNetworkProperties(networkEnvironment.getNetworkBufferPool(), 2);
		assertEquals(partitionType, inputGate.getConsumedPartitionType());

		return inputGate;
	}

	private InputChannel addUnknownInputChannel(
		NetworkEnvironment network,
		SingleInputGate inputGate,
		ResultPartitionID partitionId,
		int channelIndex) {
		UnknownInputChannel unknown =
			createUnknownInputChannel(network, inputGate, partitionId, channelIndex);
		inputGate.setInputChannel(partitionId.getPartitionId(), unknown);
		return unknown;
	}

	private UnknownInputChannel createUnknownInputChannel(
		NetworkEnvironment network,
		SingleInputGate inputGate,
		ResultPartitionID partitionId,
		int channelIndex) {
		return new UnknownInputChannel(
			inputGate,
			channelIndex,
			partitionId,
			network.getResultPartitionManager(),
			network.getTaskEventDispatcher(),
			network.getConnectionManager(),
			network.getPartitionRequestInitialBackoff(),
			network.getPartitionRequestMaxBackoff(),
			UnregisteredMetricGroups.createUnregisteredTaskMetricGroup().getIOMetricGroup()
		);
	}

	private InputChannel addRemoteInputChannel(
		NetworkEnvironment network,
		SingleInputGate inputGate,
		ConnectionID connectionId,
		ResultPartitionID partitionId,
		int channelIndex) {
		RemoteInputChannel remote =
			createUnknownInputChannel(network, inputGate, partitionId, channelIndex)
				.toRemoteInputChannel(connectionId);
		return remote;
	}

	private InputChannel addLocalInputChannel(
		NetworkEnvironment network,
		SingleInputGate inputGate,
		ResultPartitionID partitionId,
		int channelIndex) {
		LocalInputChannel local =
			createUnknownInputChannel(network, inputGate, partitionId, channelIndex)
				.toLocalInputChannel();
		return local;
	}
}
