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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.PartitionRequestClient;
import org.apache.flink.runtime.io.network.metrics.InputChannelMetrics;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateBuilder;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collection;
import java.util.Collections;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Some utility methods used for testing InputChannels and InputGates.
 */
public class InputChannelTestUtils {
	/**
	 * Creates a result partition manager that ignores all IDs, and simply returns the given
	 * subpartitions in sequence.
	 */
	public static ResultPartitionManager createResultPartitionManager(final ResultSubpartition[] sources) throws Exception {

		final Answer<ResultSubpartitionView> viewCreator = new Answer<ResultSubpartitionView>() {

			private int num = 0;

			@Override
			public ResultSubpartitionView answer(InvocationOnMock invocation) throws Throwable {
				BufferAvailabilityListener channel = (BufferAvailabilityListener) invocation.getArguments()[2];
				return sources[num++].createReadView(channel);
			}
		};

		ResultPartitionManager manager = mock(ResultPartitionManager.class);
		when(manager.createSubpartitionView(
				any(ResultPartitionID.class), anyInt(), any(BufferAvailabilityListener.class)))
				.thenAnswer(viewCreator);

		return manager;
	}

	public static SingleInputGate createSingleInputGate(int numberOfChannels) {
		return new SingleInputGateBuilder().setNumberOfChannels(numberOfChannels).build();
	}

	public static SingleInputGate createSingleInputGate(int numberOfChannels, MemorySegmentProvider segmentProvider) {
		return new SingleInputGateBuilder()
			.setNumberOfChannels(numberOfChannels)
			.setSegmentProvider(segmentProvider)
			.build();
	}

	public static ConnectionManager createDummyConnectionManager() throws Exception {
		final PartitionRequestClient mockClient = mock(PartitionRequestClient.class);

		final ConnectionManager connManager = mock(ConnectionManager.class);
		when(connManager.createPartitionRequestClient(any(ConnectionID.class))).thenReturn(mockClient);

		return connManager;
	}

	public static LocalInputChannel createLocalInputChannel(
		SingleInputGate inputGate,
		ResultPartitionManager partitionManager) {

		return createLocalInputChannel(inputGate, partitionManager, 0, 0);
	}

	public static LocalInputChannel createLocalInputChannel(
		SingleInputGate inputGate,
		int channelIndex,
		ResultPartitionManager partitionManager) {

		return InputChannelBuilder.newBuilder()
			.setChannelIndex(channelIndex)
			.setPartitionManager(partitionManager)
			.buildLocalChannel(inputGate);
	}

	public static LocalInputChannel createLocalInputChannel(
		SingleInputGate inputGate,
		ResultPartitionManager partitionManager,
		int initialBackoff,
		int maxBackoff) {

		return InputChannelBuilder.newBuilder()
			.setPartitionManager(partitionManager)
			.setInitialBackoff(initialBackoff)
			.setMaxBackoff(maxBackoff)
			.buildLocalChannel(inputGate);
	}

	public static RemoteInputChannel createRemoteInputChannel(
		SingleInputGate inputGate,
		int channelIndex,
		ConnectionManager connectionManager) {

		return InputChannelBuilder.newBuilder()
			.setChannelIndex(channelIndex)
			.setConnectionManager(connectionManager)
			.buildRemoteChannel(inputGate);
	}

	public static RemoteInputChannel createRemoteInputChannel(
		SingleInputGate inputGate,
		PartitionRequestClient client) {

		return InputChannelBuilder.newBuilder()
			.setConnectionManager(mockConnectionManagerWithPartitionRequestClient(client))
			.buildRemoteChannel(inputGate);
	}

	public static ConnectionManager mockConnectionManagerWithPartitionRequestClient(PartitionRequestClient client) {
		return new ConnectionManager() {
			@Override
			public int start() {
				return -1;
			}

			@Override
			public PartitionRequestClient createPartitionRequestClient(ConnectionID connectionId) {
				return client;
			}

			@Override
			public void closeOpenChannelConnections(ConnectionID connectionId) {
			}

			@Override
			public int getNumberOfActiveConnections() {
				return 0;
			}

			@Override
			public void shutdown() {
			}
		};
	}

	public static InputChannelMetrics newUnregisteredInputChannelMetrics() {
		return new InputChannelMetrics(UnregisteredMetricGroups.createUnregisteredTaskMetricGroup().getIOMetricGroup());
	}

	// ------------------------------------------------------------------------

	/** This class is not meant to be instantiated. */
	private InputChannelTestUtils() {}

	/**
	 * Test stub for {@link MemorySegmentProvider}.
	 */
	public static class StubMemorySegmentProvider implements MemorySegmentProvider {
		private static final MemorySegmentProvider INSTANCE = new StubMemorySegmentProvider();

		public static MemorySegmentProvider getInstance() {
			return INSTANCE;
		}

		private StubMemorySegmentProvider() {
		}

		@Override
		public Collection<MemorySegment> requestMemorySegments() {
			return Collections.emptyList();
		}

		@Override
		public void recycleMemorySegments(Collection<MemorySegment> segments) {
		}
	}

	/**
	 * {@link MemorySegmentProvider} that provides unpooled {@link MemorySegment}s.
	 */
	public static class UnpooledMemorySegmentProvider implements MemorySegmentProvider {
		private final int pageSize;

		public UnpooledMemorySegmentProvider(int pageSize) {
			this.pageSize = pageSize;
		}

		@Override
		public Collection<MemorySegment> requestMemorySegments() {
			return Collections.singletonList(MemorySegmentFactory.allocateUnpooledSegment(pageSize));
		}

		@Override
		public void recycleMemorySegments(Collection<MemorySegment> segments) {
		}
	}
}
