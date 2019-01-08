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

import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.netty.PartitionRequestClient;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

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
	
	public static ConnectionManager createDummyConnectionManager() throws Exception {
		final PartitionRequestClient mockClient = mock(PartitionRequestClient.class);

		final ConnectionManager connManager = mock(ConnectionManager.class);
		when(connManager.createPartitionRequestClient(any(ConnectionID.class))).thenReturn(mockClient);

		return connManager;
	}

	// ------------------------------------------------------------------------

	/** This class is not meant to be instantiated */
	private InputChannelTestUtils() {}
}
