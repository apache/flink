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

import com.google.common.collect.Lists;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.netty.PartitionRequestClient;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.util.TestBufferFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RemoteInputChannelTest {

	@Test
	public void testExceptionOnReordering() throws Exception {
		// Setup
		final SingleInputGate inputGate = mock(SingleInputGate.class);
		final RemoteInputChannel inputChannel = createRemoteInputChannel(inputGate);

		// The test
		inputChannel.onBuffer(TestBufferFactory.getMockBuffer(), 0);

		// This does not yet throw the exception, but sets the error at the channel.
		inputChannel.onBuffer(TestBufferFactory.getMockBuffer(), 29);

		try {
			inputChannel.getNextBuffer();

			fail("Did not throw expected exception after enqueuing an out-of-order buffer.");
		}
		catch (Exception expected) {
		}

		// Need to notify the input gate for the out-of-order buffer as well. Otherwise the
		// receiving task will not notice the error.
		verify(inputGate, times(2)).onAvailableBuffer(eq(inputChannel));
	}

	@Test
	public void testConcurrentOnBufferAndRelease() throws Exception {
		// Config
		// Repeatedly spawn two tasks: one to queue buffers and the other to release the channel
		// concurrently. We do this repeatedly to provoke races.
		final int numberOfRepetitions = 8192;

		// Setup
		final ExecutorService executor = Executors.newFixedThreadPool(2);

		try {
			// Test
			final SingleInputGate inputGate = mock(SingleInputGate.class);

			for (int i = 0; i < numberOfRepetitions; i++) {
				final RemoteInputChannel inputChannel = createRemoteInputChannel(inputGate);

				final Callable<Void> enqueueTask = new Callable<Void>() {
					@Override
					public Void call() throws Exception {
						while (true) {
							for (int j = 0; j < 128; j++) {
								inputChannel.onBuffer(TestBufferFactory.getMockBuffer(), j);
							}

							if (inputChannel.isReleased()) {
								return null;
							}
						}
					}
				};

				final Callable<Void> releaseTask = new Callable<Void>() {
					@Override
					public Void call() throws Exception {
						inputChannel.releaseAllResources();

						return null;
					}
				};

				// Submit tasks and wait to finish
				List<Future<Void>> results = Lists.newArrayListWithCapacity(2);

				results.add(executor.submit(enqueueTask));
				results.add(executor.submit(releaseTask));

				for (Future<Void> result : results) {
					result.get();
				}

				assertEquals("Resource leak during concurrent release and enqueue.",
						0, inputChannel.getNumberOfQueuedBuffers());
			}
		}
		finally {
			executor.shutdown();
		}
	}

	// ---------------------------------------------------------------------------------------------

	private RemoteInputChannel createRemoteInputChannel(SingleInputGate inputGate)
			throws IOException, InterruptedException {

		final ConnectionManager connectionManager = mock(ConnectionManager.class);
		when(connectionManager.createPartitionRequestClient(any(ConnectionID.class)))
				.thenReturn(mock(PartitionRequestClient.class));

		return new RemoteInputChannel(
				inputGate,
				0,
				new ResultPartitionID(),
				mock(ConnectionID.class),
				connectionManager);
	}
}
