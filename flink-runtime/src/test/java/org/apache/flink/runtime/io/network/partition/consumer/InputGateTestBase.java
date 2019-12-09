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

import org.apache.flink.runtime.io.PullingAsyncDataInput;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test base for {@link InputGate}.
 */
public abstract class InputGateTestBase {

	protected void testIsAvailable(
			InputGate inputGateToTest,
			SingleInputGate inputGateToNotify,
			TestInputChannel inputChannelWithNewData) throws Exception {

		assertFalse(inputGateToTest.getAvailableFuture().isDone());
		assertFalse(inputGateToTest.pollNext().isPresent());

		CompletableFuture<?> future = inputGateToTest.getAvailableFuture();

		assertFalse(inputGateToTest.getAvailableFuture().isDone());
		assertFalse(inputGateToTest.pollNext().isPresent());

		assertEquals(future, inputGateToTest.getAvailableFuture());

		inputChannelWithNewData.readBuffer();
		inputGateToNotify.notifyChannelNonEmpty(inputChannelWithNewData);

		assertTrue(future.isDone());
		assertTrue(inputGateToTest.getAvailableFuture().isDone());
		assertEquals(PullingAsyncDataInput.AVAILABLE, inputGateToTest.getAvailableFuture());
	}

	protected void testIsAvailableAfterFinished(
		InputGate inputGateToTest,
		Runnable endOfPartitionEvent) throws Exception {

		CompletableFuture<?> available = inputGateToTest.getAvailableFuture();
		assertFalse(available.isDone());
		assertFalse(inputGateToTest.pollNext().isPresent());

		endOfPartitionEvent.run();

		assertTrue(inputGateToTest.pollNext().isPresent()); // EndOfPartitionEvent

		assertTrue(available.isDone());
		assertTrue(inputGateToTest.getAvailableFuture().isDone());
		assertEquals(PullingAsyncDataInput.AVAILABLE, inputGateToTest.getAvailableFuture());
	}

	protected SingleInputGate createInputGate() {
		return createInputGate(2);
	}

	protected SingleInputGate createInputGate(int numberOfInputChannels) {
		return createInputGate(null, numberOfInputChannels, ResultPartitionType.PIPELINED);
	}

	protected SingleInputGate createInputGate(
		NettyShuffleEnvironment environment, int numberOfInputChannels, ResultPartitionType partitionType) {

		SingleInputGateBuilder builder = new SingleInputGateBuilder()
			.setNumberOfChannels(numberOfInputChannels)
			.setResultPartitionType(partitionType);

		if (environment != null) {
			builder = builder.setupBufferPoolFactory(environment);
		}

		SingleInputGate inputGate = builder.build();
		assertEquals(partitionType, inputGate.getConsumedPartitionType());
		return inputGate;
	}
}
