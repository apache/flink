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

import org.apache.flink.runtime.io.AsyncDataInput;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test base for {@link InputGate}.
 */
@RunWith(Parameterized.class)
public abstract class InputGateTestBase {

	@Parameter
	public boolean enableCreditBasedFlowControl;

	@Parameters(name = "Credit-based = {0}")
	public static List<Boolean> parameters() {
		return Arrays.asList(Boolean.TRUE, Boolean.FALSE);
	}

	protected void testIsAvailable(
			InputGate inputGateToTest,
			SingleInputGate inputGateToNotify,
			TestInputChannel inputChannelWithNewData) throws Exception {

		assertFalse(inputGateToTest.isAvailable().isDone());
		assertFalse(inputGateToTest.pollNext().isPresent());

		CompletableFuture<?> isAvailable = inputGateToTest.isAvailable();

		assertFalse(inputGateToTest.isAvailable().isDone());
		assertFalse(inputGateToTest.pollNext().isPresent());

		assertEquals(isAvailable, inputGateToTest.isAvailable());

		inputChannelWithNewData.readBuffer();
		inputGateToNotify.notifyChannelNonEmpty(inputChannelWithNewData);

		assertTrue(isAvailable.isDone());
		assertTrue(inputGateToTest.isAvailable().isDone());
		assertEquals(AsyncDataInput.AVAILABLE, inputGateToTest.isAvailable());
	}

	protected void testIsAvailableAfterFinished(
		InputGate inputGateToTest,
		Runnable endOfPartitionEvent) throws Exception {

		CompletableFuture<?> available = inputGateToTest.isAvailable();
		assertFalse(available.isDone());
		assertFalse(inputGateToTest.pollNext().isPresent());

		endOfPartitionEvent.run();

		assertTrue(inputGateToTest.pollNext().isPresent()); // EndOfPartitionEvent

		assertTrue(available.isDone());
		assertTrue(inputGateToTest.isAvailable().isDone());
		assertEquals(AsyncDataInput.AVAILABLE, inputGateToTest.isAvailable());
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
			.setResultPartitionType(partitionType)
			.setIsCreditBased(enableCreditBasedFlowControl);

		if (environment != null) {
			builder = builder.setupBufferPoolFactory(environment);
		}

		SingleInputGate inputGate = builder.build();
		assertEquals(partitionType, inputGate.getConsumedPartitionType());
		return inputGate;
	}
}
