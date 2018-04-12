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

import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.junit.Test;

import java.io.IOException;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class InputChannelTest {

	@Test
	public void testExponentialBackoff() throws Exception {
		InputChannel ch = createInputChannel(500, 4000);

		assertEquals(0, ch.getCurrentBackoff());

		assertTrue(ch.increaseBackoff());
		assertEquals(500, ch.getCurrentBackoff());

		assertTrue(ch.increaseBackoff());
		assertEquals(1000, ch.getCurrentBackoff());

		assertTrue(ch.increaseBackoff());
		assertEquals(2000, ch.getCurrentBackoff());

		assertTrue(ch.increaseBackoff());
		assertEquals(4000, ch.getCurrentBackoff());

		assertFalse(ch.increaseBackoff());
		assertEquals(4000, ch.getCurrentBackoff());
	}

	@Test
	public void testExponentialBackoffCappedAtMax() throws Exception {
		InputChannel ch = createInputChannel(500, 3000);

		assertEquals(0, ch.getCurrentBackoff());

		assertTrue(ch.increaseBackoff());
		assertEquals(500, ch.getCurrentBackoff());

		assertTrue(ch.increaseBackoff());
		assertEquals(1000, ch.getCurrentBackoff());

		assertTrue(ch.increaseBackoff());
		assertEquals(2000, ch.getCurrentBackoff());

		assertTrue(ch.increaseBackoff());
		assertEquals(3000, ch.getCurrentBackoff());

		assertFalse(ch.increaseBackoff());
		assertEquals(3000, ch.getCurrentBackoff());
	}

	@Test
	public void testExponentialBackoffSingle() throws Exception {
		InputChannel ch = createInputChannel(500, 500);

		assertEquals(0, ch.getCurrentBackoff());

		assertTrue(ch.increaseBackoff());
		assertEquals(500, ch.getCurrentBackoff());

		assertFalse(ch.increaseBackoff());
		assertEquals(500, ch.getCurrentBackoff());
	}

	@Test
	public void testExponentialNoBackoff() throws Exception {
		InputChannel ch = createInputChannel(0, 0);

		assertEquals(0, ch.getCurrentBackoff());

		assertFalse(ch.increaseBackoff());
		assertEquals(0, ch.getCurrentBackoff());
	}

	private InputChannel createInputChannel(int initialBackoff, int maxBackoff) {
		return new MockInputChannel(
			mock(SingleInputGate.class),
			0,
			new ResultPartitionID(),
			initialBackoff,
			maxBackoff);
	}

	// ---------------------------------------------------------------------------------------------

	private static class MockInputChannel extends InputChannel {

		private MockInputChannel(
			SingleInputGate inputGate,
			int channelIndex,
			ResultPartitionID partitionId,
			int initialBackoff,
			int maxBackoff) {

			super(inputGate, channelIndex, partitionId, initialBackoff, maxBackoff, new SimpleCounter());
		}

		@Override
		void requestSubpartition(int subpartitionIndex) throws IOException, InterruptedException {
		}

		@Override
		Optional<BufferAndAvailability> getNextBuffer() throws IOException, InterruptedException {
			return Optional.empty();
		}

		@Override
		void sendTaskEvent(TaskEvent event) throws IOException {
		}

		@Override
		boolean isReleased() {
			return false;
		}

		@Override
		void notifySubpartitionConsumed() throws IOException {
		}

		@Override
		void releaseAllResources() throws IOException {
		}
	}
}
