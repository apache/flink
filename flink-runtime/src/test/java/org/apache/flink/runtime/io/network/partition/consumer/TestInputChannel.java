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

import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.OngoingStubbing;

import java.io.IOException;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A mocked input channel.
 */
public class TestInputChannel {

	private final InputChannel mock = Mockito.mock(InputChannel.class);

	private final SingleInputGate inputGate;

	// Abusing Mockito here... ;)
	protected OngoingStubbing<Optional<BufferAndAvailability>> stubbing;

	public TestInputChannel(SingleInputGate inputGate, int channelIndex) {
		checkArgument(channelIndex >= 0);
		this.inputGate = checkNotNull(inputGate);

		when(mock.getChannelIndex()).thenReturn(channelIndex);
	}

	public TestInputChannel read(Buffer buffer) throws IOException, InterruptedException {
		return read(buffer, true);
	}

	public TestInputChannel read(Buffer buffer, boolean moreAvailable) throws IOException, InterruptedException {
		if (stubbing == null) {
			stubbing = when(mock.getNextBuffer()).thenReturn(Optional.of(new BufferAndAvailability(buffer, moreAvailable, 0)));
		} else {
			stubbing = stubbing.thenReturn(Optional.of(new BufferAndAvailability(buffer, moreAvailable, 0)));
		}

		return this;
	}

	public TestInputChannel readBuffer() throws IOException, InterruptedException {
		return readBuffer(true);
	}

	public TestInputChannel readBuffer(boolean moreAvailable) throws IOException, InterruptedException {
		final Buffer buffer = mock(Buffer.class);
		when(buffer.isBuffer()).thenReturn(true);

		return read(buffer, moreAvailable);
	}

	public TestInputChannel readEndOfPartitionEvent() throws IOException, InterruptedException {
		final Answer<Optional<BufferAndAvailability>> answer = new Answer<Optional<BufferAndAvailability>>() {
			@Override
			public Optional<BufferAndAvailability> answer(InvocationOnMock invocationOnMock) throws Throwable {
				// Return true after finishing
				when(mock.isReleased()).thenReturn(true);

				return Optional.of(new BufferAndAvailability(EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE), false, 0));
			}
		};

		if (stubbing == null) {
			stubbing = when(mock.getNextBuffer()).thenAnswer(answer);
		} else {
			stubbing = stubbing.thenAnswer(answer);
		}

		return this;
	}

	public InputChannel getInputChannel() {
		return mock;
	}

	// ------------------------------------------------------------------------

	/**
	 * Creates test input channels and attaches them to the specified input gate.
	 *
	 * @return The created test input channels.
	 */
	public static TestInputChannel[] createInputChannels(SingleInputGate inputGate, int numberOfInputChannels) {
		checkNotNull(inputGate);
		checkArgument(numberOfInputChannels > 0);

		TestInputChannel[] mocks = new TestInputChannel[numberOfInputChannels];

		for (int i = 0; i < numberOfInputChannels; i++) {
			mocks[i] = new TestInputChannel(inputGate, i);

			inputGate.setInputChannel(new IntermediateResultPartitionID(), mocks[i].getInputChannel());
		}

		return mocks;
	}
}
