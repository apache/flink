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

package org.apache.flink.runtime.io.network.util;

import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.EndOfSuperstepEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.OngoingStubbing;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A mocked input channel.
 */
public class MockInputChannel {

	private final InputChannel mock = Mockito.mock(InputChannel.class);

	private final SingleInputGate inputGate;

	// Abusing Mockito here... ;)
	protected OngoingStubbing<Buffer> stubbing;

	public MockInputChannel(SingleInputGate inputGate, int channelIndex) {
		checkArgument(channelIndex >= 0);
		this.inputGate = checkNotNull(inputGate);

		when(mock.getChannelIndex()).thenReturn(channelIndex);
	}

	public MockInputChannel read(Buffer buffer) throws IOException {
		if (stubbing == null) {
			stubbing = when(mock.getNextBuffer()).thenReturn(buffer);
		}
		else {
			stubbing = stubbing.thenReturn(buffer);
		}

		inputGate.onAvailableBuffer(mock);

		return this;
	}

	public MockInputChannel readBuffer() throws IOException {
		final Buffer buffer = mock(Buffer.class);
		when(buffer.isBuffer()).thenReturn(true);

		return read(buffer);
	}

	public MockInputChannel readEvent() throws IOException {
		return read(EventSerializer.toBuffer(new TestTaskEvent()));
	}

	public MockInputChannel readEndOfSuperstepEvent() throws IOException {
		return read(EventSerializer.toBuffer(EndOfSuperstepEvent.INSTANCE));
	}

	public MockInputChannel readEndOfPartitionEvent() throws IOException {
		final Answer<Buffer> answer = new Answer<Buffer>() {
			@Override
			public Buffer answer(InvocationOnMock invocationOnMock) throws Throwable {
				// Return true after finishing
				when(mock.isReleased()).thenReturn(true);

				return EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE);
			}
		};

		if (stubbing == null) {
			stubbing = when(mock.getNextBuffer()).thenAnswer(answer);
		}
		else {
			stubbing = stubbing.thenAnswer(answer);
		}

		inputGate.onAvailableBuffer(mock);

		return this;
	}

	public InputChannel getInputChannel() {
		return mock;
	}

	// ------------------------------------------------------------------------

	public static MockInputChannel[] createInputChannels(SingleInputGate inputGate, int numberOfInputChannels) {
		checkNotNull(inputGate);
		checkArgument(numberOfInputChannels > 0);

		MockInputChannel[] mocks = new MockInputChannel[numberOfInputChannels];

		for (int i = 0; i < numberOfInputChannels; i++) {
			mocks[i] = new MockInputChannel(inputGate, i);

			inputGate.setInputChannel(new IntermediateResultPartitionID(), mocks[i].getInputChannel());
		}

		return mocks;
	}
}
