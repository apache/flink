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

package org.apache.flink.runtime.io.network.api.reader;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.execution.RuntimeEnvironment;
import org.apache.flink.runtime.io.network.MockNetworkEnvironment;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.EndOfSuperstepEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.OngoingStubbing;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockBufferReader {

	protected final BufferReader reader;

	protected final InputChannel inputChannel = mock(InputChannel.class);

	// Abusing Mockito here... ;)
	protected OngoingStubbing<Buffer> stubbing;

	public MockBufferReader() throws IOException {
		reader = new BufferReader(mock(RuntimeEnvironment.class), MockNetworkEnvironment.getMock(), new IntermediateDataSetID(), 1, 0);
		reader.setInputChannel(new IntermediateResultPartitionID(), inputChannel);
	}

	MockBufferReader read(Buffer buffer) throws IOException {
		if (stubbing == null) {
			stubbing = when(inputChannel.getNextBuffer()).thenReturn(buffer);
		}
		else {
			stubbing = stubbing.thenReturn(buffer);
		}

		reader.onAvailableInputChannel(inputChannel);

		return this;
	}

	MockBufferReader readBuffer() throws IOException {
		final Buffer buffer = mock(Buffer.class);
		when(buffer.isBuffer()).thenReturn(true);

		return read(buffer);
	}

	MockBufferReader readEvent() throws IOException {
		return read(EventSerializer.toBuffer(new TestTaskEvent()));
	}

	MockBufferReader finishSuperstep() throws IOException {
		return read(EventSerializer.toBuffer(EndOfSuperstepEvent.INSTANCE));
	}

	MockBufferReader finish() throws IOException {
		final Answer<Buffer> answer = new Answer<Buffer>() {
			@Override
			public Buffer answer(InvocationOnMock invocationOnMock) throws Throwable {
				// Return true after finishing
				when(inputChannel.isReleased()).thenReturn(true);

				return EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE);
			}
		};

		if (stubbing == null) {
			stubbing = when(inputChannel.getNextBuffer()).thenAnswer(answer);
		}
		else {
			stubbing = stubbing.thenAnswer(answer);
		}

		reader.onAvailableInputChannel(inputChannel);

		return this;
	}

	public BufferReader getMock() {
		return reader;
	}

	// ------------------------------------------------------------------------

	public static class TestTaskEvent extends TaskEvent {

		public TestTaskEvent() {
		}

		@Override
		public void write(DataOutputView out) throws IOException {
		}

		@Override
		public void read(DataInputView in) throws IOException {
		}
	}
}
