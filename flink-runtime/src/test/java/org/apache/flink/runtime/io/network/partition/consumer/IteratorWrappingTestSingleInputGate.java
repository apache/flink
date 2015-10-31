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

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordSerializer;
import org.apache.flink.runtime.io.network.api.serialization.SpanningRecordSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.MutableObjectIterator;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IteratorWrappingTestSingleInputGate<T extends IOReadableWritable> extends TestSingleInputGate {

	private final TestInputChannel inputChannel = new TestInputChannel(inputGate, 0);

	private final int bufferSize;

	private MutableObjectIterator<T> inputIterator;

	private RecordSerializer<T> serializer;

	private final T reuse;

	public IteratorWrappingTestSingleInputGate(int bufferSize, Class<T> recordType, MutableObjectIterator<T> iterator) throws IOException, InterruptedException {
		super(1, false);

		this.bufferSize = bufferSize;
		this.reuse = InstantiationUtil.instantiate(recordType);

		wrapIterator(iterator);
	}

	private IteratorWrappingTestSingleInputGate<T> wrapIterator(MutableObjectIterator<T> iterator) throws IOException, InterruptedException {
		inputIterator = iterator;
		serializer = new SpanningRecordSerializer<T>();

		// The input iterator can produce an infinite stream. That's why we have to serialize each
		// record on demand and cannot do it upfront.
		final Answer<Buffer> answer = new Answer<Buffer>() {
			@Override
			public Buffer answer(InvocationOnMock invocationOnMock) throws Throwable {
				if (inputIterator.next(reuse) != null) {
					final Buffer buffer = new Buffer(MemorySegmentFactory.allocateUnpooledSegment(bufferSize), mock(BufferRecycler.class));
					serializer.setNextBuffer(buffer);
					serializer.addRecord(reuse);

					inputGate.onAvailableBuffer(inputChannel.getInputChannel());

					// Call getCurrentBuffer to ensure size is set
					return serializer.getCurrentBuffer();
				}
				else {

					when(inputChannel.getInputChannel().isReleased()).thenReturn(true);

					return EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE);
				}
			}
		};

		when(inputChannel.getInputChannel().getNextBuffer()).thenAnswer(answer);

		inputGate.setInputChannel(new IntermediateResultPartitionID(), inputChannel.getInputChannel());

		return this;
	}

	public IteratorWrappingTestSingleInputGate<T> read() {
		inputGate.onAvailableBuffer(inputChannel.getInputChannel());

		return this;
	}
}
