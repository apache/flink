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

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordSerializer;
import org.apache.flink.runtime.io.network.api.serialization.SpanningRecordSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.MutableObjectIterator;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkState;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockIteratorBufferReader<T extends IOReadableWritable> extends MockBufferReader {

	private final int bufferSize;

	private MutableObjectIterator<T> inputIterator;

	private RecordSerializer<T> serializer;

	private final T reuse;

	public MockIteratorBufferReader(int bufferSize, Class<T> recordType) throws IOException {
		this.bufferSize = bufferSize;

		this.reuse = InstantiationUtil.instantiate(recordType);
	}

	public MockIteratorBufferReader(int bufferSize, Class<T> recordType, MutableObjectIterator<T> iterator) throws IOException {
		this.bufferSize = bufferSize;

		this.reuse = InstantiationUtil.instantiate(recordType);

		wrapIterator(iterator);
	}

	public MockIteratorBufferReader<T> wrapIterator(MutableObjectIterator<T> iterator) throws IOException {
		checkState(inputIterator == null, "Iterator has already been set.");
		checkState(stubbing == null, "There is already an ongoing stubbing from the MockBufferReader, which can't be mixed with an Iterator.");

		inputIterator = iterator;
		serializer = new SpanningRecordSerializer<T>();

		// The input iterator can produce an infinite stream. That's why we have to serialize each
		// record on demand and cannot do it upfront.
		final Answer<Buffer> answer = new Answer<Buffer>() {
			@Override
			public Buffer answer(InvocationOnMock invocationOnMock) throws Throwable {
				if (inputIterator.next(reuse) != null) {
					final Buffer buffer = new Buffer(new MemorySegment(new byte[bufferSize]), mock(BufferRecycler.class));
					serializer.setNextBuffer(buffer);
					serializer.addRecord(reuse);

					reader.onAvailableInputChannel(inputChannel);

					// Call getCurrentBuffer to ensure size is set
					return serializer.getCurrentBuffer();
				}
				else {
					// Return true after finishing
					when(inputChannel.isReleased()).thenReturn(true);

					return EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE);
				}
			}
		};

		stubbing = when(inputChannel.getNextBuffer()).thenAnswer(answer);

		return this;
	}

	public MockIteratorBufferReader<T> read() {
		checkState(inputIterator != null && serializer != null, "Iterator/serializer has not been set. Call wrapIterator() first.");

		reader.onAvailableInputChannel(inputChannel);

		return this;
	}
}
