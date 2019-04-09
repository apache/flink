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

package org.apache.flink.streaming.runtime.io.benchmark;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.api.serialization.RecordSerializer;
import org.apache.flink.runtime.io.network.api.serialization.SpanningRecordSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.MathUtils.checkedDownCast;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This class provides {@link Buffer}-related utility methods.
 */
public final class BufferUtil {

	private static final int BUFFER_CAPACITY = checkedDownCast(
		MemorySize.parse(new Configuration().getString(TaskManagerOptions.MEMORY_SEGMENT_SIZE)).getBytes());

	public static <T extends IOReadableWritable> List<Buffer> createNetworkBuffers(long numRecords, T fillingRecord)
		throws IOException {

		final List<Buffer> filledBuffers = new ArrayList<>();

		final RecordSerializer<T> serializer = new SpanningRecordSerializer<>();
		serializer.serializeRecord(fillingRecord);

		BufferBuilder bufferBuilder = requestNewBufferBuilder();
		for (int i = 0; i < numRecords; i++) {
			serializer.reset();

			RecordSerializer.SerializationResult result = serializer.copyToBufferBuilder(bufferBuilder);
			while (!result.isFullRecord()) {
				checkState(result.isFullBuffer());

				bufferBuilder.finish();
				filledBuffers.add(bufferBuilder.createBufferConsumer().build());

				bufferBuilder = requestNewBufferBuilder();
				result = serializer.copyToBufferBuilder(bufferBuilder);
			}
		}

		bufferBuilder.finish();
		filledBuffers.add(bufferBuilder.createBufferConsumer().build());

		serializer.prune();

		return filledBuffers;
	}

	private static BufferBuilder requestNewBufferBuilder() {
		byte[] bytes = new byte[BUFFER_CAPACITY];
		NetworkBuffer buffer = new NetworkBuffer(MemorySegmentFactory.wrap(bytes), noOpBufferRecycler -> {});
		MemorySegment memorySegment = MemorySegmentFactory.wrap(bytes);

		return new BufferBuilder(memorySegment, buffer.getRecycler());
	}
}
