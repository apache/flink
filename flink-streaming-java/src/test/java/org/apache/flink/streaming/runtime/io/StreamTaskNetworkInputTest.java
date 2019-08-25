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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.RecordSerializer;
import org.apache.flink.runtime.io.network.api.serialization.SpanningRecordSerializer;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.StreamTestSingleInputGate;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link StreamTaskNetworkInput}.
 */
public class StreamTaskNetworkInputTest {

	private static final int PAGE_SIZE = 1000;

	private final IOManager ioManager = new IOManagerAsync();

	@After
	public void tearDown() throws Exception {
		ioManager.close();
	}

	@Test
	public void testIsAvailableWithBufferedDataInDeserializer() throws Exception {
		BufferBuilder bufferBuilder = BufferBuilderTestUtils.createEmptyBufferBuilder(PAGE_SIZE);

		serializeRecord(42L, bufferBuilder);
		serializeRecord(44L, bufferBuilder);

		Buffer buffer = bufferBuilder.createBufferConsumer().build();

		List<BufferOrEvent> buffers = Collections.singletonList(new BufferOrEvent(buffer, 0, false));

		StreamTaskNetworkInput input = new StreamTaskNetworkInput(
			new CheckpointedInputGate(
				new MockInputGate(1, buffers, false),
				new EmptyBufferStorage(),
				new CheckpointBarrierTracker(1)),
			LongSerializer.INSTANCE,
			ioManager,
			0);

		assertHasNextElement(input);
		assertHasNextElement(input);
	}

	@Test
	public void testReleasingDeserializerTimely()
		throws Exception {

		int numInputChannels = 2;
		LongSerializer inSerializer = LongSerializer.INSTANCE;
		StreamTestSingleInputGate inputGate = new StreamTestSingleInputGate<>(numInputChannels, 1024, inSerializer);

		TestRecordDeserializer[] deserializers = new TestRecordDeserializer[numInputChannels];
		for (int i = 0; i < deserializers.length; i++) {
			deserializers[i] = new TestRecordDeserializer(ioManager.getSpillingDirectoriesPaths());
		}

		TestRecordDeserializer[] copiedDeserializers = Arrays.copyOf(deserializers, deserializers.length);
		StreamTaskNetworkInput input = new StreamTaskNetworkInput(
			new CheckpointedInputGate(
				inputGate.getInputGate(),
				new EmptyBufferStorage(),
				new CheckpointBarrierTracker(1)),
			inSerializer,
			ioManager,
			0,
			deserializers);

		for (int i = 0; i < numInputChannels; i++) {
			assertNotNull(deserializers[i]);
			inputGate.sendEvent(EndOfPartitionEvent.INSTANCE, i);
			input.pollNextNullable();
			assertNull(deserializers[i]);
			assertTrue(copiedDeserializers[i].isCleared());
		}
	}

	private void serializeRecord(long value, BufferBuilder bufferBuilder) throws IOException {
		RecordSerializer<SerializationDelegate<StreamElement>> serializer = new SpanningRecordSerializer<>();
		SerializationDelegate<StreamElement> serializationDelegate =
			new SerializationDelegate<>(
				new StreamElementSerializer<>(LongSerializer.INSTANCE));
		serializationDelegate.setInstance(new StreamRecord<>(value));
		serializer.serializeRecord(serializationDelegate);

		assertFalse(serializer.copyToBufferBuilder(bufferBuilder).isFullBuffer());
	}

	private static void assertHasNextElement(StreamTaskNetworkInput input) throws Exception {
		assertTrue(input.isAvailable().isDone());
		StreamElement element = input.pollNextNullable();
		assertNotNull(element);
		assertTrue(element.isRecord());
	}

	private static class TestRecordDeserializer
		extends SpillingAdaptiveSpanningRecordDeserializer<DeserializationDelegate<StreamElement>> {

		private boolean cleared = false;

		public TestRecordDeserializer(String[] tmpDirectories) {
			super(tmpDirectories);
		}

		@Override
		public void clear() {
			cleared = true;
		}

		public boolean isCleared() {
			return cleared;
		}
	}
}
