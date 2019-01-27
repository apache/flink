/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.HeapMemorySegment;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.UnionInputGate;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.InputSelector;

import org.junit.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import static org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult.INTERMEDIATE_RECORD_FROM_BUFFER;
import static org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult.LAST_RECORD_FROM_BUFFER;
import static org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult.PARTIAL_RECORD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test for {@link InputGateFetcher}.
 */
public class InputGateFetcherTest {

	@Test
	public void testGetNextBufferOrEvent() throws Exception {
		// This should never be read.
		final List<BufferOrEvent> bufferOrEvents1 = new ArrayList<>();
		bufferOrEvents1.add(new BufferOrEvent(mock(Buffer.class), 8));

		final List<BufferOrEvent> bufferOrEvents2 = new ArrayList<>();
		final BufferOrEvent bufferOrEvent = new BufferOrEvent(mock(Buffer.class), 1);
		final BufferOrEvent eop1 = new BufferOrEvent(EndOfPartitionEvent.INSTANCE, 3);
		eop1.setMoreAvailable(false);
		final BufferOrEvent eop2 = new BufferOrEvent(EndOfPartitionEvent.INSTANCE, 2);
		eop2.setMoreAvailable(false);
		bufferOrEvents2.add(eop1);
		bufferOrEvents2.add(bufferOrEvent);
		// This EOP would trigger end input.
		bufferOrEvents2.add(eop2);

		final InputGate inputGate1 = new MockInputGate(32 * 1024, 16, bufferOrEvents1);
		final InputGate inputGate2 = new MockInputGate(32 * 1024, 4, bufferOrEvents2);

		final UnionInputGate unionInputGate = new UnionInputGate(inputGate1, inputGate2);
		final InputProcessor inputProcessor = mock(InputProcessor.class);
		final InputGateFetcher<Integer> fetcher = new InputGateFetcher<>(
			mock(InputSelector.InputSelection.class),
			inputGate2,
			new IntSerializer(),
			new BarrierTracker(unionInputGate),
			mock(IOManager.class),
			inputProcessor,
			this,
			inputGate1.getNumberOfInputChannels(),
			true,
			new Configuration());

		// The first EOP would be ignored
		assertEquals(bufferOrEvent, fetcher.getNextBufferOrEvent());
		verify(inputProcessor, never()).endInput();
		// The other EOP
		assertNull(fetcher.getNextBufferOrEvent());
		verify(inputProcessor, times(1)).endInput();
		// There is no data left
		assertNull(fetcher.getNextBufferOrEvent());
		// Current channel index should be same with last buffer channel index
		assertEquals(1, fetcher.getCurrentChannelIndex());
	}

	@Test
	public void testGetNextResult() throws Exception {
		// Prepare serialized buffer
		final DataOutputSerializer dataOutputSerializer = new DataOutputSerializer(1024);

		final IntSerializer serializer = new IntSerializer();
		final StreamElementSerializer<Integer> elementSerializer = new StreamElementSerializer<>(serializer);
		// Write the first record(1024) to a temporary buffer
		final DataOutputSerializer dataOutputSerializer1 = new DataOutputSerializer(1024);
		elementSerializer.serialize(new StreamRecord<>(1024), dataOutputSerializer1);
		serializer.serialize(dataOutputSerializer1.length(), dataOutputSerializer);
		// Write the first record length and content
		dataOutputSerializer.write(dataOutputSerializer1.getSharedBuffer(), 0, dataOutputSerializer1.length());

		// Write the second record(9527) to a temporary buffer
		final DataOutputSerializer dataOutputSerializer2 = new DataOutputSerializer(1024);
		elementSerializer.serialize(new StreamRecord<>(9527), dataOutputSerializer2);
		serializer.serialize(dataOutputSerializer2.length(), dataOutputSerializer);
		// Write the second record length and content
		dataOutputSerializer.write(dataOutputSerializer2.getSharedBuffer(), 0, dataOutputSerializer2.length());
		// Write a large length of the third record, there would be no enough space in the read buffer for this record(partial record)
		serializer.serialize(102400, dataOutputSerializer);

		final List<BufferOrEvent> bufferOrEvents = new ArrayList<>();
		final BufferRecycler recycler = mock(BufferRecycler.class);
		final Buffer buffer = new NetworkBuffer(HeapMemorySegment.FACTORY.wrap(dataOutputSerializer.getSharedBuffer()), recycler, true, 1024);
		final BufferOrEvent bufferOrEvent = new BufferOrEvent(buffer, 7);
		bufferOrEvents.add(bufferOrEvent);
		final InputGate inputGate = new MockInputGate(32 * 1024, 16, bufferOrEvents);
		final InputGateFetcher<Integer> fetcher = new InputGateFetcher<>(
			mock(InputSelector.InputSelection.class),
			inputGate,
			new IntSerializer(),
			new BarrierTracker(inputGate),
			mock(IOManager.class),
			mock(InputProcessor.class),
			this,
			4,
			true,
			new Configuration());

		// Read from the input gate
		fetcher.setCurrentRecordDeserializer(null);
		final DeserializationResult result1 = fetcher.getNextResult();
		assertTrue(result1.isFullRecord());
		assertNotNull(fetcher.getCurrentRecordDeserializer());
		assertEquals(3, fetcher.getCurrentChannelIndex());
		final StreamElement streamElement1 = fetcher.getDeserializationDelegate().getInstance();
		assertTrue(streamElement1.isRecord());
		assertEquals(1024, streamElement1.asRecord().getValue());

		final DeserializationResult result2 = fetcher.getNextResult();
		assertTrue(result2.isFullRecord());
		assertNotNull(fetcher.getCurrentRecordDeserializer());
		assertEquals(3, fetcher.getCurrentChannelIndex());
		final StreamElement streamElement2 = fetcher.getDeserializationDelegate().getInstance();
		assertTrue(streamElement2.isRecord());
		assertEquals(9527, streamElement2.asRecord().getValue());

		// There is no enough space, it's a partial record
		final DeserializationResult result3 = fetcher.getNextResult();
		assertNotNull(result3);
		assertFalse(result3.isFullRecord());

		verify(recycler, times(1)).recycle(any(MemorySegment.class));
	}

	@Test
	public void testGetNextResultObjectReuse() throws Exception {
		// Prepare serialized buffer
		final DataOutputSerializer dataOutputSerializer = new DataOutputSerializer(1024);

		final Tuple2<Integer, String> recordTuple = new Tuple2<>(9527, "record1");
		final IntSerializer serializer0 = new IntSerializer();
		final StringSerializer serializer1 = new StringSerializer();
		final TypeSerializer[] serializers = new TypeSerializer[] {
			serializer0, serializer1
		};

		//noinspection unchecked
		final TupleSerializer<Tuple2> serializer = new TupleSerializer(Tuple.getTupleClass(2), serializers);
		final StreamElementSerializer<Tuple2> elementSerializer = new StreamElementSerializer<>(serializer);
		// Write the first record(1024) to a temporary buffer
		final DataOutputSerializer dataOutputSerializer1 = new DataOutputSerializer(1024);
		elementSerializer.serialize(new StreamRecord<>(recordTuple), dataOutputSerializer1);
		serializer0.serialize(dataOutputSerializer1.length(), dataOutputSerializer);
		// Write the first record length and content
		dataOutputSerializer.write(dataOutputSerializer1.getSharedBuffer(), 0, dataOutputSerializer1.length());

		final BufferRecycler recycler = mock(BufferRecycler.class);

		{
			final List<BufferOrEvent> bufferOrEvents = new ArrayList<>();
			final Buffer buffer = new NetworkBuffer(
				HeapMemorySegment.FACTORY.wrap(dataOutputSerializer.getSharedBuffer()),
				recycler, true, 1024);
			final BufferOrEvent bufferOrEvent1 = new BufferOrEvent(buffer, 7);
			bufferOrEvents.add(bufferOrEvent1);
			final InputGate inputGate1 = new MockInputGate(32 * 1024, 16, bufferOrEvents);

			final InputGateFetcher<Tuple2> objectReusedFetcher = new InputGateFetcher<>(
				mock(InputSelector.InputSelection.class),
				inputGate1,
				serializer,
				new BarrierTracker(inputGate1),
				mock(IOManager.class),
				mock(InputProcessor.class),
				this,
				4,
				true,
				new Configuration());

			// Read from the input gate
			objectReusedFetcher.setCurrentRecordDeserializer(null);
			final DeserializationResult result = objectReusedFetcher.getNextResult();
			assertTrue(result.isFullRecord());
			assertNotNull(objectReusedFetcher.getCurrentRecordDeserializer());
			final StreamElement streamElement =
				objectReusedFetcher.getDeserializationDelegate().getInstance();
			assertTrue(streamElement.isRecord());
			assertEquals(recordTuple, streamElement.asRecord().getValue());
			assertEquals(objectReusedFetcher.getReusedObject(), streamElement.asRecord().getValue());
			assertTrue(objectReusedFetcher.getReusedObject() == streamElement.asRecord().getValue());
		}

		{
			final List<BufferOrEvent> bufferOrEvents = new ArrayList<>();
			final Buffer buffer = new NetworkBuffer(
				HeapMemorySegment.FACTORY.wrap(dataOutputSerializer.getSharedBuffer()),
				recycler, true, 1024);
			final BufferOrEvent bufferOrEvent2 = new BufferOrEvent(buffer, 5);
			bufferOrEvents.add(bufferOrEvent2);
			final InputGate inputGate2 = new MockInputGate(32 * 1024, 16, bufferOrEvents);
			final InputGateFetcher<Tuple2> objectNonReusedFetcher = new InputGateFetcher<>(
				mock(InputSelector.InputSelection.class),
				inputGate2,
				serializer,
				new BarrierTracker(inputGate2),
				mock(IOManager.class),
				mock(InputProcessor.class),
				this,
				4,
				false,
				new Configuration());

			objectNonReusedFetcher.setCurrentRecordDeserializer(null);
			final DeserializationResult result = objectNonReusedFetcher.getNextResult();
			assertTrue(result.isFullRecord());
			assertNotNull(objectNonReusedFetcher.getCurrentRecordDeserializer());
			final StreamElement streamElement =
				objectNonReusedFetcher.getDeserializationDelegate().getInstance();
			assertTrue(streamElement.isRecord());
			assertEquals(recordTuple, streamElement.asRecord().getValue());
			assertFalse(objectNonReusedFetcher.getReusedObject() == streamElement.asRecord().getValue());
		}
	}

	@Test
	public void testFetchAndProcess() throws Exception {
		final InputProcessor inputProcessor = mock(InputProcessor.class);

		@SuppressWarnings("unchecked")
		final RecordDeserializer<DeserializationDelegate<StreamElement>> deserializer = mock(RecordDeserializer.class);
		final Buffer buffer = mock(Buffer.class);
		when(deserializer.getCurrentBuffer()).thenReturn(buffer);

		final InputGate inputGate = mock(InputGate.class);
		when(inputGate.moreAvailable()).thenReturn(false);
		when(inputGate.getAllInputChannels()).thenReturn(new InputChannel[]{});

		final FakeInputGateFetcher<Integer> fetcher = new FakeInputGateFetcher<>(
			mock(InputSelector.InputSelection.class),
			inputGate,
			new IntSerializer(),
			mock(SelectedReadingBarrierHandler.class),
			mock(IOManager.class),
			inputProcessor,
			this,
			4,
			true,
			new Configuration());

		fetcher.addNextResult(PARTIAL_RECORD);
		fetcher.addNextResult(INTERMEDIATE_RECORD_FROM_BUFFER);
		fetcher.addNextResult(LAST_RECORD_FROM_BUFFER);
		final StreamRecord record = new StreamRecord<>(1024);
		fetcher.getDeserializationDelegate().setInstance(record);

		assertTrue(fetcher.fetchAndProcess());
		assertFalse(fetcher.moreAvailable());

		assertTrue(fetcher.fetchAndProcess());
		fetcher.setCurrentRecordDeserializer(deserializer);
		assertTrue(fetcher.moreAvailable());

		verify(inputProcessor, times(2)).processRecord(record, fetcher.getCurrentChannelIndex());
	}

	class FakeInputGateFetcher<IN> extends InputGateFetcher<IN> {

		private Queue<DeserializationResult> nextResults = new ArrayDeque<>();

		FakeInputGateFetcher(
			InputSelector.InputSelection inputSelection,
			InputGate inputGate,
			TypeSerializer<IN> serializer,
			SelectedReadingBarrierHandler barrierHandler,
			IOManager ioManager,
			InputProcessor inputProcessor, Object checkpointLock,
			int basedChannelCount,
			boolean objectReuse,
			Configuration taskManagerConfig) {

			super(inputSelection, inputGate, serializer, barrierHandler, ioManager, inputProcessor,
				checkpointLock, basedChannelCount, objectReuse, taskManagerConfig);
		}

		void addNextResult(DeserializationResult nextResult) {
			nextResults.add(nextResult);
		}

		@Override
		DeserializationResult getNextResult() {
			return nextResults.poll();
		}
	}
}
