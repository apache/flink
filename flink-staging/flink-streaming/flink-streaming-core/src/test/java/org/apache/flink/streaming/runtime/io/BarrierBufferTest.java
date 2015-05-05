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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.reader.AbstractReader;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.util.event.EventListener;
import org.apache.flink.streaming.runtime.tasks.StreamingSuperstep;

import org.junit.Test;

public class BarrierBufferTest {

	@Test
	public void testWithoutBarriers() throws IOException, InterruptedException {

		List<BufferOrEvent> input = new LinkedList<BufferOrEvent>();
		input.add(createBuffer(0));
		input.add(createBuffer(0));
		input.add(createBuffer(0));

		InputGate mockIG = new MockInputGate(1, input);
		AbstractReader mockAR = new MockReader(mockIG);

		BarrierBuffer bb = new BarrierBuffer(mockIG, mockAR);

		assertEquals(input.get(0), bb.getNextNonBlocked());
		assertEquals(input.get(1), bb.getNextNonBlocked());
		assertEquals(input.get(2), bb.getNextNonBlocked());

		bb.cleanup();
	}

	@Test
	public void testOneChannelBarrier() throws IOException, InterruptedException {

		List<BufferOrEvent> input = new LinkedList<BufferOrEvent>();
		input.add(createBuffer(0));
		input.add(createBuffer(0));
		input.add(createSuperstep(1, 0));
		input.add(createBuffer(0));
		input.add(createBuffer(0));
		input.add(createSuperstep(2, 0));
		input.add(createBuffer(0));

		InputGate mockIG = new MockInputGate(1, input);
		AbstractReader mockAR = new MockReader(mockIG);

		BarrierBuffer bb = new BarrierBuffer(mockIG, mockAR);
		BufferOrEvent nextBoe;

		assertEquals(input.get(0), nextBoe = bb.getNextNonBlocked());
		assertEquals(input.get(1), nextBoe = bb.getNextNonBlocked());
		assertEquals(input.get(2), nextBoe = bb.getNextNonBlocked());
		bb.processSuperstep(nextBoe);
		assertEquals(input.get(3), nextBoe = bb.getNextNonBlocked());
		assertEquals(input.get(4), nextBoe = bb.getNextNonBlocked());
		assertEquals(input.get(5), nextBoe = bb.getNextNonBlocked());
		bb.processSuperstep(nextBoe);
		assertEquals(input.get(6), nextBoe = bb.getNextNonBlocked());

		bb.cleanup();
	}

	@Test
	public void testMultiChannelBarrier() throws IOException, InterruptedException {

		List<BufferOrEvent> input = new LinkedList<BufferOrEvent>();
		input.add(createBuffer(0));
		input.add(createBuffer(1));
		input.add(createSuperstep(1, 0));
		input.add(createSuperstep(2, 0));
		input.add(createBuffer(0));
		input.add(createSuperstep(3, 0));
		input.add(createBuffer(0));
		input.add(createBuffer(1));
		input.add(createSuperstep(1, 1));
		input.add(createBuffer(0));
		input.add(createBuffer(1));
		input.add(createSuperstep(2, 1));
		input.add(createSuperstep(3, 1));
		input.add(createSuperstep(4, 0));
		input.add(createBuffer(0));
		input.add(new BufferOrEvent(new EndOfPartitionEvent(), 1));
		

		InputGate mockIG1 = new MockInputGate(2, input);
		AbstractReader mockAR1 = new MockReader(mockIG1);

		BarrierBuffer bb = new BarrierBuffer(mockIG1, mockAR1);
		BufferOrEvent nextBoe;

		check(input.get(0), nextBoe = bb.getNextNonBlocked());
		check(input.get(1), nextBoe = bb.getNextNonBlocked());
		check(input.get(2), nextBoe = bb.getNextNonBlocked());
		bb.processSuperstep(nextBoe);
		check(input.get(7), nextBoe = bb.getNextNonBlocked());
		check(input.get(8), nextBoe = bb.getNextNonBlocked());
		bb.processSuperstep(nextBoe);
		check(input.get(3), nextBoe = bb.getNextNonBlocked());
		bb.processSuperstep(nextBoe);
		check(input.get(10), nextBoe = bb.getNextNonBlocked());
		check(input.get(11), nextBoe = bb.getNextNonBlocked());
		bb.processSuperstep(nextBoe);
		check(input.get(4), nextBoe = bb.getNextNonBlocked());
		check(input.get(5), nextBoe = bb.getNextNonBlocked());
		bb.processSuperstep(nextBoe);
		check(input.get(12), nextBoe = bb.getNextNonBlocked());
		bb.processSuperstep(nextBoe);
		check(input.get(6), nextBoe = bb.getNextNonBlocked());
		check(input.get(9), nextBoe = bb.getNextNonBlocked());
		check(input.get(13), nextBoe = bb.getNextNonBlocked());
		bb.processSuperstep(nextBoe);
		check(input.get(14), nextBoe = bb.getNextNonBlocked());
		check(input.get(15), nextBoe = bb.getNextNonBlocked());

		bb.cleanup();
	}

	private static void check(BufferOrEvent expected, BufferOrEvent actual) {
		assertEquals(expected.isBuffer(), actual.isBuffer());
		assertEquals(expected.getChannelIndex(), actual.getChannelIndex());
		if (expected.isEvent()) {
			assertEquals(expected.getEvent(), actual.getEvent());
		}
	}

	protected static class MockInputGate implements InputGate {

		private int numChannels;
		private Queue<BufferOrEvent> boes;

		public MockInputGate(int numChannels, List<BufferOrEvent> boes) {
			this.numChannels = numChannels;
			this.boes = new LinkedList<BufferOrEvent>(boes);
		}

		@Override
		public int getNumberOfInputChannels() {
			return numChannels;
		}

		@Override
		public boolean isFinished() {
			return boes.isEmpty();
		}

		@Override
		public void requestPartitions() throws IOException, InterruptedException {
		}

		@Override
		public BufferOrEvent getNextBufferOrEvent() throws IOException, InterruptedException {
			return boes.remove();
		}

		@Override
		public void sendTaskEvent(TaskEvent event) throws IOException {
		}

		@Override
		public void registerListener(EventListener<InputGate> listener) {
		}

	}

	protected static class MockReader extends AbstractReader {

		protected MockReader(InputGate inputGate) {
			super(inputGate);
		}

	}

	protected static BufferOrEvent createSuperstep(long id, int channel) {
		return new BufferOrEvent(new StreamingSuperstep(id, System.currentTimeMillis()), channel);
	}

	protected static BufferOrEvent createBuffer(int channel) {
		return new BufferOrEvent(new Buffer(new MemorySegment(new byte[] { 1 }),
				new BufferRecycler() {

					@Override
					public void recycle(MemorySegment memorySegment) {
					}
				}), channel);
	}

}
