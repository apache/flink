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

// We have it in this package because we could not mock the methods otherwise

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordSerializer;
import org.apache.flink.runtime.io.network.api.serialization.SpanningRecordSerializer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.buildSingleBuffer;
import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createBufferBuilder;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

/**
 * Test {@link InputGate} that allows setting multiple channels. Use
 * {@link #sendElement(Object, int)} to offer an element on a specific channel. Use
 * {@link #sendEvent(AbstractEvent, int)} to offer an event on the specified channel. Use
 * {@link #endInput()} to notify all channels of input end.
 */
public class StreamTestSingleInputGate<T> extends TestSingleInputGate {

	private final int numInputChannels;

	private final TestInputChannel[] inputChannels;

	private final int bufferSize;

	private TypeSerializer<T> serializer;

	private ConcurrentLinkedQueue<InputValue<Object>>[] inputQueues;

	@SuppressWarnings("unchecked")
	public StreamTestSingleInputGate(
		int numInputChannels,
		int bufferSize,
		TypeSerializer<T> serializer) throws IOException, InterruptedException {
		super(numInputChannels, false);

		this.bufferSize = bufferSize;
		this.serializer = serializer;

		this.numInputChannels = numInputChannels;
		inputChannels = new TestInputChannel[numInputChannels];

		inputQueues = new ConcurrentLinkedQueue[numInputChannels];

		setupInputChannels();
		doReturn(bufferSize).when(inputGate).getPageSize();
	}

	@SuppressWarnings("unchecked")
	private void setupInputChannels() throws IOException, InterruptedException {

		for (int i = 0; i < numInputChannels; i++) {
			final int channelIndex = i;
			final RecordSerializer<SerializationDelegate<Object>> recordSerializer = new SpanningRecordSerializer<SerializationDelegate<Object>>();
			final SerializationDelegate<Object> delegate = (SerializationDelegate<Object>) (SerializationDelegate<?>)
				new SerializationDelegate<StreamElement>(new StreamElementSerializer<T>(serializer));

			inputQueues[channelIndex] = new ConcurrentLinkedQueue<InputValue<Object>>();
			inputChannels[channelIndex] = new TestInputChannel(inputGate, i);

			final Answer<Optional<BufferAndAvailability>> answer = new Answer<Optional<BufferAndAvailability>>() {
				@Override
				public Optional<BufferAndAvailability> answer(InvocationOnMock invocationOnMock) throws Throwable {
					ConcurrentLinkedQueue<InputValue<Object>> inputQueue = inputQueues[channelIndex];
					InputValue<Object> input;
					boolean moreAvailable;
					synchronized (inputQueue) {
						input = inputQueue.poll();
						moreAvailable = !inputQueue.isEmpty();
					}
					if (input != null && input.isStreamEnd()) {
						when(inputChannels[channelIndex].getInputChannel().isReleased()).thenReturn(
							true);
						return Optional.of(new BufferAndAvailability(EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE), moreAvailable, 0));
					} else if (input != null && input.isStreamRecord()) {
						Object inputElement = input.getStreamRecord();

						BufferBuilder bufferBuilder = createBufferBuilder(bufferSize);
						recordSerializer.continueWritingWithNextBufferBuilder(bufferBuilder);
						delegate.setInstance(inputElement);
						recordSerializer.addRecord(delegate);
						bufferBuilder.finish();

						// Call getCurrentBuffer to ensure size is set
						return Optional.of(new BufferAndAvailability(buildSingleBuffer(bufferBuilder), moreAvailable, 0));
					} else if (input != null && input.isEvent()) {
						AbstractEvent event = input.getEvent();
						return Optional.of(new BufferAndAvailability(EventSerializer.toBuffer(event), moreAvailable, 0));
					} else {
						return Optional.empty();
					}
				}
			};

			when(inputChannels[channelIndex].getInputChannel().getNextBuffer()).thenAnswer(answer);

			inputGate.setInputChannel(new IntermediateResultPartitionID(),
				inputChannels[channelIndex].getInputChannel());
		}
	}

	public void sendElement(Object element, int channel) {
		synchronized (inputQueues[channel]) {
			inputQueues[channel].add(InputValue.element(element));
			inputQueues[channel].notifyAll();
		}
		inputGate.notifyChannelNonEmpty(inputChannels[channel].getInputChannel());
	}

	public void sendEvent(AbstractEvent event, int channel) {
		synchronized (inputQueues[channel]) {
			inputQueues[channel].add(InputValue.event(event));
			inputQueues[channel].notifyAll();
		}
		inputGate.notifyChannelNonEmpty(inputChannels[channel].getInputChannel());
	}

	public void endInput() {
		for (int i = 0; i < numInputChannels; i++) {
			synchronized (inputQueues[i]) {
				inputQueues[i].add(InputValue.streamEnd());
				inputQueues[i].notifyAll();
			}
			inputGate.notifyChannelNonEmpty(inputChannels[i].getInputChannel());
		}
	}

	/**
	 * Returns true iff all input queues are empty.
	 */
	public boolean allQueuesEmpty() {
//		for (int i = 0; i < numInputChannels; i++) {
//			synchronized (inputQueues[i]) {
//				inputQueues[i].add(InputValue.<T>event(new DummyEvent()));
//				inputQueues[i].notifyAll();
//				inputGate.onAvailableBuffer(inputChannels[i].getInputChannel());
//			}
//		}

		for (int i = 0; i < numInputChannels; i++) {
			if (inputQueues[i].size() > 0) {
				return false;
			}
		}
		return true;
	}

	private static class InputValue<T> {
		private Object elementOrEvent;
		private boolean isStreamEnd;
		private boolean isStreamRecord;
		private boolean isEvent;

		private InputValue(Object elementOrEvent, boolean isStreamEnd, boolean isEvent, boolean isStreamRecord) {
			this.elementOrEvent = elementOrEvent;
			this.isStreamEnd = isStreamEnd;
			this.isStreamRecord = isStreamRecord;
			this.isEvent = isEvent;
		}

		public static <X> InputValue<X> element(Object element) {
			return new InputValue<X>(element, false, false, true);
		}

		public static <X> InputValue<X> streamEnd() {
			return new InputValue<X>(null, true, false, false);
		}

		public static <X> InputValue<X> event(AbstractEvent event) {
			return new InputValue<X>(event, false, true, false);
		}

		public Object getStreamRecord() {
			return elementOrEvent;
		}

		public AbstractEvent getEvent() {
			return (AbstractEvent) elementOrEvent;
		}

		public boolean isStreamEnd() {
			return isStreamEnd;
		}

		public boolean isStreamRecord() {
			return isStreamRecord;
		}

		public boolean isEvent() {
			return isEvent;
		}
	}
}
