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

import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.io.network.api.reader.AbstractReader;
import org.apache.flink.runtime.io.network.api.reader.MutableRecordReader;
import org.apache.flink.runtime.io.network.api.serialization.AdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.UnionInputGate;
import org.apache.flink.runtime.util.event.EventListener;
import org.apache.flink.streaming.runtime.tasks.StreamingSuperstep;

/**
 * A CoRecordReader wraps {@link MutableRecordReader}s of two different input
 * types to read records effectively.
 */
@SuppressWarnings("rawtypes")
public class CoRecordReader<T1 extends IOReadableWritable, T2 extends IOReadableWritable> extends
		AbstractReader implements EventListener<InputGate>, StreamingReader {

	private final InputGate bufferReader1;

	private final InputGate bufferReader2;

	private final LinkedBlockingDeque<Integer> availableRecordReaders = new LinkedBlockingDeque<Integer>();

	private LinkedList<Integer> processed = new LinkedList<Integer>();

	private AdaptiveSpanningRecordDeserializer[] reader1RecordDeserializers;

	private RecordDeserializer<T1> reader1currentRecordDeserializer;

	private AdaptiveSpanningRecordDeserializer[] reader2RecordDeserializers;

	private RecordDeserializer<T2> reader2currentRecordDeserializer;

	// 0 => none, 1 => reader (T1), 2 => reader (T2)
	private int currentReaderIndex;

	private boolean hasRequestedPartitions;

	protected CoBarrierBuffer barrierBuffer1;
	protected CoBarrierBuffer barrierBuffer2;

	public CoRecordReader(InputGate inputgate1, InputGate inputgate2) {
		super(new UnionInputGate(inputgate1, inputgate2));

		this.bufferReader1 = inputgate1;
		this.bufferReader2 = inputgate2;

		this.reader1RecordDeserializers = new AdaptiveSpanningRecordDeserializer[inputgate1
				.getNumberOfInputChannels()];
		this.reader2RecordDeserializers = new AdaptiveSpanningRecordDeserializer[inputgate2
				.getNumberOfInputChannels()];

		for (int i = 0; i < reader1RecordDeserializers.length; i++) {
			reader1RecordDeserializers[i] = new AdaptiveSpanningRecordDeserializer<T1>();
		}

		for (int i = 0; i < reader2RecordDeserializers.length; i++) {
			reader2RecordDeserializers[i] = new AdaptiveSpanningRecordDeserializer<T2>();
		}

		inputgate1.registerListener(this);
		inputgate2.registerListener(this);

		barrierBuffer1 = new CoBarrierBuffer(inputgate1, this);
		barrierBuffer2 = new CoBarrierBuffer(inputgate2, this);

		barrierBuffer1.setOtherBarrierBuffer(barrierBuffer2);
		barrierBuffer2.setOtherBarrierBuffer(barrierBuffer1);
	}

	public void requestPartitionsOnce() throws IOException, InterruptedException {
		if (!hasRequestedPartitions) {
			bufferReader1.requestPartitions();
			bufferReader2.requestPartitions();

			hasRequestedPartitions = true;
		}
	}

	@SuppressWarnings("unchecked")
	protected int getNextRecord(T1 target1, T2 target2) throws IOException, InterruptedException {

		requestPartitionsOnce();

		while (true) {
			if (currentReaderIndex == 0) {
				if ((bufferReader1.isFinished() && bufferReader2.isFinished())) {
					return 0;
				}

				currentReaderIndex = getNextReaderIndexBlocking();

			}

			if (currentReaderIndex == 1) {
				while (true) {
					if (reader1currentRecordDeserializer != null) {
						RecordDeserializer.DeserializationResult result = reader1currentRecordDeserializer
								.getNextRecord(target1);

						if (result.isBufferConsumed()) {
							reader1currentRecordDeserializer.getCurrentBuffer().recycle();
							reader1currentRecordDeserializer = null;

							currentReaderIndex = 0;
						}

						if (result.isFullRecord()) {
							return 1;
						}
					} else {

						final BufferOrEvent boe = barrierBuffer1.getNextNonBlocked();

						if (boe.isBuffer()) {
							reader1currentRecordDeserializer = reader1RecordDeserializers[boe
									.getChannelIndex()];
							reader1currentRecordDeserializer.setNextBuffer(boe.getBuffer());
						} else if (boe.getEvent() instanceof StreamingSuperstep) {
							barrierBuffer1.processSuperstep(boe);
							currentReaderIndex = 0;

							break;
						} else if (handleEvent(boe.getEvent())) {
							currentReaderIndex = 0;

							break;
						}
					}
				}
			} else if (currentReaderIndex == 2) {
				while (true) {
					if (reader2currentRecordDeserializer != null) {
						RecordDeserializer.DeserializationResult result = reader2currentRecordDeserializer
								.getNextRecord(target2);

						if (result.isBufferConsumed()) {
							reader2currentRecordDeserializer.getCurrentBuffer().recycle();
							reader2currentRecordDeserializer = null;

							currentReaderIndex = 0;
						}

						if (result.isFullRecord()) {
							return 2;
						}
					} else {
						final BufferOrEvent boe = barrierBuffer2.getNextNonBlocked();

						if (boe.isBuffer()) {
							reader2currentRecordDeserializer = reader2RecordDeserializers[boe
									.getChannelIndex()];
							reader2currentRecordDeserializer.setNextBuffer(boe.getBuffer());
						} else if (boe.getEvent() instanceof StreamingSuperstep) {
							barrierBuffer2.processSuperstep(boe);
							currentReaderIndex = 0;

							break;
						} else if (handleEvent(boe.getEvent())) {
							currentReaderIndex = 0;

							break;
						}
					}
				}
			} else {
				throw new IllegalStateException("Bug: unexpected current reader index.");
			}
		}
	}

	protected int getNextReaderIndexBlocking() throws InterruptedException {

		Integer nextIndex = 0;

		while (processed.contains(nextIndex = availableRecordReaders.take())) {
			processed.remove(nextIndex);
		}

		if (nextIndex == 1) {
			if (barrierBuffer1.isAllBlocked()) {
				availableRecordReaders.addFirst(1);
				processed.add(2);
				return 2;
			} else {
				return 1;
			}
		} else {
			if (barrierBuffer2.isAllBlocked()) {
				availableRecordReaders.addFirst(2);
				processed.add(1);
				return 1;
			} else {
				return 2;
			}

		}

	}

	// ------------------------------------------------------------------------
	// Data availability notifications
	// ------------------------------------------------------------------------

	@Override
	public void onEvent(InputGate bufferReader) {
		addToAvailable(bufferReader);
	}

	protected void addToAvailable(InputGate bufferReader) {
		if (bufferReader == bufferReader1) {
			availableRecordReaders.add(1);
		} else if (bufferReader == bufferReader2) {
			availableRecordReaders.add(2);
		}
	}

	public void clearBuffers() {
		for (RecordDeserializer<?> deserializer : reader1RecordDeserializers) {
			Buffer buffer = deserializer.getCurrentBuffer();
			if (buffer != null && !buffer.isRecycled()) {
				buffer.recycle();
			}
		}
		for (RecordDeserializer<?> deserializer : reader2RecordDeserializers) {
			Buffer buffer = deserializer.getCurrentBuffer();
			if (buffer != null && !buffer.isRecycled()) {
				buffer.recycle();
			}
		}
	}

	@Override
	public void setReporter(AccumulatorRegistry.Reporter reporter) {
		for (AdaptiveSpanningRecordDeserializer serializer : reader1RecordDeserializers) {
			serializer.setReporter(reporter);
		}
		for (AdaptiveSpanningRecordDeserializer serializer : reader2RecordDeserializers) {
			serializer.setReporter(reporter);
		}
	}

	private class CoBarrierBuffer extends BarrierBuffer {

		private CoBarrierBuffer otherBuffer;

		public CoBarrierBuffer(InputGate inputGate, AbstractReader reader) {
			super(inputGate, reader);
		}

		public void setOtherBarrierBuffer(CoBarrierBuffer other) {
			this.otherBuffer = other;
		}

		@Override
		protected void actOnAllBlocked() {
			if (otherBuffer.isAllBlocked()) {
				super.actOnAllBlocked();
				otherBuffer.releaseBlocks();
			}
		}

	}

	public void cleanup() throws IOException {
		try {
			barrierBuffer1.cleanup();
		} finally {
			barrierBuffer2.cleanup();
		}

	}
}
