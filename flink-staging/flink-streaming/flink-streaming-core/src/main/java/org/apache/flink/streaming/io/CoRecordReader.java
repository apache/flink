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

package org.apache.flink.streaming.io;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.api.reader.AbstractReader;
import org.apache.flink.runtime.io.network.api.reader.MutableRecordReader;
import org.apache.flink.runtime.io.network.api.serialization.AdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.UnionInputGate;
import org.apache.flink.runtime.util.event.EventListener;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A CoRecordReader wraps {@link MutableRecordReader}s of two different input
 * types to read records effectively.
 */
@SuppressWarnings("rawtypes")
public class CoRecordReader<T1 extends IOReadableWritable, T2 extends IOReadableWritable> extends
		AbstractReader implements EventListener<InputGate> {

	private final InputGate bufferReader1;

	private final InputGate bufferReader2;

	private final BlockingQueue<Integer> availableRecordReaders = new LinkedBlockingQueue<Integer>();

	private AdaptiveSpanningRecordDeserializer[] reader1RecordDeserializers;

	private RecordDeserializer<T1> reader1currentRecordDeserializer;

	private AdaptiveSpanningRecordDeserializer[] reader2RecordDeserializers;

	private RecordDeserializer<T2> reader2currentRecordDeserializer;

	// 0 => none, 1 => reader (T1), 2 => reader (T2)
	private int currentReaderIndex;

	private boolean hasRequestedPartitions;

	public CoRecordReader(InputGate bufferReader1, InputGate bufferReader2) {
		super(new UnionInputGate(bufferReader1, bufferReader2));

		this.bufferReader1 = bufferReader1;
		this.bufferReader2 = bufferReader2;

		this.reader1RecordDeserializers = new AdaptiveSpanningRecordDeserializer[bufferReader1
				.getNumberOfInputChannels()];
		this.reader2RecordDeserializers = new AdaptiveSpanningRecordDeserializer[bufferReader2
				.getNumberOfInputChannels()];

		for (int i = 0; i < reader1RecordDeserializers.length; i++) {
			reader1RecordDeserializers[i] = new AdaptiveSpanningRecordDeserializer<T1>();
		}

		for (int i = 0; i < reader2RecordDeserializers.length; i++) {
			reader2RecordDeserializers[i] = new AdaptiveSpanningRecordDeserializer<T2>();
		}

		bufferReader1.registerListener(this);
		bufferReader2.registerListener(this);
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

						final BufferOrEvent boe = bufferReader1.getNextBufferOrEvent();

						if (boe.isBuffer()) {
							reader1currentRecordDeserializer = reader1RecordDeserializers[boe
									.getChannelIndex()];
							reader1currentRecordDeserializer.setNextBuffer(boe.getBuffer());
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
						final BufferOrEvent boe = bufferReader2.getNextBufferOrEvent();

						if (boe.isBuffer()) {
							reader2currentRecordDeserializer = reader2RecordDeserializers[boe
									.getChannelIndex()];
							reader2currentRecordDeserializer.setNextBuffer(boe.getBuffer());
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

	private int getNextReaderIndexBlocking() throws InterruptedException {
		return availableRecordReaders.take();
	}

	// ------------------------------------------------------------------------
	// Data availability notifications
	// ------------------------------------------------------------------------

	@Override
	public void onEvent(InputGate bufferReader) {
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
}
