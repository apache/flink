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

import java.io.IOException;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.event.task.AbstractEvent;
import org.apache.flink.runtime.io.network.api.reader.AbstractReader;
import org.apache.flink.runtime.io.network.api.reader.ReaderBase;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.streaming.runtime.tasks.StreamingSuperstep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A record-oriented reader.
 * <p>
 * This abstract base class is used by both the mutable and immutable record
 * readers.
 * 
 * @param <T>
 *            The type of the record that can be read with this record reader.
 */
public abstract class StreamingAbstractRecordReader<T extends IOReadableWritable> extends
		AbstractReader implements ReaderBase, StreamingReader {

	@SuppressWarnings("unused")
	private static final Logger LOG = LoggerFactory.getLogger(StreamingAbstractRecordReader.class);

	private final RecordDeserializer<T>[] recordDeserializers;

	private RecordDeserializer<T> currentRecordDeserializer;

	private boolean isFinished;

	private final BarrierBuffer barrierBuffer;


	@SuppressWarnings("unchecked")
	protected StreamingAbstractRecordReader(InputGate inputGate) {
		super(inputGate);
		barrierBuffer = new BarrierBuffer(inputGate, this);

		// Initialize one deserializer per input channel
		this.recordDeserializers = new SpillingAdaptiveSpanningRecordDeserializer[inputGate
				.getNumberOfInputChannels()];
		for (int i = 0; i < recordDeserializers.length; i++) {
			recordDeserializers[i] = new SpillingAdaptiveSpanningRecordDeserializer<T>();
		}
	}

	protected boolean getNextRecord(T target) throws IOException, InterruptedException {
		if (isFinished) {
			return false;
		}

		while (true) {
			if (currentRecordDeserializer != null) {
				DeserializationResult result = currentRecordDeserializer.getNextRecord(target);

				if (result.isBufferConsumed()) {
					Buffer currentBuffer = currentRecordDeserializer.getCurrentBuffer();
					currentBuffer.recycle();
					currentRecordDeserializer = null;
				}

				if (result.isFullRecord()) {
					return true;
				}
			}

			final BufferOrEvent bufferOrEvent = barrierBuffer.getNextNonBlocked();

			if (bufferOrEvent.isBuffer()) {
				currentRecordDeserializer = recordDeserializers[bufferOrEvent.getChannelIndex()];
				currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());
			} else {
				// Event received
				final AbstractEvent event = bufferOrEvent.getEvent();

				if (event instanceof StreamingSuperstep) {
					barrierBuffer.processSuperstep(bufferOrEvent);
				} else {
					if (handleEvent(event)) {
						if (inputGate.isFinished()) {
							if (!barrierBuffer.isEmpty()) {
								throw new RuntimeException(
										"BarrierBuffer should be empty at this point");
							}
							isFinished = true;
							return false;
						} else if (hasReachedEndOfSuperstep()) {
							return false;
						} // else: More data is coming...
					}
				}
			}
		}
	}

	public void clearBuffers() {
		for (RecordDeserializer<?> deserializer : recordDeserializers) {
			Buffer buffer = deserializer.getCurrentBuffer();
			if (buffer != null && !buffer.isRecycled()) {
				buffer.recycle();
			}
		}
	}

	public void cleanup() throws IOException {
		barrierBuffer.cleanup();
	}

	@Override
	public void setReporter(AccumulatorRegistry.Reporter reporter) {
		for (RecordDeserializer<?> deserializer : recordDeserializers) {
			deserializer.setReporter(reporter);
		}
	}

}
