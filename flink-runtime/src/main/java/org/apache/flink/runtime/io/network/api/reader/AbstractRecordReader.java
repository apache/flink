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
import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.util.event.EventListener;

import java.io.IOException;

/**
 * A record-oriented runtime result reader, which wraps a {@link BufferReaderBase}.
 * <p>
 * This abstract base class is used by both the mutable and immutable record
 * reader.
 *
 * @param <T> The type of the record that can be read with this record reader.
 */
abstract class AbstractRecordReader<T extends IOReadableWritable> implements ReaderBase {

	private final BufferReaderBase reader;

	private final RecordDeserializer<T>[] recordDeserializers;

	private RecordDeserializer<T> currentRecordDeserializer;

	private boolean isFinished;

	protected AbstractRecordReader(BufferReaderBase reader) {
		this.reader = reader;

		// Initialize one deserializer per input channel
		this.recordDeserializers = new SpillingAdaptiveSpanningRecordDeserializer[reader.getNumberOfInputChannels()];
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
					currentRecordDeserializer.getCurrentBuffer().recycle();
					currentRecordDeserializer = null;
				}

				if (result.isFullRecord()) {
					return true;
				}
			}

			final Buffer nextBuffer = reader.getNextBufferBlocking();
			final int channelIndex = reader.getChannelIndexOfLastBuffer();

			if (nextBuffer == null) {
				if (reader.isFinished()) {
					isFinished = true;
					return false;
				}
				else if (reader.hasReachedEndOfSuperstep()) {
					return false;
				}
				else {
					// More data is coming...
					continue;
				}
			}

			currentRecordDeserializer = recordDeserializers[channelIndex];
			currentRecordDeserializer.setNextBuffer(nextBuffer);
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

	@Override
	public void sendTaskEvent(TaskEvent event) throws IOException, InterruptedException {
		reader.sendTaskEvent(event);
	}

	@Override
	public boolean isFinished() {
		return reader.isFinished();
	}

	@Override
	public void subscribeToTaskEvent(EventListener<TaskEvent> eventListener, Class<? extends TaskEvent> eventType) {
		reader.subscribeToTaskEvent(eventListener, eventType);
	}

	@Override
	public void setIterativeReader() {
		reader.setIterativeReader();
	}

	@Override
	public void startNextSuperstep() {
		reader.startNextSuperstep();
	}

	@Override
	public boolean hasReachedEndOfSuperstep() {
		return reader.hasReachedEndOfSuperstep();
	}
}
