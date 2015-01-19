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
import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.io.network.api.reader.BufferReaderBase;
import org.apache.flink.runtime.io.network.api.reader.MutableRecordReader;
import org.apache.flink.runtime.io.network.api.reader.ReaderBase;
import org.apache.flink.runtime.io.network.api.serialization.AdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.util.event.EventListener;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A CoRecordReader wraps {@link MutableRecordReader}s of two different input
 * types to read records effectively.
 */
@SuppressWarnings("rawtypes")
public class CoRecordReader<T1 extends IOReadableWritable, T2 extends IOReadableWritable> implements ReaderBase, EventListener<BufferReaderBase> {

	private final BufferReaderBase bufferReader1;

	private final BufferReaderBase bufferReader2;

	private final BlockingQueue<Integer> availableRecordReaders = new LinkedBlockingQueue<Integer>();

	private AdaptiveSpanningRecordDeserializer[] reader1RecordDeserializers;

	private RecordDeserializer<T1> reader1currentRecordDeserializer;

	private AdaptiveSpanningRecordDeserializer[] reader2RecordDeserializers;

	private RecordDeserializer<T2> reader2currentRecordDeserializer;

	// 0 => none, 1 => reader (T1), 2 => reader (T2)
	private int currentReaderIndex;

	private boolean hasRequestedPartitions;

	public CoRecordReader(BufferReaderBase bufferReader1, BufferReaderBase bufferReader2) {
		this.bufferReader1 = bufferReader1;
		this.bufferReader2 = bufferReader2;

		this.reader1RecordDeserializers = new AdaptiveSpanningRecordDeserializer[bufferReader1.getNumberOfInputChannels()];
		this.reader2RecordDeserializers = new AdaptiveSpanningRecordDeserializer[bufferReader2.getNumberOfInputChannels()];

		for (int i = 0; i < reader1RecordDeserializers.length; i++) {
			reader1RecordDeserializers[i] = new AdaptiveSpanningRecordDeserializer<T1>();
		}

		for (int i = 0; i < reader2RecordDeserializers.length; i++) {
			reader2RecordDeserializers[i] = new AdaptiveSpanningRecordDeserializer<T2>();
		}

		bufferReader1.subscribeToReader(this);
		bufferReader2.subscribeToReader(this);
	}

	public void requestPartitionsOnce() throws IOException {
		if (!hasRequestedPartitions) {
			bufferReader1.requestPartitionsOnce();
			bufferReader2.requestPartitionsOnce();

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
						RecordDeserializer.DeserializationResult result = reader1currentRecordDeserializer.getNextRecord(target1);

						if (result.isBufferConsumed()) {
							reader1currentRecordDeserializer.getCurrentBuffer().recycle();
							reader1currentRecordDeserializer = null;

							currentReaderIndex = 0;
						}

						if (result.isFullRecord()) {
							return 1;
						}
					}

					final Buffer nextBuffer = bufferReader1.getNextBufferBlocking();
					final int channelIndex = bufferReader1.getChannelIndexOfLastBuffer();

					if (nextBuffer == null) {
						currentReaderIndex = 0;

						break;
					}

					reader1currentRecordDeserializer = reader1RecordDeserializers[channelIndex];
					reader1currentRecordDeserializer.setNextBuffer(nextBuffer);
				}
			}
			else if (currentReaderIndex == 2) {
				while (true) {
					if (reader2currentRecordDeserializer != null) {
						RecordDeserializer.DeserializationResult result = reader2currentRecordDeserializer.getNextRecord(target2);

						if (result.isBufferConsumed()) {
							reader2currentRecordDeserializer.getCurrentBuffer().recycle();
							reader2currentRecordDeserializer = null;

							currentReaderIndex = 0;
						}

						if (result.isFullRecord()) {
							return 2;
						}
					}

					final Buffer nextBuffer = bufferReader2.getNextBufferBlocking();
					final int channelIndex = bufferReader2.getChannelIndexOfLastBuffer();

					if (nextBuffer == null) {
						currentReaderIndex = 0;

						break;
					}

					reader2currentRecordDeserializer = reader2RecordDeserializers[channelIndex];
					reader2currentRecordDeserializer.setNextBuffer(nextBuffer);
				}
			}
			else {
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
	public void onEvent(BufferReaderBase bufferReader) {
		if (bufferReader == bufferReader1) {
			availableRecordReaders.add(1);
		}
		else if (bufferReader == bufferReader2) {
			availableRecordReaders.add(2);
		}
	}

	// ------------------------------------------------------------------------

	@Override
	public boolean isFinished() {
		return bufferReader1.isFinished() && bufferReader2.isFinished();
	}

	@Override
	public void subscribeToTaskEvent(EventListener<TaskEvent> eventListener, Class<? extends TaskEvent> eventType) {
		bufferReader1.subscribeToTaskEvent(eventListener, eventType);
		bufferReader2.subscribeToTaskEvent(eventListener, eventType);
	}

	@Override
	public void sendTaskEvent(TaskEvent event) throws IOException, InterruptedException {
		bufferReader1.sendTaskEvent(event);
		bufferReader2.sendTaskEvent(event);
	}

	@Override
	public void setIterativeReader() {
		bufferReader1.setIterativeReader();
		bufferReader2.setIterativeReader();
	}

	@Override
	public void startNextSuperstep() {
		bufferReader1.startNextSuperstep();
		bufferReader2.startNextSuperstep();
	}

	@Override
	public boolean hasReachedEndOfSuperstep() {
		return bufferReader1.hasReachedEndOfSuperstep() && bufferReader2.hasReachedEndOfSuperstep();
	}
}
