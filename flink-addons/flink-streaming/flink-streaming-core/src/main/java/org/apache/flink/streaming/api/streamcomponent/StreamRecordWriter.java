/**
 *
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
 *
 */

package org.apache.flink.streaming.api.streamcomponent;

import java.io.IOException;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.Buffer;
import org.apache.flink.runtime.io.network.api.ChannelSelector;
import org.apache.flink.runtime.io.network.api.RecordWriter;
import org.apache.flink.runtime.io.network.api.RoundRobinChannelSelector;
import org.apache.flink.runtime.io.network.bufferprovider.BufferProvider;
import org.apache.flink.runtime.io.network.serialization.RecordSerializer;
import org.apache.flink.runtime.io.network.serialization.SpanningRecordSerializer;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

public class StreamRecordWriter<T extends IOReadableWritable> extends
		RecordWriter<T> {

	private final BufferProvider bufferPool;

	private final ChannelSelector<T> channelSelector;

	private int numChannels;

	private long timeout;

	/** RecordSerializer per outgoing channel */
	private RecordSerializer<T>[] serializers;

	// -----------------------------------------------------------------------------------------------------------------

	public StreamRecordWriter(AbstractInvokable invokable) {
		this(invokable, new RoundRobinChannelSelector<T>(), 1000);
	}

	public StreamRecordWriter(AbstractInvokable invokable,
			ChannelSelector<T> channelSelector) {
		this(invokable, channelSelector, 1000);
	}

	public StreamRecordWriter(AbstractInvokable invokable,
			ChannelSelector<T> channelSelector, long timeout) {
		// initialize the gate
		super(invokable);

		this.timeout = timeout;
		this.bufferPool = invokable.getEnvironment().getOutputBufferProvider();
		this.channelSelector = channelSelector;
	}

	// -----------------------------------------------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	@Override
	public void initializeSerializers() {
		this.numChannels = this.outputGate.getNumChannels();
		this.serializers = new RecordSerializer[numChannels];
		for (int i = 0; i < this.numChannels; i++) {
			this.serializers[i] = new SpanningRecordSerializer<T>();
		}
		(new OutputFlusher()).start();
	}

	@Override
	public void emit(final T record) throws IOException, InterruptedException {
		for (int targetChannel : this.channelSelector.selectChannels(record,
				this.numChannels)) {
			// serialize with corresponding serializer and send full buffer

			RecordSerializer<T> serializer = this.serializers[targetChannel];

			synchronized (serializer) {
				RecordSerializer.SerializationResult result = serializer
						.addRecord(record);
				while (result.isFullBuffer()) {
					Buffer buffer = serializer.getCurrentBuffer();
					if (buffer != null) {
						sendBuffer(buffer, targetChannel);
					}

					buffer = this.bufferPool
							.requestBufferBlocking(this.bufferPool
									.getBufferSize());
					result = serializer.setNextBuffer(buffer);
				}
			}
		}
	}

	@Override
	public void flush() throws IOException, InterruptedException {
		for (int targetChannel = 0; targetChannel < this.numChannels; targetChannel++) {
			RecordSerializer<T> serializer = this.serializers[targetChannel];
			synchronized (serializer) {
				Buffer buffer = serializer.getCurrentBuffer();
				if (buffer != null) {
					sendBuffer(buffer, targetChannel);
				}

				serializer.clear();
			}

		}
	}

	private class OutputFlusher extends Thread {

		@Override
		public void run() {
			while (!outputGate.isClosed()) {
				try {
					Thread.sleep(timeout);
					flush();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		}
	}

}
