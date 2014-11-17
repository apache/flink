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

import java.io.IOException;
import java.util.ArrayList;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.Buffer;
import org.apache.flink.runtime.io.network.api.ChannelSelector;
import org.apache.flink.runtime.io.network.api.RecordWriter;
import org.apache.flink.runtime.io.network.api.RoundRobinChannelSelector;
import org.apache.flink.runtime.io.network.bufferprovider.BufferProvider;
import org.apache.flink.runtime.io.network.serialization.RecordSerializer;
import org.apache.flink.runtime.io.network.serialization.SpanningRecordSerializer;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

public class StreamRecordWriter<T extends IOReadableWritable> extends RecordWriter<T> {

	private final BufferProvider bufferPool;

	private final ChannelSelector<T> channelSelector;

	private int numChannels;

	private long timeout;

	private OutputFlusher outputFlusher;

	/** RecordSerializer per outgoing channel */
	private RecordSerializer<T>[] serializers;

	private ArrayList<TargetChannel> targetChannels;

	// -----------------------------------------------------------------------------------------------------------------

	public StreamRecordWriter(AbstractInvokable invokable) {
		this(invokable, new RoundRobinChannelSelector<T>(), 1000);
	}

	public StreamRecordWriter(AbstractInvokable invokable, ChannelSelector<T> channelSelector) {
		this(invokable, channelSelector, 1000);
	}

	public StreamRecordWriter(AbstractInvokable invokable, ChannelSelector<T> channelSelector,
			long timeout) {
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
		this.targetChannels = new ArrayList<TargetChannel>(numChannels);

		for (int i = 0; i < this.numChannels; i++) {
			this.serializers[i] = new SpanningRecordSerializer<T>();
			this.targetChannels.add(new TargetChannel(i));
		}

		outputFlusher = new OutputFlusher();
		outputFlusher.start();
	}

	@Override
	public void emit(final T record) throws IOException, InterruptedException {
		for (int targetChannel : this.channelSelector.selectChannels(record, this.numChannels)) {
			targetChannels.get(targetChannel).emit(record);
		}
	}

	@Override
	public void flush() throws IOException, InterruptedException {
		for (int targetChannel = 0; targetChannel < this.numChannels; targetChannel++) {
			targetChannels.get(targetChannel).flush();
		}
	}

	public void close() {
		try {
			if (outputFlusher != null) {
				outputFlusher.terminate();
				outputFlusher.join();
			}
			flush();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private class TargetChannel {

		private int targetChannel;
		private RecordSerializer<T> serializer;

		public TargetChannel(int targetChannel) {
			this.targetChannel = targetChannel;
			this.serializer = serializers[targetChannel];
		}

		public synchronized void emit(final T record) throws IOException, InterruptedException {
			RecordSerializer.SerializationResult result = serializer.addRecord(record);
			while (result.isFullBuffer()) {
				Buffer buffer = serializer.getCurrentBuffer();
				if (buffer != null) {
					sendBuffer(buffer, targetChannel);
				}

				buffer = bufferPool.requestBufferBlocking(bufferPool.getBufferSize());
				result = serializer.setNextBuffer(buffer);
			}
		}

		public synchronized void flush() throws IOException, InterruptedException {
			Buffer buffer = serializer.getCurrentBuffer();
			if (buffer != null) {
				sendBuffer(buffer, targetChannel);
			}

			serializer.clear();
		}
	}
	
	private class OutputFlusher extends Thread {

		private boolean running = true;

		public void terminate() {
			running = false;
		}

		@Override
		public void run() {
			while (running && !outputGate.isClosed()) {
				try {
					flush();
					Thread.sleep(timeout);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		}
	}

}
