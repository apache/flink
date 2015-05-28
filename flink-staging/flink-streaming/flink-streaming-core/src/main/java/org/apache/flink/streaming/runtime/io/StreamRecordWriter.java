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

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.api.writer.ChannelSelector;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RoundRobinChannelSelector;

import java.io.IOException;

public class StreamRecordWriter<T extends IOReadableWritable> extends RecordWriter<T> {

	private long timeout;
	private boolean flushAlways = false;

	private OutputFlusher outputFlusher;

	public StreamRecordWriter(ResultPartitionWriter writer) {
		this(writer, new RoundRobinChannelSelector<T>(), 1000);
	}

	public StreamRecordWriter(ResultPartitionWriter writer, ChannelSelector<T> channelSelector) {
		this(writer, channelSelector, 1000);
	}

	public StreamRecordWriter(ResultPartitionWriter writer, ChannelSelector<T> channelSelector,
			long timeout) {
		super(writer, channelSelector);

		this.timeout = timeout;
		if (timeout == 0) {
			flushAlways = true;
		} else {
			this.outputFlusher = new OutputFlusher();
			outputFlusher.start();
		}
	}
	
	@Override
	public void emit(T record) throws IOException, InterruptedException {
		super.emit(record);
		if (flushAlways) {
			flush();
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
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			// Do nothing here
		}
	}

	private class OutputFlusher extends Thread {
		private volatile boolean running = true;

		public void terminate() {
			running = false;
		}

		@Override
		public void run() {
			while (running) {
				try {
					flush();
					Thread.sleep(timeout);
				} catch (InterruptedException e) {
					// Do nothing here
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		}
	}
}
