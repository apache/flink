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
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.api.writer.ChannelSelector;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RoundRobinChannelSelector;

import java.io.IOException;

public class StreamRecordWriter<T extends IOReadableWritable> extends RecordWriter<T> {

	private long timeout;

	private OutputFlusher outputFlusher;

	public StreamRecordWriter(ResultPartitionWriter writer) {
		this(writer, new RoundRobinChannelSelector<T>(), 1000);
	}

	public StreamRecordWriter(ResultPartitionWriter writer, ChannelSelector<T> channelSelector) {
		this(writer, channelSelector, 1000);
	}

	public StreamRecordWriter(ResultPartitionWriter writer, ChannelSelector<T> channelSelector, long timeout) {
		super(writer, channelSelector);

		this.timeout = timeout;
		this.outputFlusher = new OutputFlusher();

		outputFlusher.start();
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

	private class OutputFlusher extends Thread {
		private boolean running = true;
		public void terminate() {
			running = false;
		}
		@Override
		public void run() {
			while (running) {
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
