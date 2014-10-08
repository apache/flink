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
import org.apache.flink.runtime.io.network.api.writer.ChannelSelector;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RoundRobinChannelSelector;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

public class StreamRecordWriter<T extends IOReadableWritable> extends RecordWriter {

	private long timeout;

	public StreamRecordWriter(AbstractInvokable invokable) {
		this(invokable, new RoundRobinChannelSelector<T>(), 1000);
	}

	public StreamRecordWriter(AbstractInvokable invokable, ChannelSelector<T> channelSelector) {
		this(invokable, channelSelector, 1000);
	}

	public StreamRecordWriter(AbstractInvokable invokable, ChannelSelector<T> channelSelector, long timeout) {
		super(invokable.getEnvironment().getWriter(0), channelSelector);

		this.timeout = timeout;

		(new OutputFlusher()).start();
	}

	private class OutputFlusher extends Thread {

		@Override
		public void run() {
			while (!isFinished()) {
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
