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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.core.io.IOReadableWritable;

/**
 * Utility class to encapsulate the logic of building a {@link RecordWriter} instance.
 */
public class RecordWriterBuilder<T extends IOReadableWritable> {

	private ChannelSelector<T> selector = new RoundRobinChannelSelector<>();

	private long timeout = -1;

	private String taskName = "test";

	public RecordWriterBuilder<T> setChannelSelector(ChannelSelector<T> selector) {
		this.selector = selector;
		return this;
	}

	public RecordWriterBuilder<T> setTimeout(long timeout) {
		this.timeout = timeout;
		return this;
	}

	public RecordWriterBuilder<T> setTaskName(String taskName) {
		this.taskName = taskName;
		return this;
	}

	public RecordWriter<T> build(ResultPartitionWriter writer) {
		if (selector.isBroadcast()) {
			return new BroadcastRecordWriter<>(writer, timeout, taskName);
		} else {
			return new ChannelSelectorRecordWriter<>(writer, selector, timeout, taskName);
		}
	}
}
