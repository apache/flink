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

/**
 * Utility class to encapsulate the logic of building a {@link RecordWriter} instance.
 */
public class RecordWriterBuilder {

	private ChannelSelector selector = new RoundRobinChannelSelector();

	private long timeout = -1;

	private String taskName = "test";

	public RecordWriterBuilder setChannelSelector(ChannelSelector selector) {
		this.selector = selector;
		return this;
	}

	public RecordWriterBuilder setTimeout(long timeout) {
		this.timeout = timeout;
		return this;
	}

	public RecordWriterBuilder setTaskName(String taskName) {
		this.taskName = taskName;
		return this;
	}

	public RecordWriter build(ResultPartitionWriter writer) {
		if (selector.isBroadcast()) {
			return new BroadcastRecordWriter(writer, selector, timeout, taskName);
		} else {
			return new RecordWriter(writer, selector, timeout, taskName);
		}
	}
}
