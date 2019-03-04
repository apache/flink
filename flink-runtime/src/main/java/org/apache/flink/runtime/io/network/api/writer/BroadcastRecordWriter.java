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

import java.io.IOException;

/**
 * A special record-oriented runtime result writer only for broadcast mode.
 *
 * <p>The BroadcastRecordWriter extends the {@link RecordWriter} and handles {@link #emit(IOReadableWritable)}
 * operation via {@link #broadcastEmit(IOReadableWritable)} directly in a more efficient way.
 *
 * @param <T> the type of the record that can be emitted with this record writer
 */
public class BroadcastRecordWriter<T extends IOReadableWritable> extends RecordWriter<T> {

	public BroadcastRecordWriter(
			ResultPartitionWriter writer,
			ChannelSelector<T> channelSelector,
			long timeout,
			String taskName) {
		super(writer, channelSelector, timeout, taskName);
	}

	@Override
	public void emit(T record) throws IOException, InterruptedException {
		broadcastEmit(record);
	}
}
