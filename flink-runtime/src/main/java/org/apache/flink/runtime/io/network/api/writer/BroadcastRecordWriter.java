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
 * <p>The BroadcastRecordWriter extends the {@link RecordWriter} and emits records to all channels for
 * regular {@link #emit(IOReadableWritable)}.
 *
 * @param <T> the type of the record that can be emitted with this record writer
 */
public final class BroadcastRecordWriter<T extends IOReadableWritable> extends RecordWriter<T> {

	BroadcastRecordWriter(
			ResultPartitionWriter writer,
			long timeout,
			String taskName) {
		super(writer, timeout, taskName);
	}

	@Override
	public void emit(T record) throws IOException {
		broadcastEmit(record);
	}

	@Override
	public void broadcastEmit(T record) throws IOException {
		checkErroneous();

		targetPartition.broadcastRecord(serializeRecord(serializer, record));

		if (flushAlways) {
			flushAll();
		}
	}
}
