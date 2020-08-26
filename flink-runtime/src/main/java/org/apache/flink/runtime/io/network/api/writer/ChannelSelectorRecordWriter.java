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
import java.nio.ByteBuffer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A regular record-oriented runtime result writer.
 *
 * <p>The ChannelSelectorRecordWriter extends the {@link RecordWriter} and emits records to the channel
 * selected by the {@link ChannelSelector} for regular {@link #emit(IOReadableWritable)}.
 *
 * @param <T> the type of the record that can be emitted with this record writer
 */
public final class ChannelSelectorRecordWriter<T extends IOReadableWritable> extends RecordWriter<T> {

	private final ChannelSelector<T> channelSelector;

	ChannelSelectorRecordWriter(
			ResultPartitionWriter writer,
			ChannelSelector<T> channelSelector,
			long timeout,
			String taskName) {
		super(writer, timeout, taskName);

		this.channelSelector = checkNotNull(channelSelector);
		this.channelSelector.setup(numberOfChannels);
	}

	@Override
	public void emit(T record) throws IOException {
		emit(record, channelSelector.selectChannel(record));
	}

	@Override
	public void broadcastEmit(T record) throws IOException {
		checkErroneous();

		// Emitting to all channels in a for loop can be better than calling
		// ResultPartitionWriter#broadcastRecord because the broadcastRecord
		// method incurs extra overhead.
		ByteBuffer serializedRecord = serializeRecord(serializer, record);
		for (int channelIndex = 0; channelIndex < numberOfChannels; channelIndex++) {
			serializedRecord.rewind();
			emit(record, channelIndex);
		}

		if (flushAlways) {
			flushAll();
		}
	}
}
