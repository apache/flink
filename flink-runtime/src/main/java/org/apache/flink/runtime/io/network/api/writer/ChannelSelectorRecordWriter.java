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
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A regular record-oriented runtime result writer.
 *
 * <p>The ChannelSelectorRecordWriter extends the {@link RecordWriter} and maintains an array of
 * {@link BufferBuilder}s for all the channels. The {@link #emit(IOReadableWritable)}
 * operation is based on {@link ChannelSelector} to select the target channel.
 *
 * @param <T> the type of the record that can be emitted with this record writer
 */
public final class ChannelSelectorRecordWriter<T extends IOReadableWritable> extends RecordWriter<T> {

	private final ChannelSelector<T> channelSelector;

	/** Every subpartition maintains a separate buffer builder which might be null. */
	private final BufferBuilder[] bufferBuilders;

	ChannelSelectorRecordWriter(
			ResultPartitionWriter writer,
			ChannelSelector<T> channelSelector,
			long timeout,
			String taskName) {
		super(writer, timeout, taskName);

		this.channelSelector = checkNotNull(channelSelector);
		this.channelSelector.setup(numberOfChannels);

		this.bufferBuilders = new BufferBuilder[numberOfChannels];
	}

	@Override
	public void emit(T record) throws IOException, InterruptedException {
		emit(record, channelSelector.selectChannel(record));
	}

	@Override
	public void randomEmit(T record) throws IOException, InterruptedException {
		emit(record, rng.nextInt(numberOfChannels));
	}

	/**
	 * The record is serialized into intermediate serialization buffer which is then copied
	 * into the target buffer for every channel.
	 */
	@Override
	public void broadcastEmit(T record) throws IOException, InterruptedException {
		checkErroneous();

		serializer.serializeRecord(record);

		boolean pruneAfterCopying = false;
		for (int targetChannel = 0; targetChannel < numberOfChannels; targetChannel++) {
			if (copyFromSerializerToTargetChannel(targetChannel)) {
				pruneAfterCopying = true;
			}
		}

		if (pruneAfterCopying) {
			serializer.prune();
		}
	}

	@Override
	public BufferBuilder getBufferBuilder(int targetChannel) throws IOException, InterruptedException {
		if (bufferBuilders[targetChannel] != null) {
			return bufferBuilders[targetChannel];
		} else {
			return requestNewBufferBuilder(targetChannel);
		}
	}

	@Override
	public BufferBuilder requestNewBufferBuilder(int targetChannel) throws IOException, InterruptedException {
		checkState(bufferBuilders[targetChannel] == null || bufferBuilders[targetChannel].isFinished());

		BufferBuilder bufferBuilder = super.requestNewBufferBuilder(targetChannel);
		addBufferConsumer(bufferBuilder.createBufferConsumer(), targetChannel);
		bufferBuilders[targetChannel] = bufferBuilder;
		return bufferBuilder;
	}

	@Override
	public void tryFinishCurrentBufferBuilder(int targetChannel) {
		if (bufferBuilders[targetChannel] == null) {
			return;
		}
		BufferBuilder bufferBuilder = bufferBuilders[targetChannel];
		bufferBuilders[targetChannel] = null;

		finishBufferBuilder(bufferBuilder);
	}

	@Override
	public void emptyCurrentBufferBuilder(int targetChannel) {
		bufferBuilders[targetChannel] = null;
	}

	@Override
	public void closeBufferBuilder(int targetChannel) {
		if (bufferBuilders[targetChannel] != null) {
			bufferBuilders[targetChannel].finish();
			bufferBuilders[targetChannel] = null;
		}
	}

	@Override
	public void clearBuffers() {
		for (int index = 0; index < numberOfChannels; index++) {
			closeBufferBuilder(index);
		}
	}
}
