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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A special record-oriented runtime result writer only for broadcast mode.
 *
 * <p>The BroadcastRecordWriter extends the {@link RecordWriter} and maintain a single {@link BufferBuilder}
 * for all the channels. Then the serialization results need be copied only once to this buffer which would be
 * shared for all the channels in a more efficient way.
 *
 * @param <T> the type of the record that can be emitted with this record writer
 */
public final class BroadcastRecordWriter<T extends IOReadableWritable> extends RecordWriter<T> {

	/** The current buffer builder shared for all the channels. */
	@Nullable
	private BufferBuilder bufferBuilder;

	/**
	 * The flag for judging whether {@link #requestNewBufferBuilder(int)} and {@link #flushTargetPartition(int)}
	 * is triggered by {@link #randomEmit(IOReadableWritable)} or not.
	 */
	private boolean randomTriggered;

	BroadcastRecordWriter(
			ResultPartitionWriter writer,
			long timeout,
			String taskName) {
		super(writer, timeout, taskName);
	}

	@Override
	public void emit(T record) throws IOException, InterruptedException {
		broadcastEmit(record);
	}

	@Override
	public void randomEmit(T record) throws IOException, InterruptedException {
		randomEmit(record, rng.nextInt(numberOfChannels));
	}

	/**
	 * For non-broadcast emit, we try to finish the current {@link BufferBuilder} first, and then request
	 * a new {@link BufferBuilder} for the random channel. If this new {@link BufferBuilder} is not full,
	 * it can be shared for all the other channels via initializing readable position in created
	 * {@link BufferConsumer}.
	 */
	@VisibleForTesting
	void randomEmit(T record, int targetChannelIndex) throws IOException, InterruptedException {
		tryFinishCurrentBufferBuilder(targetChannelIndex);

		randomTriggered = true;
		emit(record, targetChannelIndex);
		randomTriggered = false;

		if (bufferBuilder != null) {
			for (int index = 0; index < numberOfChannels; index++) {
				if (index != targetChannelIndex) {
					targetPartition.addBufferConsumer(bufferBuilder.createBufferConsumer(), index);
				}
			}
		}
	}

	@Override
	public void broadcastEmit(T record) throws IOException, InterruptedException {
		// We could actually select any target channel here because all the channels
		// are sharing the same BufferBuilder in broadcast mode.
		emit(record, 0);
	}

	/**
	 * The flush could be triggered by {@link #randomEmit(IOReadableWritable)}, {@link #emit(IOReadableWritable)}
	 * or {@link #broadcastEmit(IOReadableWritable)}. Only random emit should flush a single target channel,
	 * otherwise we should flush all the channels.
	 */
	@Override
	public void flushTargetPartition(int targetChannel) {
		if (randomTriggered) {
			super.flushTargetPartition(targetChannel);
		} else {
			flushAll();
		}
	}

	@Override
	public BufferBuilder getBufferBuilder(int targetChannel) throws IOException, InterruptedException {
		return bufferBuilder != null ? bufferBuilder : requestNewBufferBuilder(targetChannel);
	}

	/**
	 * The request could be from broadcast or non-broadcast modes like {@link #randomEmit(IOReadableWritable)}.
	 *
	 * <p>For non-broadcast, the created {@link BufferConsumer} is only for the target channel.
	 *
	 * <p>For broadcast, all the channels share the same requested {@link BufferBuilder} and the created
	 * {@link BufferConsumer} is copied for every channel.
	 */
	@Override
	public BufferBuilder requestNewBufferBuilder(int targetChannel) throws IOException, InterruptedException {
		checkState(bufferBuilder == null || bufferBuilder.isFinished());

		BufferBuilder builder = targetPartition.getBufferBuilder();
		if (randomTriggered) {
			targetPartition.addBufferConsumer(builder.createBufferConsumer(), targetChannel);
		} else {
			try (BufferConsumer bufferConsumer = builder.createBufferConsumer()) {
				for (int channel = 0; channel < numberOfChannels; channel++) {
					targetPartition.addBufferConsumer(bufferConsumer.copy(), channel);
				}
			}
		}

		bufferBuilder = builder;
		return builder;
	}

	@Override
	public void tryFinishCurrentBufferBuilder(int targetChannel) {
		if (bufferBuilder == null) {
			return;
		}

		BufferBuilder builder = bufferBuilder;
		bufferBuilder = null;

		finishBufferBuilder(builder);
	}

	@Override
	public void emptyCurrentBufferBuilder(int targetChannel) {
		bufferBuilder = null;
	}

	@Override
	public void closeBufferBuilder(int targetChannel) {
		closeBufferBuilder();
	}

	@Override
	public void clearBuffers() {
		closeBufferBuilder();
	}

	private void closeBufferBuilder() {
		if (bufferBuilder != null) {
			bufferBuilder.finish();
			bufferBuilder = null;
		}
	}
}
