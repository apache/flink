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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A dummy implementation of the {@link InputChannel}. We implement this here rather than using Mockito
 * to avoid using mockito in this benchmark class.
 */
public final class IterableInputChannel extends InputChannel {

	private final List<Buffer> dataBuffers;

	private final int maxBufferIndex;

	private int nextBufferIndex;

	public IterableInputChannel(
		SingleInputGate inputGate,
		int channelIndex,
		List<Buffer> dataBuffers) {

		super(inputGate, channelIndex, new ResultPartitionID(), 0, 0, new SimpleCounter(), new SimpleCounter());

		checkState(dataBuffers != null && dataBuffers.size() > 0);
		this.dataBuffers = dataBuffers;
		this.maxBufferIndex = dataBuffers.size() - 1;

		this.nextBufferIndex = 0;

		// notify the owning SingleInputGate that this channel became non-empty
		this.inputGate.notifyChannelNonEmpty(this);
	}

	@Override
	Optional<BufferAndAvailability> getNextBuffer() {
		if (nextBufferIndex <= maxBufferIndex)  {
			Buffer nextBuffer = dataBuffers.get(nextBufferIndex);
			boolean moreAvailable = (nextBufferIndex < maxBufferIndex);

			nextBufferIndex++;
			return Optional.of(new BufferAndAvailability(nextBuffer, moreAvailable, 0));
		}

		return Optional.empty();
	}

	/**
	 * Supports consuming the data buffers multiple times by resetting its initial buffer index.
	 */
	public void reset() throws IOException {
		if (nextBufferIndex > maxBufferIndex) {
			nextBufferIndex = 0;

			// increase reference counts for buffers
			for (int i = 0; i < dataBuffers.size(); i++) {
				dataBuffers.get(i).retainBuffer();
			}

			// notify the owning SingleInputGate that this channel became non-empty
			inputGate.notifyChannelNonEmpty(this);
		} else {
			throw new IOException("The data for the last iteration is not fully consumed.");
		}
	}

	@Override
	void requestSubpartition(int subpartitionIndex) {

	}

	@Override
	void sendTaskEvent(TaskEvent event) {

	}

	@Override
	boolean isReleased() {
		return false;
	}

	@Override
	void notifySubpartitionConsumed() {

	}

	@Override
	void releaseAllResources() {

	}
}
