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

import com.google.common.base.Optional;
import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.io.network.BufferOrEvent;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.api.reader.BufferReader;
import org.apache.flink.runtime.io.network.partition.IntermediateResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.IntermediateResultPartitionQueueIterator;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkState;

public class LocalInputChannel extends InputChannel {

	private final IntermediateResultPartitionProvider partitionProvider;

	private final TaskEventDispatcher taskEventDispatcher;

	private IntermediateResultPartitionQueueIterator queueIterator;

	private boolean isFinished;

	public LocalInputChannel(
			int channelIndex, IntermediateResultPartitionID partitionId, BufferReader reader,
			IntermediateResultPartitionProvider partitionProvider, TaskEventDispatcher taskEventDispatcher) {

		super(channelIndex, partitionId, reader);

		this.partitionProvider = partitionProvider;
		this.taskEventDispatcher = taskEventDispatcher;
	}

	public void initialize() {
		if (queueIterator != null) {
			queueIterator = partitionProvider.getIntermediateResultPartitionIterator(
					partitionId, channelIndex, Optional.of(reader.getBufferProvider()));

			if (queueIterator == null) {
				throw new IllegalStateException("Local intermediate result partition queue does not exist.");
			}

			if (queueIterator.hasNext()) {
				notifyReaderAboutAvailableBufferOrEvent();
			}
		}
	}

	@Override
	public BufferOrEvent getNextBufferOrEvent() throws IOException {
		checkState(!isFinished, "Input channel has already been finished.");
		checkState(queueIterator.hasNext(), "Queried local input channel even though no data is available.");

		// If there is further data available, notify the reader. Note that
		// this is not a notification about the current buffer or event, but
		// about whether there is another one *after* this one.
		if (queueIterator.hasNext()) {
			notifyReaderAboutAvailableBufferOrEvent();
		}

		return queueIterator.getNextBufferOrEvent();
	}

	@Override
	public void sendTaskEvent(TaskEvent event) throws IOException {
		if (!taskEventDispatcher.publish(partitionId, event)) {
			throw new IOException("Could not publish event");
		}
	}

	@Override
	public void finish() {
		if (!isFinished) {
			if (queueIterator.hasNext()) {
				queueIterator.discard();
			}

			isFinished = true;
		}
	}

	@Override
	public boolean isFinished() {
		return isFinished;
	}

	@Override
	public void releaseAllResources() {
		finish();
	}
}
