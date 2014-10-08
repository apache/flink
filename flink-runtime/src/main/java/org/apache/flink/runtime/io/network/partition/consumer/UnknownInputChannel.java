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

import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.RemoteAddress;
import org.apache.flink.runtime.io.network.api.reader.BufferReader;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An input channel place holder to be replaced by either a {@link RemoteInputChannel}
 * or {@link LocalInputChannel} at runtime.
 */
public class UnknownInputChannel extends InputChannel {

	public UnknownInputChannel(int channelIndex, ExecutionAttemptID producerExecutionId, IntermediateResultPartitionID partitionId, BufferReader reader) {
		super(channelIndex, producerExecutionId, partitionId, reader);
	}

	@Override
	public void requestIntermediateResultPartition(int queueIndex) throws IOException {
		// Nothing to do here
	}

	@Override
	public Buffer getNextBuffer() throws IOException {
		// Nothing to do here
		return null;
	}

	@Override
	public void sendTaskEvent(TaskEvent event) throws IOException {
		// Nothing to do here
	}

	/**
	 * Returns <code>false</code>.
	 * <p>
	 * <strong>Important</strong>: It is important that the method correctly
	 * always <code>false</code> for unknown input channels in order to not
	 * finish the consumption of an intermediate result partition early in
	 * {@link BufferReader}.
	 */
	@Override
	public boolean isReleased() {
		return false;
	}

	@Override
	public void releaseAllResources() throws IOException {
		// Nothing to do here
	}

	@Override
	public String toString() {
		return "UNKNOWN " + super.toString();
	}

	// ------------------------------------------------------------------------
	// Graduation to a local or remote input channel at runtime
	// ------------------------------------------------------------------------

	public RemoteInputChannel toRemoteInputChannel(RemoteAddress producerAddress) {
		return new RemoteInputChannel(channelIndex, producerExecutionId, partitionId, reader, checkNotNull(producerAddress));
	}

	public LocalInputChannel toLocalInputChannel() {
		return new LocalInputChannel(channelIndex, producerExecutionId, partitionId, reader);
	}
}
