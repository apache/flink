/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.state.StateObject;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * Writes channel state during checkpoint/savepoint.
 */
@Internal
public interface ChannelStateWriter extends AutoCloseable {

	/**
	 * Sequence number for the buffers that were saved during the previous execution attempt; then restored; and now are
	 * to be saved again (as opposed to the buffers received from the upstream or from the operator).
	 */
	int SEQUENCE_NUMBER_RESTORED = -1;

	/**
	 * Signifies that buffer sequence number is unknown (e.g. if passing sequence numbers is not implemented).
	 */
	int SEQUENCE_NUMBER_UNKNOWN = -2;

	/**
	 * Initiate write of channel state for the given checkpoint id.
	 */
	void start(long checkpointId);

	/**
	 * Add in-flight buffers from the {@link org.apache.flink.runtime.io.network.partition.consumer.InputChannel InputChannel}.
	 * Must be called after {@link #start(long)} and before {@link #finish(long)}.
	 * @param startSeqNum sequence number of the 1st passed buffer.
	 *                    It is intended to use for incremental snapshots.
	 *                    If no data is passed it is ignored.
	 * @param data zero or more buffers ordered by their sequence numbers
	 * @see org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter#SEQUENCE_NUMBER_RESTORED
	 * @see org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter#SEQUENCE_NUMBER_UNKNOWN
	 */
	void addInputData(long checkpointId, InputChannelInfo info, int startSeqNum, Buffer... data);

	/**
	 * Add in-flight buffers from the {@link org.apache.flink.runtime.io.network.partition.ResultSubpartition ResultSubpartition}.
	 * Must be called after {@link #start(long)} and before {@link #finish(long)}.
	 * @param startSeqNum sequence number of the 1st passed buffer.
	 *                    It is intended to use for incremental snapshots.
	 *                    If no data is passed it is ignored.
	 * @param data zero or more buffers ordered by their sequence numbers
	 * @see org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter#SEQUENCE_NUMBER_RESTORED
	 * @see org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter#SEQUENCE_NUMBER_UNKNOWN
	 */
	void addOutputData(long checkpointId, ResultSubpartitionInfo info, int startSeqNum, Buffer... data);

	/**
	 * Finalize write of channel state for the given checkpoint id.
	 * Must be called after {@link #start(long)} and all of the data of the given checkpoint added.
	 */
	void finish(long checkpointId);

	/**
	 * Must be called after {@link #start(long)}.
	 */
	Future<Collection<StateObject>> getWriteCompletionFuture(long checkpointId);

	@Override
	void close() throws Exception;

	ChannelStateWriter NO_OP = new ChannelStateWriter() {

		@Override
		public void start(long checkpointId) {
		}

		@Override
		public void addInputData(long checkpointId, InputChannelInfo info, int startSeqNum, Buffer... data) {
		}

		@Override
		public void addOutputData(long checkpointId, ResultSubpartitionInfo info, int startSeqNum, Buffer... data) {
		}

		@Override
		public void finish(long checkpointId) {
		}

		@Override
		public Future<Collection<StateObject>> getWriteCompletionFuture(long checkpointId) {
			return CompletableFuture.completedFuture(Collections.emptyList());
		}

		@Override
		public void close() {
		}
	};

}
