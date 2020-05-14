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
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;

import java.io.Closeable;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/**
 * Writes channel state during checkpoint/savepoint.
 */
@Internal
public interface ChannelStateWriter extends Closeable {

	/**
	 * Channel state write result.
	 */
	class ChannelStateWriteResult {
		final CompletableFuture<Collection<InputChannelStateHandle>> inputChannelStateHandles;
		final CompletableFuture<Collection<ResultSubpartitionStateHandle>> resultSubpartitionStateHandles;

		ChannelStateWriteResult() {
			this(new CompletableFuture<>(), new CompletableFuture<>());
		}

		ChannelStateWriteResult(
				CompletableFuture<Collection<InputChannelStateHandle>> inputChannelStateHandles,
				CompletableFuture<Collection<ResultSubpartitionStateHandle>> resultSubpartitionStateHandles) {
			this.inputChannelStateHandles = inputChannelStateHandles;
			this.resultSubpartitionStateHandles = resultSubpartitionStateHandles;
		}

		public CompletableFuture<Collection<InputChannelStateHandle>> getInputChannelStateHandles() {
			return inputChannelStateHandles;
		}

		public CompletableFuture<Collection<ResultSubpartitionStateHandle>> getResultSubpartitionStateHandles() {
			return resultSubpartitionStateHandles;
		}

		public static final ChannelStateWriteResult EMPTY = new ChannelStateWriteResult(
			CompletableFuture.completedFuture(Collections.emptyList()),
			CompletableFuture.completedFuture(Collections.emptyList())
		);

		public void fail(Throwable e) {
			inputChannelStateHandles.completeExceptionally(e);
			resultSubpartitionStateHandles.completeExceptionally(e);
		}

		boolean isDone() {
			return inputChannelStateHandles.isDone() && resultSubpartitionStateHandles.isDone();
		}
	}

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
	void start(long checkpointId, CheckpointOptions checkpointOptions);

	/**
	 * Add in-flight buffers from the {@link org.apache.flink.runtime.io.network.partition.consumer.InputChannel InputChannel}.
	 * Must be called after {@link #start} (long)} and before {@link #finishInput(long)}.
	 * Buffers are recycled after they are written or exception occurs.
	 * @param startSeqNum sequence number of the 1st passed buffer.
	 *                    It is intended to use for incremental snapshots.
	 *                    If no data is passed it is ignored.
	 * @param data zero or more <b>data</b> buffers ordered by their sequence numbers
	 * @throws IllegalArgumentException if one or more passed buffers {@link Buffer#isBuffer()  isn't a buffer}
	 * @see org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter#SEQUENCE_NUMBER_RESTORED
	 * @see org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter#SEQUENCE_NUMBER_UNKNOWN
	 */
	void addInputData(long checkpointId, InputChannelInfo info, int startSeqNum, Buffer... data) throws IllegalArgumentException;

	/**
	 * Add in-flight buffers from the {@link org.apache.flink.runtime.io.network.partition.ResultSubpartition ResultSubpartition}.
	 * Must be called after {@link #start} and before {@link #finishOutput(long)}.
	 * Buffers are recycled after they are written or exception occurs.
	 * @param startSeqNum sequence number of the 1st passed buffer.
	 *                    It is intended to use for incremental snapshots.
	 *                    If no data is passed it is ignored.
	 * @param data zero or more <b>data</b> buffers ordered by their sequence numbers
	 * @throws IllegalArgumentException if one or more passed buffers {@link Buffer#isBuffer()  isn't a buffer}
	 * @see org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter#SEQUENCE_NUMBER_RESTORED
	 * @see org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter#SEQUENCE_NUMBER_UNKNOWN
	 */
	void addOutputData(long checkpointId, ResultSubpartitionInfo info, int startSeqNum, Buffer... data) throws IllegalArgumentException;

	/**
	 * Finalize write of channel state data for the given checkpoint id.
	 * Must be called after {@link #start(long, CheckpointOptions)} and all of the input data of the given checkpoint added.
	 * When both {@link #finishInput} and {@link #finishOutput} were called the results can be (eventually) obtained
	 * using {@link #getWriteResult}
	 */
	void finishInput(long checkpointId);

	/**
	 * Finalize write of channel state data for the given checkpoint id.
	 * Must be called after {@link #start(long, CheckpointOptions)} and all of the output data of the given checkpoint added.
	 * When both {@link #finishInput} and {@link #finishOutput} were called the results can be (eventually) obtained
	 * using {@link #getWriteResult}
	 */
	void finishOutput(long checkpointId);

	/**
	 * Aborts the checkpoint and fails pending result for this checkpoint.
	 */
	void abort(long checkpointId, Throwable cause);

	/**
	 * Must be called after {@link #start(long, CheckpointOptions)}.
	 */
	ChannelStateWriteResult getWriteResult(long checkpointId);

	/**
	 * Cleans up the internal state for the given checkpoint.
	 */
	void stop(long checkpointId);

	ChannelStateWriter NO_OP = new NoOpChannelStateWriter();

	/**
	 * No-op implementation of {@link ChannelStateWriter}.
	 */
	class NoOpChannelStateWriter implements ChannelStateWriter {
		@Override
		public void start(long checkpointId, CheckpointOptions checkpointOptions) {
		}

		@Override
		public void addInputData(long checkpointId, InputChannelInfo info, int startSeqNum, Buffer... data) {
		}

		@Override
		public void addOutputData(long checkpointId, ResultSubpartitionInfo info, int startSeqNum, Buffer... data) {
		}

		@Override
		public void finishInput(long checkpointId) {
		}

		@Override
		public void finishOutput(long checkpointId) {
		}

		@Override
		public void abort(long checkpointId, Throwable cause) {
		}

		@Override
		public ChannelStateWriteResult getWriteResult(long checkpointId) {
			return ChannelStateWriteResult.EMPTY;
		}

		@Override
		public void close() {
		}

		@Override
		public void stop(long checkpointId) {
		}
	}
}
