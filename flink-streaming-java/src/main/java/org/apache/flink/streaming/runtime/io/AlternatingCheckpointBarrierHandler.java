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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.buffer.BufferReceivedListener;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

import org.apache.flink.shaded.guava18.com.google.common.io.Closer;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;
import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.CHECKPOINT_DECLINED_SUBSUMED;

class AlternatingCheckpointBarrierHandler extends CheckpointBarrierHandler {
	private final CheckpointBarrierAligner alignedHandler;
	private final CheckpointBarrierUnaligner unalignedHandler;
	private CheckpointBarrierHandler activeHandler;
	private long lastSeenBarrierId;

	AlternatingCheckpointBarrierHandler(CheckpointBarrierAligner alignedHandler, CheckpointBarrierUnaligner unalignedHandler, AbstractInvokable invokable) {
		super(invokable);
		this.activeHandler = this.alignedHandler = alignedHandler;
		this.unalignedHandler = unalignedHandler;
	}

	@Override
	public void releaseBlocksAndResetBarriers() {
		activeHandler.releaseBlocksAndResetBarriers();
	}

	@Override
	public boolean isBlocked(InputChannelInfo channelInfo) {
		return activeHandler.isBlocked(channelInfo);
	}

	@Override
	public void processBarrier(CheckpointBarrier receivedBarrier, InputChannelInfo channelInfo) throws Exception {
		if (receivedBarrier.getId() < lastSeenBarrierId) {
			return;
		}

		lastSeenBarrierId = receivedBarrier.getId();
		CheckpointBarrierHandler previousHandler = activeHandler;
		activeHandler = receivedBarrier.isCheckpoint() ? unalignedHandler : alignedHandler;
		if (previousHandler != activeHandler) {
			previousHandler.abortPendingCheckpoint(
				lastSeenBarrierId,
				new CheckpointException(format("checkpoint subsumed by %d", lastSeenBarrierId), CHECKPOINT_DECLINED_SUBSUMED));
		}

		activeHandler.processBarrier(receivedBarrier, channelInfo);
	}

	@Override
	public void processCancellationBarrier(CancelCheckpointMarker cancelBarrier) throws Exception {
		activeHandler.processCancellationBarrier(cancelBarrier);
	}

	@Override
	public void processEndOfPartition() throws Exception {
		alignedHandler.processEndOfPartition();
		unalignedHandler.processEndOfPartition();
	}

	@Override
	public long getLatestCheckpointId() {
		return activeHandler.getLatestCheckpointId();
	}

	@Override
	public long getAlignmentDurationNanos() {
		return activeHandler.getAlignmentDurationNanos();
	}

	@Override
	public boolean hasInflightData(long checkpointId, InputChannelInfo channelInfo) {
		// should only be called for unaligned checkpoint
		return unalignedHandler.hasInflightData(checkpointId, channelInfo);
	}

	@Override
	public CompletableFuture<Void> getAllBarriersReceivedFuture(long checkpointId) {
		// should only be called for unaligned checkpoint
		return unalignedHandler.getAllBarriersReceivedFuture(checkpointId);
	}

	@Override
	public Optional<BufferReceivedListener> getBufferReceivedListener() {
		// should only be used for handling unaligned checkpoints
		return unalignedHandler.getBufferReceivedListener();
	}

	@Override
	protected boolean isCheckpointPending() {
		return activeHandler.isCheckpointPending();
	}

	@Override
	public void close() throws IOException {
		try (Closer closer = Closer.create()) {
			closer.register(alignedHandler);
			closer.register(unalignedHandler);
			closer.register(super::close);
		}
	}
}
