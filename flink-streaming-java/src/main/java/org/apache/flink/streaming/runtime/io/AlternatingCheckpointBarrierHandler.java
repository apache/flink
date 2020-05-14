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
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.buffer.BufferReceivedListener;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;
import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.CHECKPOINT_DECLINED_SUBSUMED;

class AlternatingCheckpointBarrierHandler extends CheckpointBarrierHandler {
	private final CheckpointBarrierAligner alignedHandler;
	private final CheckpointBarrierUnaligner unalignedHandler;
	private CheckpointBarrierHandler activeHandler;

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
	public boolean isBlocked(int channelIndex) {
		return activeHandler.isBlocked(channelIndex);
	}

	@Override
	public void processBarrier(CheckpointBarrier receivedBarrier, int channelIndex) throws Exception {
		CheckpointBarrierHandler previousHandler = activeHandler;
		activeHandler = receivedBarrier.isCheckpoint() ? unalignedHandler : alignedHandler;
		abortPreviousIfNeeded(receivedBarrier, previousHandler);
		activeHandler.processBarrier(receivedBarrier, channelIndex);
	}

	private void abortPreviousIfNeeded(CheckpointBarrier barrier, CheckpointBarrierHandler prevHandler) throws IOException {
		if (prevHandler != activeHandler && prevHandler.isCheckpointPending() && prevHandler.getLatestCheckpointId() < barrier.getId()) {
			prevHandler.releaseBlocksAndResetBarriers();
			notifyAbort(
				prevHandler.getLatestCheckpointId(),
				new CheckpointException(
					format("checkpoint %d subsumed by %d", prevHandler.getLatestCheckpointId(), barrier.getId()),
					CHECKPOINT_DECLINED_SUBSUMED));
		}
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
		return alignedHandler.getAlignmentDurationNanos();
	}

	@Override
	public boolean hasInflightData(long checkpointId, int channelIndex) {
		return activeHandler.hasInflightData(checkpointId, channelIndex);
	}

	@Override
	public CompletableFuture<Void> getAllBarriersReceivedFuture(long checkpointId) {
		return activeHandler.getAllBarriersReceivedFuture(checkpointId);
	}

	@Override
	public Optional<BufferReceivedListener> getBufferReceivedListener() {
		return unalignedHandler.getBufferReceivedListener();
	}

	@Override
	protected boolean isCheckpointPending() {
		return activeHandler.isCheckpointPending();
	}
}
