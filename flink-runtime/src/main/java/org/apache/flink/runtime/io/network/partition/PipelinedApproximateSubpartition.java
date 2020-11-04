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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumerWithPartialRecordLength;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A pipelined in-memory only subpartition, which allows to reconnecting after failure.
 * Only one view is allowed at a time to read teh subpartition.
 */
public class PipelinedApproximateSubpartition extends PipelinedSubpartition {

	private static final Logger LOG = LoggerFactory.getLogger(PipelinedApproximateSubpartition.class);

	@GuardedBy("buffers")
	private boolean isPartialBufferCleanupRequired = false;

	PipelinedApproximateSubpartition(int index, ResultPartition parent) {
		super(index, parent);
	}

	/**
	 * To simply the view releasing threading model, {@link PipelinedApproximateSubpartition#releaseView()} is called
	 * only before creating a new view.
	 *
	 * <p>There is still one corner case when a downstream task fails continuously in a short period of time
	 * then multiple netty worker threads can createReadView at the same time.
	 * TODO: This problem will be solved in FLINK-19774
	 */
	@Override
	public PipelinedSubpartitionView createReadView(BufferAvailabilityListener availabilityListener) {
		synchronized (buffers) {
			checkState(!isReleased);

			releaseView();

			LOG.debug("{}: Creating read view for subpartition {} of partition {}.",
				parent.getOwningTaskName(), getSubPartitionIndex(), parent.getPartitionId());

			readView = new PipelinedApproximateSubpartitionView(this, availabilityListener);
		}

		return readView;
	}

	@Override
	Buffer buildSliceBuffer(BufferConsumerWithPartialRecordLength buffer) {
		if (isPartialBufferCleanupRequired) {
			isPartialBufferCleanupRequired = !buffer.cleanupPartialRecord();
		}

		return buffer.build();
	}

	private void releaseView() {
		assert Thread.holdsLock(buffers);
		if (readView != null) {
			// upon reconnecting, two netty threads may require the same view to release
			LOG.debug("Releasing view of subpartition {} of {}.", getSubPartitionIndex(), parent.getPartitionId());

			readView.releaseAllResources();
			readView = null;

			isPartialBufferCleanupRequired = true;
			isBlocked = false;
			sequenceNumber = 0;
		}
	}

	@Override
	public void finishReadRecoveredState(boolean notifyAndBlockOnCompletion) throws IOException {
		// The Approximate Local Recovery can not work with unaligned checkpoint for now, so no need to recover channel state
	}

	/** for testing only. */
	@VisibleForTesting
	boolean isPartialBufferCleanupRequired() {
		return isPartialBufferCleanupRequired;
	}

	/** for testing only. */
	@VisibleForTesting
	void setIsPartialBufferCleanupRequired() {
		isPartialBufferCleanupRequired = true;
	}
}
