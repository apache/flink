package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkState;

public class PipelinedApproximateSubpartition extends PipelinedSubpartition {

	private static final Logger LOG = LoggerFactory.getLogger(PipelinedApproximateSubpartition.class);

	private boolean isPartialBuffer = false;

	PipelinedApproximateSubpartition(int index, ResultPartition parent) {
		super(index, parent);
	}

	public void releaseView() {
		readView = null;
		isPartialBuffer = true;
		isBlockedByCheckpoint = false;
		sequenceNumber = 0;
	}

	@Override
	public PipelinedSubpartitionView createReadView(BufferAvailabilityListener availabilityListener) {
		synchronized (buffers) {
			checkState(!isReleased);

			// if the view is not released yet
			if (readView != null) {
				releaseView();
			}

			LOG.debug("{}: Creating read view for subpartition {} of partition {}.",
				parent.getOwningTaskName(), getSubPartitionIndex(), parent.getPartitionId());

			readView = new PipelinedApproximateSubpartitionView(this, availabilityListener);
		}

		return readView;
	}

	@Nullable
	@Override
	BufferAndBacklog pollBuffer() {
		synchronized (buffers) {
			if (isBlockedByCheckpoint) {
				return null;
			}

			Buffer buffer = null;

			if (buffers.isEmpty()) {
				flushRequested = false;
			}

			while (!buffers.isEmpty()) {
				BufferConsumer bufferConsumer = buffers.peek();

				// `isPartialBuffer` is set to true in the same Netty thread when ResultPartitionView is released
				if (isPartialBuffer) {
					BufferConsumer.PartialRecordCleanupResult result = bufferConsumer.skipPartialRecord();
					buffer = result.getBuffer();
					isPartialBuffer = !result.getCleaned();
				} else {
					buffer = bufferConsumer.build();
				}

				checkState(bufferConsumer.isFinished() || buffers.size() == 1,
					"When there are multiple buffers, an unfinished bufferConsumer can not be at the head of the buffers queue.");

				if (buffers.size() == 1) {
					// turn off flushRequested flag if we drained all of the available data
					flushRequested = false;
				}

				if (bufferConsumer.isFinished()) {
					buffers.poll().close();
					decreaseBuffersInBacklogUnsafe(bufferConsumer.isBuffer());
				}

				if (buffer.readableBytes() > 0) {
					break;
				}

				buffer.recycleBuffer();
				buffer = null;

				if (!bufferConsumer.isFinished()) {
					break;
				}
			}

			if (buffer == null) {
				return null;
			}

			if (buffer.getDataType().isBlockingUpstream()) {
				isBlockedByCheckpoint = true;
			}

			updateStatistics(buffer);
			// Do not report last remaining buffer on buffers as available to read (assuming it's unfinished).
			// It will be reported for reading either on flush or when the number of buffers in the queue
			// will be 2 or more.
			return new BufferAndBacklog(
				buffer,
				getBuffersInBacklog(),
				isDataAvailableUnsafe() ? getNextBufferTypeUnsafe() : Buffer.DataType.NONE,
				sequenceNumber++);
		}
	}

	@Override
	public String toString() {
		final long numBuffers;
		final long numBytes;
		final boolean finished;
		final boolean hasReadView;

		synchronized (buffers) {
			numBuffers = getTotalNumberOfBuffers();
			numBytes = getTotalNumberOfBytes();
			finished = isFinished;
			hasReadView = readView != null;
		}

		return String.format(
			"PipelinedApproximateSubpartition#%d [number of buffers: %d (%d bytes), number of buffers in backlog: %d, finished? %s, read view? %s]",
			getSubPartitionIndex(), numBuffers, numBytes, getBuffersInBacklog(), finished, hasReadView);
	}
}
