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

package org.apache.flink.runtime.io.network.partition.external;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.SynchronousBufferFileReader;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.DataConsumptionException;
import org.apache.flink.runtime.io.network.partition.FixedLengthBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Reader for a subpartition of an external result partition.
 */
public class ExternalBlockSubpartitionView implements ResultSubpartitionView, Runnable {
	private static final Logger LOG = LoggerFactory.getLogger(ExternalBlockSubpartitionView.class);

	private final ExternalBlockResultPartitionMeta externalResultPartitionMeta;

	private final int subpartitionIndex;

	private final ExecutorService threadPool;

	/** The result partition id used to send data consumption exception. */
	private final ResultPartitionID resultPartitionId;

	/** The buffer pool to read data into. */
	private final FixedLengthBufferPool bufferPool;

	/**
	 * Timeout of waiting for more credit to arrive after reading stopped due to credit consumed. If it
	 * is less than 0, the view will wait forever, if it is 0, the view will not wait and if it is larger
	 * than 0, the view will wait for the given period.
	 */
	private final long waitCreditTimeoutInMills;

	/** The number of total length in bytes for this subpartition. */
	private long totalLength;

	/** The lock to guard the state of this view */
	private final Object lock = new Object();

	/**
	 * The buffers are filled by io thread and ready to be fetched by netty thread.
	 * Access to the buffers is synchronized on this object.
	 */
	@GuardedBy("lock")
	private final ArrayDeque<Buffer> buffers = new ArrayDeque<>();

	/** Flag indicating whether the view has been released. */
	@GuardedBy("lock")
	private volatile Throwable cause;

	/** The length in bytes of the data that has already been read. */
	private long totalReadLength = 0;

	/** Iterator of subpartition metas. */
	private Iterator<ExternalBlockResultPartitionMeta.ExternalSubpartitionMeta> metaIterator;

	/** The current input stream of distributed or local file systems. */
	private SynchronousBufferFileReader currFsIn = null;

	/** Remaining length in bytes to read for the current spill file. */
	private long currRemainLength = 0;

	/** The listener used to be notified how many buffers are available for transferring. */
	private final BufferAvailabilityListener listener;

	/** Flag indicating whether the view has been released. */
	@GuardedBy("lock")
	private volatile boolean isReleased;

	/** Flag indicating whether the view is running. */
	@GuardedBy("lock")
	private boolean isRunning;

	/** The current unused credit. */
	@GuardedBy("lock")
	private volatile int currentCredit = 0;

	public ExternalBlockSubpartitionView(
			ExternalBlockResultPartitionMeta externalResultPartitionMeta,
			int subpartitionIndex,
			ExecutorService threadPool,
			ResultPartitionID resultPartitionId,
			FixedLengthBufferPool bufferPool,
			long waitCreditTimeoutInMills,
			BufferAvailabilityListener listener) {

		this.externalResultPartitionMeta = checkNotNull(externalResultPartitionMeta);
		this.subpartitionIndex = subpartitionIndex;
		this.threadPool = checkNotNull(threadPool);
		this.resultPartitionId = checkNotNull(resultPartitionId);
		this.bufferPool = checkNotNull(bufferPool);
		this.waitCreditTimeoutInMills = waitCreditTimeoutInMills;
		this.listener = checkNotNull(listener);
	}

	@Override
	public void run() {
		synchronized (lock) {
			checkState(!isRunning, "All the previous instances should be already exited.");

			if (isReleased) {
				return;
			}

			isRunning = true;
		}

		try {
			if (metaIterator == null) {
				initializeMeta();
			}

			if (totalLength == 0) {
				enqueueBuffer(EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE));
				return;
			}

			while (true) {
				while (isAvailableForReadUnsafe()) {
					Buffer buffer = readNextBuffer();
					enqueueBuffer(buffer);
				}

				// Check whether we need to wait for credit feedback before exiting.
				if (waitCreditTimeoutInMills == 0 || !hasMoreDataToReadUnsafe()) {
					break;
				}

				synchronized (lock) {
					if (isReleased) {
						return;
					}

					if (waitCreditTimeoutInMills < 0) {
						// The waiting should only be interrupted by credit arrival or released.
						lock.wait();
					} else {
						// Since the waiting should only be interrupted by credit arrival, released
						// or timeout, we wait only once directly instead of tracking the actual waiting
						// time and using iteration to ensure waitCreditTimeoutInMills milliseconds elapse.
						lock.wait(waitCreditTimeoutInMills);

						if (!isAvailableForReadUnsafe()) {
							break;
						}
					}
				}
			}
		} catch (Throwable t) {
			LOG.error("Exception during reading {}", this, t);

			releaseAllResources(new DataConsumptionException(resultPartitionId, t));

			// We should notify the handler the error in order to further notify the consumer.
			listener.notifyDataAvailable();
		} finally {
			synchronized (lock) {
				if (isReleased) {
					closeCurrentFileReader();
				}

				isRunning = false;

				if (isAvailableForReadUnsafe()) {
					threadPool.submit(this);
				}
			}
		}
	}

	private void initializeMeta() throws IOException {
		if (!externalResultPartitionMeta.hasInitialized()) {
			externalResultPartitionMeta.initialize();
		}

		List<ExternalBlockResultPartitionMeta.ExternalSubpartitionMeta> subpartitionMetas = externalResultPartitionMeta.getSubpartitionMeta(subpartitionIndex);
		metaIterator = subpartitionMetas.iterator();

		for (ExternalBlockResultPartitionMeta.ExternalSubpartitionMeta meta : subpartitionMetas) {
			totalLength += meta.getLength();
		}
	}

	private boolean isAvailableForReadUnsafe() {
		return hasMoreDataToReadUnsafe() && currentCredit > 0;
	}

	private boolean hasMoreDataToReadUnsafe() {
		return !isReleased && totalReadLength < totalLength;
	}

	/**
	 * Reads the next buffer from the view.
	 *
	 * @return true if the view is available for reading next time.
	 */
	@Nonnull
	private Buffer readNextBuffer() throws IOException, InterruptedException {
		if (currFsIn == null) {
			currFsIn = getNextFileReader();
		}
		checkState(currFsIn != null, "No more data to read.");

		Buffer buffer = bufferPool.requestBufferBlocking();
		checkState(buffer != null, "Failed to request a buffer.");

		checkState(currRemainLength > 0, "Should have data to read from the current file.");

		try {
			long lengthToRead = Math.min(currRemainLength, buffer.getMaxCapacity());
			currFsIn.readInto(buffer, lengthToRead);
			currRemainLength -= lengthToRead;
		} catch (Throwable t) {
			if (!buffer.isRecycled()) {
				buffer.recycleBuffer();
			}
			throw t;
		}

		if (currRemainLength == 0) {
			closeCurrentFileReader();
		}

		return buffer;
	}

	private void closeCurrentFileReader() {
		if (currFsIn != null) {
			try {
				currFsIn.close();
			} catch (IOException ioe) {
				LOG.error("Ignore the close file exception.", ioe);
			}
			currFsIn = null;
		}
	}

	private SynchronousBufferFileReader getNextFileReader() throws IOException {
		SynchronousBufferFileReader nextFsIn = null;

		ExternalBlockResultPartitionMeta.ExternalSubpartitionMeta nextMeta;
		while (metaIterator.hasNext()) {
			nextMeta = metaIterator.next();
			currRemainLength = nextMeta.getLength();
			if (currRemainLength > 0) {
				FileIOChannel.ID fileChannelID = new FileIOChannel.ID(nextMeta.getDataFile().getPath());
				nextFsIn = new SynchronousBufferFileReader(fileChannelID, false, false);
				nextFsIn.seekToPosition(nextMeta.getOffset());

				break;
			}
		}

		return nextFsIn;
	}

	private void enqueueBuffer(Buffer buffer) throws IOException {
		synchronized (lock) {
			if (isReleased) {
				buffer.recycleBuffer();
				return;
			}

			buffers.add(buffer);

			if (buffer.isBuffer()) {
				--currentCredit;
				totalReadLength += buffer.getSize();
			}

			// If EOF is enqueued directly, the condition will fail and there will be no second EOF enqueued.
			if (totalReadLength == totalLength && totalLength != 0) {
				buffers.add(EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE));
			}
		}

		listener.notifyDataAvailable();
	}

	@Override
	public ResultSubpartition.BufferAndBacklog getNextBuffer() {
		synchronized (lock) {
			Buffer buffer = buffers.poll();
			Buffer nextBuffer = buffers.peek();
			if (buffer != null) {
				// If buffer is read, there must be no exceptions occur and cause is null.
				return new ResultSubpartition.BufferAndBacklog(buffer, nextBuffer != null, buffers.size(),
					nextBuffer != null && !nextBuffer.isBuffer());
			} else {
				return null;
			}
		}
	}

	@Override
	public boolean nextBufferIsEvent() {
		synchronized (lock) {
			if (cause != null) {
				checkState(buffers.size() == 0,
					"All the buffer should be cleared after errors occur and released.");
				return true;
			} else {
				return buffers.size() > 0 && !buffers.peek().isBuffer();
			}
		}
	}

	@Override
	public boolean isAvailable() {
		synchronized (lock) {
			return buffers.size() > 0 || cause != null;
		}
	}

	@Override
	public void notifyCreditAdded(int creditDeltas) {
		synchronized (lock) {
			int creditBeforeAdded = currentCredit;
			currentCredit += creditDeltas;

			if (creditBeforeAdded == 0) {
				if (!isRunning) {
					threadPool.submit(this);
				} else {
					lock.notifyAll();
				}
			}
		}
	}

	int getCreditUnsafe() {
		return currentCredit;
	}

	String getResultPartitionDir() {
		return externalResultPartitionMeta.getResultPartitionDir();
	}

	int getSubpartitionIndex() {
		return subpartitionIndex;
	}

	@Override
	public void notifyDataAvailable() {
		// Nothing to do
	}

	@Override
	public void releaseAllResources() {
		releaseAllResources(null);
	}

	private void releaseAllResources(Throwable cause) {
		synchronized (lock) {
			if (isReleased) {
				return;
			}

			if (cause != null) {
				this.cause = cause;
			}

			// Release all buffers
			Buffer buffer;
			while ((buffer = buffers.poll()) != null) {
				buffer.recycleBuffer();
			}

			// If the IO thread is still running, the file handle will be closed after it exits and
			// we do not need to close it here.
			if (!isRunning) {
				closeCurrentFileReader();
			}

			externalResultPartitionMeta.notifySubpartitionConsumed(subpartitionIndex);

			isReleased = true;

			// If the view is waiting for more credits to arrive, it should stop waiting.
			lock.notifyAll();
		}
	}

	@Override
	public void notifySubpartitionConsumed() {
		releaseAllResources();
	}

	@Override
	public boolean isReleased() {
		return isReleased;
	}

	@Override
	public Throwable getFailureCause() {
		return cause;
	}

	@Override
	public String toString() {
		return String.format("ExternalSubpartitionView [current read file path : %s]",
			currFsIn == null ? null : currFsIn.getChannelID().getPath());
	}

	@VisibleForTesting
	long getTotalLength() {
		return totalLength;
	}

	@VisibleForTesting
	Iterator<ExternalBlockResultPartitionMeta.ExternalSubpartitionMeta> getMetaIterator() {
		return metaIterator;
	}

	@VisibleForTesting
	int getCurrentCredit() {
		synchronized (lock) {
			return currentCredit;
		}
	}

	@VisibleForTesting
	public boolean isRunning() {
		synchronized (lock) {
			return isRunning;
		}
	}
}
