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

import org.apache.flink.runtime.io.disk.iomanager.BufferFileWriter;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManager.IOMode;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A blocking in-memory subpartition, which is able to spill to disk.
 *
 * <p> Buffers are kept in-memory as long as possible. If not possible anymore, all buffers are
 * spilled to disk.
 */
class SpillableSubpartition extends ResultSubpartition {

	private static final Logger LOG = LoggerFactory.getLogger(SpillableSubpartition.class);

	/** All buffers of this subpartition. */
	final ArrayList<Buffer> buffers = new ArrayList<Buffer>();

	/** The I/O manager to create the spill writer from. */
	final IOManager ioManager;

	/** The default I/O mode to use. */
	final IOMode ioMode;

	/** The writer used for spilling. As long as this is null, we are in-memory. */
	BufferFileWriter spillWriter;

	/** Flag indicating whether the subpartition has been finished. */
	private boolean isFinished;

	/** Flag indicating whether the subpartition has been released. */
	private boolean isReleased;

	/** The read view to consume this subpartition. */
	private ResultSubpartitionView readView;

	SpillableSubpartition(int index, ResultPartition parent, IOManager ioManager, IOMode ioMode) {
		super(index, parent);

		this.ioManager = checkNotNull(ioManager);
		this.ioMode = checkNotNull(ioMode);
	}

	@Override
	public boolean add(Buffer buffer) throws IOException {
		checkNotNull(buffer);

		synchronized (buffers) {
			if (isFinished || isReleased) {
				return false;
			}

			// In-memory
			if (spillWriter == null) {
				buffers.add(buffer);

				return true;
			}
		}

		// Else: Spilling
		spillWriter.writeBlock(buffer);

		return true;
	}

	@Override
	public void finish() throws IOException {
		synchronized (buffers) {
			if (add(EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE))) {
				isFinished = true;
			}
		}

		// If we are spilling/have spilled, wait for the writer to finish.
		if (spillWriter != null) {
			spillWriter.close();
		}
	}

	@Override
	public void release() throws IOException {
		final ResultSubpartitionView view;

		synchronized (buffers) {
			if (isReleased) {
				return;
			}

			// Recycle all in-memory buffers
			for (Buffer buffer : buffers) {
				buffer.recycle();
			}

			buffers.clear();
			buffers.trimToSize();

			// If we are spilling/have spilled, wait for the writer to finish and delete the file.
			if (spillWriter != null) {
				spillWriter.closeAndDelete();
			}

			// Get the view...
			view = readView;
			readView = null;

			isReleased = true;
		}

		// Release the view outside of the synchronized block
		if (view != null) {
			view.notifySubpartitionConsumed();
		}
	}

	@Override
	public int releaseMemory() throws IOException {
		synchronized (buffers) {
			if (spillWriter == null) {
				// Create the spill writer
				spillWriter = ioManager.createBufferFileWriter(ioManager.createChannel());

				final int numberOfBuffers = buffers.size();

				// Spill all buffers
				for (int i = 0; i < numberOfBuffers; i++) {
					spillWriter.writeBlock(buffers.remove(0));
				}

				LOG.debug("Spilling {} buffers of {}.", numberOfBuffers, this);

				return numberOfBuffers;
			}
		}

		// Else: We have already spilled and don't hold any buffers
		return 0;
	}

	@Override
	public ResultSubpartitionView createReadView(BufferProvider bufferProvider) throws IOException {
		synchronized (buffers) {
			if (!isFinished) {
				throw new IllegalStateException("Subpartition has not been finished yet, " +
						"but blocking subpartitions can only be consumed after they have " +
						"been finished.");
			}

			if (readView != null) {
				throw new IllegalStateException("Subpartition is being or already has been " +
						"consumed, but we currently allow subpartitions to only be consumed once.");
			}

			// Spilled if closed and no outstanding write requests
			boolean isSpilled = spillWriter != null && (spillWriter.isClosed()
					|| spillWriter.getNumberOfOutstandingRequests() == 0);

			if (isSpilled) {
				if (ioMode.isSynchronous()) {
					readView = new SpilledSubpartitionViewSyncIO(
							this,
							bufferProvider.getMemorySegmentSize(),
							spillWriter.getChannelID(),
							0);
				}
				else {
					readView = new SpilledSubpartitionViewAsyncIO(
							this,
							bufferProvider,
							ioManager,
							spillWriter.getChannelID(),
							0);
				}
			}
			else {
				readView = new SpillableSubpartitionView(
						this, bufferProvider, buffers.size(), ioMode);
			}

			return readView;
		}
	}

	@Override
	public String toString() {
		return String.format("SpillableSubpartition [%d number of buffers (%d bytes)," +
						"finished? %s, read view? %s, spilled? %s]",
				getTotalNumberOfBuffers(), getTotalNumberOfBytes(), isFinished, readView != null,
				spillWriter != null);
	}
}
