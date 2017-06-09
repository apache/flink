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

package org.apache.flink.runtime.iterative.io;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelReader;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelWriter;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memory.AbstractPagedInputView;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * {@link AbstractPagedOutputView} used by the {@code BlockingBackChannel} for
 * transmitting superstep results.
 */
public class SerializedUpdateBuffer extends AbstractPagedOutputView {

	private static final int HEADER_LENGTH = 4;

	private static final float SPILL_THRESHOLD = 0.95f;

	private final LinkedBlockingQueue<MemorySegment> emptyBuffers;

	private ArrayDeque<MemorySegment> fullBuffers;

	private BlockChannelWriter<MemorySegment> currentWriter;

	private final IOManager ioManager;

	private final FileIOChannel.Enumerator channelEnumerator;

	private final int numSegmentsSpillingThreshold;

	private int numBuffersSpilled;

	private final int minBuffersForWriteEnd;

	private final int minBuffersForSpilledReadEnd;

	private final List<ReadEnd> readEnds;

	private final int totalNumBuffers;

	public SerializedUpdateBuffer() {
		super(-1, HEADER_LENGTH);

		emptyBuffers = null;
		fullBuffers = null;

		ioManager = null;
		channelEnumerator = null;

		numSegmentsSpillingThreshold = -1;
		minBuffersForWriteEnd = -1;
		minBuffersForSpilledReadEnd = -1;
		totalNumBuffers = -1;

		readEnds = Collections.emptyList();
	}

	public SerializedUpdateBuffer(List<MemorySegment> memSegments, int segmentSize, IOManager ioManager) {
		super(memSegments.remove(memSegments.size() - 1), segmentSize, HEADER_LENGTH);

		totalNumBuffers = memSegments.size() + 1;
		if (totalNumBuffers < 3) {
			throw new IllegalArgumentException("SerializedUpdateBuffer needs at least 3 memory segments.");
		}

		emptyBuffers = new LinkedBlockingQueue<MemorySegment>(totalNumBuffers);
		fullBuffers = new ArrayDeque<MemorySegment>(64);

		emptyBuffers.addAll(memSegments);

		int threshold = (int) ((1 - SPILL_THRESHOLD) * totalNumBuffers);
		numSegmentsSpillingThreshold = threshold > 0 ? threshold : 0;
		minBuffersForWriteEnd = Math.max(2, Math.min(16, totalNumBuffers / 2));
		minBuffersForSpilledReadEnd = Math.max(1, Math.min(16, totalNumBuffers / 4));

		if (minBuffersForSpilledReadEnd + minBuffersForWriteEnd > totalNumBuffers) {
			throw new IllegalArgumentException("BUG: Unfulfillable memory assignment.");
		}

		this.ioManager = ioManager;
		channelEnumerator = ioManager.createChannelEnumerator();
		readEnds = new ArrayList<ReadEnd>();
	}

	@Override
	protected MemorySegment nextSegment(MemorySegment current, int positionInCurrent) throws IOException {
		current.putInt(0, positionInCurrent);

		// check if we keep the segment in memory, or if we spill it
		if (emptyBuffers.size() > numSegmentsSpillingThreshold) {
			// keep buffer in memory
			fullBuffers.addLast(current);
		} else {
			// spill all buffers up to now
			// check, whether we have a channel already
			if (currentWriter == null) {
				currentWriter = ioManager.createBlockChannelWriter(channelEnumerator.next(), emptyBuffers);
			}

			// spill all elements gathered up to now
			numBuffersSpilled += fullBuffers.size();
			while (fullBuffers.size() > 0) {
				currentWriter.writeBlock(fullBuffers.removeFirst());
			}
			currentWriter.writeBlock(current);
			numBuffersSpilled++;
		}

		try {
			return emptyBuffers.take();
		} catch (InterruptedException iex) {
			throw new RuntimeException("Spilling Fifo Queue was interrupted while waiting for next buffer.");
		}
	}

	public void flush() throws IOException {
		advance();
	}

	public ReadEnd switchBuffers() throws IOException {
		// remove exhausted read ends
		for (int i = readEnds.size() - 1; i >= 0; --i) {
			final ReadEnd re = readEnds.get(i);
			if (re.disposeIfDone()) {
				readEnds.remove(i);
			}
		}

		// add the current memorySegment and reset this writer
		final MemorySegment current = getCurrentSegment();
		current.putInt(0, getCurrentPositionInSegment());
		fullBuffers.addLast(current);

		// create the reader
		final ReadEnd readEnd;
		if (numBuffersSpilled == 0 && emptyBuffers.size() >= minBuffersForWriteEnd) {
			// read completely from in-memory segments
			readEnd = new ReadEnd(fullBuffers.removeFirst(), emptyBuffers, fullBuffers, null, null, 0);
		} else {
			int toSpill = Math.min(minBuffersForSpilledReadEnd + minBuffersForWriteEnd - emptyBuffers.size(),
				fullBuffers.size());

			// reader reads also segments on disk
			// grab some empty buffers to re-read the first segment
			if (toSpill > 0) {
				// need to spill to make a buffers available
				if (currentWriter == null) {
					currentWriter = ioManager.createBlockChannelWriter(channelEnumerator.next(), emptyBuffers);
				}

				for (int i = 0; i < toSpill; i++) {
					currentWriter.writeBlock(fullBuffers.removeFirst());
				}
				numBuffersSpilled += toSpill;
			}

			// now close the writer and create the reader
			currentWriter.close();
			final BlockChannelReader<MemorySegment> reader = ioManager.createBlockChannelReader(currentWriter.getChannelID());

			// gather some memory segments to circulate while reading back the data
			final List<MemorySegment> readSegments = new ArrayList<MemorySegment>();
			try {
				while (readSegments.size() < minBuffersForSpilledReadEnd) {
					readSegments.add(emptyBuffers.take());
				}

				// read the first segment
				MemorySegment firstSeg = readSegments.remove(readSegments.size() - 1);
				reader.readBlock(firstSeg);
				firstSeg = reader.getReturnQueue().take();

				// create the read end reading one less buffer, because the first buffer is already read back
				readEnd = new ReadEnd(firstSeg, emptyBuffers, fullBuffers, reader, readSegments,
						numBuffersSpilled - 1);
			} catch (InterruptedException e) {
				throw new RuntimeException(
					"SerializedUpdateBuffer was interrupted while reclaiming memory by spilling.", e);
			}
		}

		// reset the writer
		fullBuffers = new ArrayDeque<MemorySegment>(64);
		currentWriter = null;
		numBuffersSpilled = 0;
		try {
			seekOutput(emptyBuffers.take(), HEADER_LENGTH);
		} catch (InterruptedException e) {
			throw new RuntimeException("SerializedUpdateBuffer was interrupted while reclaiming memory by spilling.", e);
		}

		// register this read end
		readEnds.add(readEnd);
		return readEnd;
	}

	public List<MemorySegment> close() {
		if (currentWriter != null) {
			try {
				currentWriter.closeAndDelete();
			} catch (Throwable t) {
				// do nothing
			}
		}

		List<MemorySegment> freeMem = new ArrayList<MemorySegment>(64);

		// add all memory allocated to the write end
		freeMem.add(getCurrentSegment());
		clear();
		freeMem.addAll(fullBuffers);
		fullBuffers = null;

		// add memory from non-exhausted read ends
		try {
			for (int i = readEnds.size() - 1; i >= 0; --i) {
				final ReadEnd re = readEnds.remove(i);
				re.forceDispose(freeMem);
			}

			// release all empty segments
			while (freeMem.size() < totalNumBuffers) {
				freeMem.add(emptyBuffers.take());
			}
		} catch (InterruptedException e) {
			throw new RuntimeException("Retrieving memory back from asynchronous I/O was interrupted.", e);
		}

		return freeMem;
	}

	// ============================================================================================

	private static final class ReadEnd extends AbstractPagedInputView {

		private final LinkedBlockingQueue<MemorySegment> emptyBufferTarget;

		private final Deque<MemorySegment> fullBufferSource;

		private final BlockChannelReader<MemorySegment> spilledBufferSource;

		private int spilledBuffersRemaining;

		private int requestsRemaining;

		private ReadEnd(MemorySegment firstMemSegment, LinkedBlockingQueue<MemorySegment> emptyBufferTarget,
										Deque<MemorySegment> fullBufferSource, BlockChannelReader<MemorySegment> spilledBufferSource,
										List<MemorySegment> emptyBuffers, int numBuffersSpilled)
			throws IOException {
			super(firstMemSegment, firstMemSegment.getInt(0), HEADER_LENGTH);

			this.emptyBufferTarget = emptyBufferTarget;
			this.fullBufferSource = fullBufferSource;

			this.spilledBufferSource = spilledBufferSource;

			requestsRemaining = numBuffersSpilled;
			this.spilledBuffersRemaining = numBuffersSpilled;

			// send the first requests
			while (requestsRemaining > 0 && emptyBuffers.size() > 0) {
				this.spilledBufferSource.readBlock(emptyBuffers.remove(emptyBuffers.size() - 1));
				requestsRemaining--;
			}
		}

		@Override
		protected MemorySegment nextSegment(MemorySegment current) throws IOException {
			// use the buffer to send the next request
			if (requestsRemaining > 0) {
				requestsRemaining--;
				spilledBufferSource.readBlock(current);
			} else {
				emptyBufferTarget.add(current);
			}

			// get the next buffer either from the return queue, or the full buffer source
			if (spilledBuffersRemaining > 0) {
				spilledBuffersRemaining--;
				try {
					return spilledBufferSource.getReturnQueue().take();
				} catch (InterruptedException e) {
					throw new RuntimeException("Read End was interrupted while waiting for spilled buffer.", e);
				}
			} else if (fullBufferSource.size() > 0) {
				return fullBufferSource.removeFirst();
			} else {
				clear();

				// delete the channel, if we had one
				if (spilledBufferSource != null) {
					spilledBufferSource.closeAndDelete();
				}

				throw new EOFException();
			}
		}

		@Override
		protected int getLimitForSegment(MemorySegment segment) {
			return segment.getInt(0);
		}

		private boolean disposeIfDone() {
			if (fullBufferSource.isEmpty() && spilledBuffersRemaining == 0) {
				if (getCurrentSegment() == null || getCurrentPositionInSegment() >= getCurrentSegmentLimit()) {
					if (getCurrentSegment() != null) {
						emptyBufferTarget.add(getCurrentSegment());
						clear();
					}

					if (spilledBufferSource != null) {
						try {
							spilledBufferSource.closeAndDelete();
						} catch (Throwable t) {
							// do nothing
						}
					}
					return true;
				}
			}
			return false;
		}

		private void forceDispose(List<MemorySegment> freeMemTarget) throws InterruptedException {
			// add the current segment
			final MemorySegment current = getCurrentSegment();
			clear();
			if (current != null) {
				freeMemTarget.add(current);
			}

			// add all remaining memory
			freeMemTarget.addAll(fullBufferSource);

			// add the segments with the requests issued but not returned
			for (int i = spilledBuffersRemaining - requestsRemaining; i > 0; --i) {
				freeMemTarget.add(emptyBufferTarget.take());
			}

			if (spilledBufferSource != null) {
				try {
					spilledBufferSource.closeAndDelete();
				} catch (Throwable t) {
					// do nothing
				}
			}
		}
	}
}
