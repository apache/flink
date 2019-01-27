/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.util;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelReader;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelWriter;
import org.apache.flink.runtime.io.disk.iomanager.ChannelReaderInputView;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.HeaderlessChannelReaderInputView;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.typeutils.AbstractRowSerializer;
import org.apache.flink.table.typeutils.BinaryRowSerializer;
import org.apache.flink.util.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A resettable external buffer for binary row. It stores records in memory and spill to disk
 * when memory is not enough. When the spill is completed, the records are written to memory.
 * The returned iterator reads the data in write order (read spilled records first).
 */
public class ResettableExternalBuffer implements Closeable {

	private static final Logger LOG = LoggerFactory.getLogger(ResettableExternalBuffer.class);

	// We will only read one spilled file at the same time.
	static final int READ_BUFFER = 2;

	private final MemoryManager memoryManager;
	private final IOManager ioManager;
	private final List<MemorySegment> memory;

	private final BinaryRowSerializer binaryRowSerializer;

	private final InMemoryBuffer inMemoryBuffer;

	private final List<ChannelWithMeta> channelIDs;
	private long spillSize;

	// The size of each segment
	private int segmentSize;

	// The length of each row, if each row is of fixed length
	private long fixedLength;
	// If each row is of fixed length
	private boolean isFixedLength;

	private final List<Integer> numRowsUntilThisChannel;
	private int numRows;

	// Number of iterators currently opened
	private int iteratorCount;
	// Times of reset() called. Used to check validity of iterators.
	private int resetCount;

	public ResettableExternalBuffer(
		MemoryManager memoryManager,
		IOManager ioManager,
		List<MemorySegment> memory,
		AbstractRowSerializer serializer) {
		this.memoryManager = memoryManager;
		this.ioManager = ioManager;
		this.memory = memory;

		if (serializer instanceof BinaryRowSerializer) {
			// serializer has states, so we must duplicate
			this.binaryRowSerializer = (BinaryRowSerializer) serializer.duplicate();
		} else {
			this.binaryRowSerializer = new BinaryRowSerializer(serializer.getTypes());
		}

		this.inMemoryBuffer = new InMemoryBuffer(memory, serializer);

		this.channelIDs = new ArrayList<>();
		this.spillSize = 0;

		this.segmentSize = memory.get(0).size();

		this.numRowsUntilThisChannel = new ArrayList<>();
		this.numRows = 0;

		this.iteratorCount = 0;
		this.resetCount = 0;

		this.isFixedLength = binaryRowSerializer.isRowFixedLength();
		if (this.isFixedLength) {
			this.fixedLength = binaryRowSerializer.getSerializedRowFixedPartLength();
		}
	}

	public int size() {
		return numRows;
	}

	private int memorySize() {
		return memory.size() * segmentSize;
	}

	private String getRowSize(BaseRow row) {
		if (row instanceof BinaryRow) {
			return String.valueOf(((BinaryRow) row).getSizeInBytes());
		} else {
			return "?";
		}
	}

	public void add(BaseRow row) throws IOException {
		if (!inMemoryBuffer.write(row)) {
			// Check if record is too big.
			if (inMemoryBuffer.getCurrentDataBufferOffset() == 0) {
				throw new IOException("Record can't be added to a empty InMemoryBuffer! " +
					"Record size: " + getRowSize(row) + ", Buffer: " + memorySize());
			}
			spill();
			if (!inMemoryBuffer.write(row)) {
				throw new IOException("Record can't be added to a empty InMemoryBuffer! " +
					"Record size: " + getRowSize(row) + ", Buffer: " + memorySize());
			}
		}

		numRows++;
	}

	private void spill() throws IOException {
		FileIOChannel.ID channel = ioManager.createChannel();

		final BlockChannelWriter<MemorySegment> writer = this.ioManager.createBlockChannelWriter(channel);
		int numRecordBuffers = inMemoryBuffer.getNumRecordBuffers();
		ArrayList<MemorySegment> segments = inMemoryBuffer.getRecordBufferSegments();
		try {
			// spill in memory buffer in zero-copy.
			for (int i = 0; i < numRecordBuffers; i++) {
				writer.writeBlock(segments.get(i));
			}
			LOG.info("here spill the reset buffer data with {} bytes", writer.getSize());
			writer.close();
		} catch (IOException e) {
			writer.closeAndDelete();
			throw e;
		}

		spillSize += numRecordBuffers * segmentSize;
		channelIDs.add(new ChannelWithMeta(
			channel,
			inMemoryBuffer.getNumRecordBuffers(),
			inMemoryBuffer.getNumBytesInLastBuffer()));
		this.numRowsUntilThisChannel.add(numRows);

		inMemoryBuffer.reset();
	}

	public long getUsedMemoryInBytes() {
		return memorySize() + iteratorCount * READ_BUFFER * segmentSize;
	}

	public int getNumSpillFiles() {
		return channelIDs.size();
	}

	public long getSpillInBytes() {
		return spillSize;
	}

	public void reset() {
		clearChannels();
		inMemoryBuffer.reset();
		numRows = 0;
		resetCount++;
	}

	@Override
	public void close() {
		clearChannels();
		memoryManager.release(memory);
		inMemoryBuffer.close();
	}

	private void clearChannels() {
		for (ChannelWithMeta meta : channelIDs) {
			final File f = new File(meta.getChannel().getPath());
			if (f.exists()) {
				f.delete();
			}
		}
		channelIDs.clear();
		spillSize = 0;

		numRowsUntilThisChannel.clear();
	}

	public BufferIterator newIterator() {
		return newIterator(0);
	}

	/**
	 * Get a new iterator starting from the `beginRow`-th row. `beginRow` is 0-indexed.
	 */
	public BufferIterator newIterator(int beginRow) {
		checkArgument(beginRow >= 0, "`beginRow` can't be negative!");
		iteratorCount++;
		return new BufferIterator(beginRow);
	}

	/**
	 * Iterator of external buffer.
	 */
	public class BufferIterator implements Closeable {

		MutableObjectIterator<BinaryRow> currentIterator;

		// memory for file reader to store read result
		List<MemorySegment> freeMemory = null;
		BlockChannelReader<MemorySegment> fileReader;
		int currentChannelID = -1;

		BinaryRow reuse = binaryRowSerializer.createInstance();
		BinaryRow row;
		int beginRow;
		int nextRow;

		// reuse in memory buffer iterator to reduce initialization cost.
		InMemoryBuffer.BufferIterator reusableMemoryIterator;

		// value of resetCount of buffer when this iterator is created.
		// used to check validity.
		int bufferVersion;
		// if this iterator is closed
		boolean closed;

		private BufferIterator(int beginRow) {
			this.beginRow = Math.min(beginRow, numRows);
			this.nextRow = this.beginRow;

			this.bufferVersion = resetCount;
			this.closed = false;

			createFreeMemoryIfNeeded();
		}

		private void checkValidity() {
			if (closed) {
				throw new RuntimeException("This iterator is closed!");
			} else if (bufferVersion != resetCount) {
				throw new RuntimeException("This iterator is no longer valid!");
			}
		}

		public void reset() throws IOException {
			checkValidity();
			resetImpl();
		}

		private void resetImpl() throws IOException {
			closeCurrentFileReader();

			nextRow = beginRow;
			currentChannelID = -1;
			currentIterator = null;

			row = null;
			reuse.unbindMemorySegment();
		}

		@Override
		public void close() {
			if (closed) {
				return;
			}

			try {
				resetImpl();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}

			if (freeMemory != null) {
				freeMemory.clear();
			}
			if (reusableMemoryIterator != null) {
				reusableMemoryIterator.close();
			}

			closed = true;
			iteratorCount--;
		}

		public boolean hasNext() {
			return nextRow < numRows;
		}

		public int getBeginRow() {
			return beginRow;
		}

		public boolean advanceNext() {
			checkValidity();

			try {
				updateIteratorIfNeeded();

				// get from curr iterator or new iterator.
				while (true) {
					if (currentIterator != null &&
						(row = currentIterator.next(reuse)) != null) {
						this.nextRow++;
						return true;
					} else {
						if (!nextIterator()) {
							return false;
						}
					}
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		private boolean nextIterator() throws IOException {
			if (currentChannelID == -1) {
				// First call to next iterator. Fetch iterator according to beginRow.
				if (isFixedLength) {
					gotoFixedLengthRow(beginRow);
				} else {
					gotoVariableLengthRow(beginRow);
				}
			} else if (currentChannelID == Integer.MAX_VALUE) {
				// The last one is in memory, so the end.
				return false;
			} else if (currentChannelID < channelIDs.size() - 1) {
				// Next spilled iterator.
				nextSpilledIterator();
			} else {
				// It is the last iterator.
				newMemoryIterator();
			}
			return true;
		}

		private boolean iteratorNeedsUpdate() {
			int size = numRowsUntilThisChannel.size();
			return size > 0
				&& currentChannelID == Integer.MAX_VALUE
				&& nextRow <= numRowsUntilThisChannel.get(size - 1);
		}

		private void updateIteratorIfNeeded() throws IOException {
			createFreeMemoryIfNeeded();

			if (iteratorNeedsUpdate()) {
				reuse.unbindMemorySegment();
				reusableMemoryIterator = null;

				if (isFixedLength) {
					gotoFixedLengthRow(nextRow);
				} else {
					gotoVariableLengthRow(nextRow);
				}
			}
		}

		public BinaryRow getRow() {
			return row;
		}

		private void closeCurrentFileReader() throws IOException {
			if (fileReader != null) {
				fileReader.close();
				fileReader = null;
			}
		}

		private void gotoFixedLengthRow(int beginRow) throws IOException {
			// Find which channel contains the row.
			int beginChannel = upperBound(beginRow, numRowsUntilThisChannel);
			// Find the row number in its own channel (0-indexed).
			int beginRowInChannel = getBeginIndexInChannel(beginRow, beginChannel);
			if (beginRow == numRows) {
				// Row number out of range! Should return an "empty" iterator.
				newMemoryIterator(beginRowInChannel, inMemoryBuffer.getCurrentDataBufferOffset());
				return;
			}

			// Fixed length. Calculate offset directly.
			long numRecordsInSegment = segmentSize / fixedLength;
			long offset =
				(beginRowInChannel / numRecordsInSegment) * segmentSize +
					(beginRowInChannel % numRecordsInSegment) * fixedLength;

			if (beginChannel < numRowsUntilThisChannel.size()) {
				// Data on disk
				newSpilledIterator(beginChannel, offset);
			} else {
				// Data in memory
				newMemoryIterator(beginRowInChannel, offset);
			}
		}

		private void gotoVariableLengthRow(int beginRow) throws IOException {
			// Find which channel contains the row.
			int beginChannel = upperBound(beginRow, numRowsUntilThisChannel);
			// Find the row number in its own channel (0-indexed).
			int beginRowInChannel = getBeginIndexInChannel(beginRow, beginChannel);
			if (beginRow == numRows) {
				// Row number out of range! Should return an "empty" iterator.
				newMemoryIterator(beginRowInChannel, inMemoryBuffer.getCurrentDataBufferOffset());
				return;
			}

			if (beginChannel < numRowsUntilThisChannel.size()) {
				// Data on disk
				newSpilledIterator(beginChannel);
			} else {
				// Data in memory
				newMemoryIterator();
			}

			nextRow -= beginRowInChannel;
			for (int i = 0; i < beginRowInChannel; i++) {
				advanceNext();
			}
		}

		private void nextSpilledIterator() throws IOException {
			newSpilledIterator(currentChannelID + 1);
		}

		private void newSpilledIterator(int channelID) throws IOException {
			newSpilledIterator(channelID, 0);
		}

		private void newSpilledIterator(int channelID, long offset) throws IOException {
			ChannelWithMeta channel = channelIDs.get(channelID);
			currentChannelID = channelID;

			// close current reader first.
			closeCurrentFileReader();

			// calculate segment number
			int segmentNum = (int) (offset / segmentSize);
			long seekPosition = segmentNum * segmentSize;

			// new reader.
			this.fileReader = ioManager.createBlockChannelReader(channel.getChannel());
			if (offset > 0) {
				// seek to the beginning of that segment
				fileReader.seekToPosition(seekPosition);
			}
			ChannelReaderInputView inView = new HeaderlessChannelReaderInputView(
				fileReader, freeMemory, channel.getBlockCount() - segmentNum,
				channel.getNumBytesInLastBlock(), false, offset - seekPosition
			);
			this.currentIterator = new PagedChannelReaderInputViewIterator<>(
				inView, null, binaryRowSerializer
			);
		}

		private void newMemoryIterator() throws IOException {
			newMemoryIterator(0, 0);
		}

		private void newMemoryIterator(int beginRow, long offset) throws IOException {
			currentChannelID = Integer.MAX_VALUE;
			// close curr reader first.
			closeCurrentFileReader();

			if (reusableMemoryIterator == null) {
				reusableMemoryIterator = inMemoryBuffer.newIterator(beginRow, offset);
			} else {
				reusableMemoryIterator.reset(offset);
			}
			this.currentIterator = reusableMemoryIterator;
		}

		private int getBeginIndexInChannel(int beginRow, int beginChannel) {
			if (beginChannel > 0) {
				return beginRow - numRowsUntilThisChannel.get(beginChannel - 1);
			} else {
				return beginRow;
			}
		}

		public boolean rowInSpill(int rowNum) {
			int size = numRowsUntilThisChannel.size();
			return size > 0 && rowNum < numRowsUntilThisChannel.get(size - 1);
		}

		private void createFreeMemoryIfNeeded() {
			if (freeMemory == null && rowInSpill(beginRow)) {
				// Only initialize freeMemory when we need to read spilled records.
				freeMemory = new ArrayList<>();
				// Iterator will use memory segments from heap
				for (int i = 0; i < READ_BUFFER; i++) {
					freeMemory.add(MemorySegmentFactory.allocateUnpooledSegment(segmentSize));
				}
			}
		}

		// Find the index of the first element which is strictly greater than `goal` in `list`.
		// `list` must be sorted.
		// If every element in `list` is not larger than `goal`, return `list.size()`.
		private int upperBound(int goal, List<Integer> list) {
			if (list.size() == 0) {
				return 0;
			}
			if (list.get(list.size() - 1) <= goal) {
				return list.size();
			}

			// Binary search
			int head = 0;
			int tail = list.size() - 1;
			int mid;
			while (head < tail) {
				mid = (head + tail) / 2;
				if (list.get(mid) <= goal) {
					head = mid + 1;
				} else {
					tail = mid;
				}
			}
			return head;
		}
	}

	@VisibleForTesting
	List<ChannelWithMeta> getSpillChannels() {
		return channelIDs;
	}
}
