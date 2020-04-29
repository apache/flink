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
import org.apache.flink.runtime.io.disk.RandomAccessInputView;
import org.apache.flink.runtime.io.disk.SimpleCollectingOutputView;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelReader;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelWriter;
import org.apache.flink.runtime.io.disk.iomanager.ChannelReaderInputView;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.HeaderlessChannelReaderInputView;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.io.BinaryRowChannelInputViewIterator;
import org.apache.flink.table.runtime.io.ChannelWithMeta;
import org.apache.flink.table.runtime.typeutils.AbstractRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A resettable external buffer for binary row. It stores records in memory and spill to disk
 * when memory is not enough. When the spill is completed, the records are written to memory again.
 * The returned iterator reads the data in write order (read spilled records first). It supports
 * infinite length. It can open multiple Iterators. It support new iterator with beginRow.
 *
 * <p>NOTE: Not supports reading while writing. In the face of concurrent modification, the
 * iterator fails quickly and cleanly, rather than risking arbitrary, non-deterministic behavior
 * at an undetermined time in the future.
 */
public class ResettableExternalBuffer implements ResettableRowBuffer {

	private static final Logger LOG = LoggerFactory.getLogger(ResettableExternalBuffer.class);

	/** The minimum number of segments that are required, 320 KibiBytes. */
	public static final int MIN_NUM_MEMORY = 10 * MemoryManager.DEFAULT_PAGE_SIZE;

	// We will only read one spilled file at the same time.
	private static final int READ_BUFFER = 2;

	private final IOManager ioManager;
	private final LazyMemorySegmentPool pool;
	private final BinaryRowDataSerializer binaryRowSerializer;
	private final InMemoryBuffer inMemoryBuffer;

	// The size of each segment
	private final int segmentSize;

	private long spillSize;

	// The length of each row, if each row is of fixed length
	private long rowLength;
	// If each row is of fixed length
	private boolean isRowAllInFixedPart;

	private final List<ChannelWithMeta> spilledChannelIDs;
	private final List<Integer> spilledChannelRowOffsets;
	private int numRows;

	// Number of iterators currently opened
	private int iterOpenedCount;

	// Times of reset() called. Used to check validity of iterators.
	private int externalBufferVersion;

	private boolean addCompleted;

	public ResettableExternalBuffer(
		IOManager ioManager,
		LazyMemorySegmentPool pool,
		AbstractRowDataSerializer serializer,
		boolean isRowAllInFixedPart) {
		this.ioManager = ioManager;
		this.pool = pool;

		this.binaryRowSerializer = serializer instanceof BinaryRowDataSerializer ?
				(BinaryRowDataSerializer) serializer.duplicate() :
				new BinaryRowDataSerializer(serializer.getArity());

		this.segmentSize = pool.pageSize();

		this.spilledChannelIDs = new ArrayList<>();
		this.spillSize = 0;

		this.spilledChannelRowOffsets = new ArrayList<>();
		this.numRows = 0;

		this.iterOpenedCount = 0;
		this.externalBufferVersion = 0;

		this.isRowAllInFixedPart = isRowAllInFixedPart;
		this.rowLength = isRowAllInFixedPart ? binaryRowSerializer.getSerializedRowFixedPartLength() : -1;
		this.addCompleted = false;

		this.inMemoryBuffer = new InMemoryBuffer(serializer);
	}

	@Override
	public void reset() {
		clearChannels();
		inMemoryBuffer.reset();
		numRows = 0;
		externalBufferVersion++;
		addCompleted = false;
	}

	@Override
	public void add(RowData row) throws IOException {
		checkState(!addCompleted, "This buffer has add completed.");
		if (!inMemoryBuffer.write(row)) {
			// Check if record is too big.
			if (inMemoryBuffer.getCurrentDataBufferOffset() == 0) {
				throwTooBigException(row);
			}
			spill();
			if (!inMemoryBuffer.write(row)) {
				throwTooBigException(row);
			}
		}

		numRows++;
	}

	@Override
	public void complete() {
		addCompleted = true;
	}

	@Override
	public BufferIterator newIterator() {
		return newIterator(0);
	}

	@Override
	public BufferIterator newIterator(int beginRow) {
		checkState(addCompleted, "This buffer has not add completed.");
		checkArgument(beginRow >= 0, "`beginRow` can't be negative!");
		iterOpenedCount++;
		return new BufferIterator(beginRow);
	}

	/**
	 * Delete all files and release the memory.
	 */
	@Override
	public void close() {
		clearChannels();
		inMemoryBuffer.close();
		pool.close();
	}

	private void throwTooBigException(RowData row) throws IOException {
		int rowSize = InstantiationUtil.serializeToByteArray(inMemoryBuffer.serializer, row).length;
		throw new IOException("Record is too big, it can't be added to a empty InMemoryBuffer! " +
				"Record size: " + rowSize + ", Buffer: " + memorySize());
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
		spilledChannelIDs.add(new ChannelWithMeta(
			channel,
			inMemoryBuffer.getNumRecordBuffers(),
			inMemoryBuffer.getNumBytesInLastBuffer()));
		this.spilledChannelRowOffsets.add(numRows);

		inMemoryBuffer.reset();
	}

	public int size() {
		return numRows;
	}

	private int memorySize() {
		return pool.freePages() * segmentSize;
	}

	public long getUsedMemoryInBytes() {
		return memorySize() + iterOpenedCount * READ_BUFFER * segmentSize;
	}

	public int getNumSpillFiles() {
		return spilledChannelIDs.size();
	}

	public long getSpillInBytes() {
		return spillSize;
	}

	private void clearChannels() {
		for (ChannelWithMeta meta : spilledChannelIDs) {
			final File f = new File(meta.getChannel().getPath());
			if (f.exists()) {
				f.delete();
			}
		}
		spilledChannelIDs.clear();
		spillSize = 0;

		spilledChannelRowOffsets.clear();
	}

	/**
	 * Iterator of external buffer.
	 */
	public class BufferIterator implements ResettableRowBuffer.ResettableIterator {

		MutableObjectIterator<BinaryRowData> currentIterator;

		// memory for file reader to store read result
		List<MemorySegment> freeMemory = null;
		BlockChannelReader<MemorySegment> fileReader;
		int currentChannelID = -1;

		BinaryRowData reuse = binaryRowSerializer.createInstance();
		BinaryRowData row;
		int beginRow;
		int nextRow;

		// reuse in memory buffer iterator to reduce initialization cost.
		InMemoryBuffer.InMemoryBufferIterator reusableMemoryIterator;

		// value of resetCount of buffer when this iterator is created.
		// used to check validity.
		int versionSnapshot;
		// if this iterator is closed
		boolean closed;

		private BufferIterator(int beginRow) {
			this.beginRow = Math.min(beginRow, numRows);
			this.nextRow = this.beginRow;

			this.versionSnapshot = externalBufferVersion;
			this.closed = false;
		}

		private void checkValidity() {
			if (closed) {
				throw new RuntimeException("This iterator is closed!");
			} else if (this.versionSnapshot != externalBufferVersion) {
				throw new RuntimeException("This iterator is no longer valid!");
			}
		}

		@Override
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
			reuse.clear();
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
			iterOpenedCount--;
		}

		@Override
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
				if (isRowAllInFixedPart) {
					gotoAllInFixedPartRow(beginRow);
				} else {
					gotoVariableLengthRow(beginRow);
				}
			} else if (currentChannelID == Integer.MAX_VALUE) {
				// The last one is in memory, so the end.
				return false;
			} else if (currentChannelID < spilledChannelIDs.size() - 1) {
				// Next spilled iterator.
				nextSpilledIterator();
			} else {
				// It is the last iterator.
				newMemoryIterator();
			}
			return true;
		}

		private boolean iteratorNeedsUpdate() {
			int size = spilledChannelRowOffsets.size();
			return size > 0
				&& currentChannelID == Integer.MAX_VALUE
				&& nextRow <= spilledChannelRowOffsets.get(size - 1);
		}

		private void updateIteratorIfNeeded() throws IOException {
			if (iteratorNeedsUpdate()) {
				reuse.clear();
				reusableMemoryIterator = null;

				if (isRowAllInFixedPart) {
					gotoAllInFixedPartRow(nextRow);
				} else {
					gotoVariableLengthRow(nextRow);
				}
			}
		}

		@Override
		public BinaryRowData getRow() {
			return row;
		}

		private void closeCurrentFileReader() throws IOException {
			if (fileReader != null) {
				fileReader.close();
				fileReader = null;
			}
		}

		private void gotoAllInFixedPartRow(int beginRow) throws IOException {
			// Find which channel contains the row.
			int beginChannel = upperBound(beginRow, spilledChannelRowOffsets);
			// Find the row number in its own channel (0-indexed).
			int beginRowInChannel = getBeginIndexInChannel(beginRow, beginChannel);
			if (beginRow == numRows) {
				// Row number out of range! Should return an "empty" iterator.
				newMemoryIterator(beginRowInChannel, inMemoryBuffer.getCurrentDataBufferOffset());
				return;
			}

			// Fixed length. Calculate offset directly.
			long numRecordsInSegment = segmentSize / rowLength;
			long offset =
				(beginRowInChannel / numRecordsInSegment) * segmentSize +
					(beginRowInChannel % numRecordsInSegment) * rowLength;

			if (beginChannel < spilledChannelRowOffsets.size()) {
				// Data on disk
				newSpilledIterator(beginChannel, offset);
			} else {
				// Data in memory
				newMemoryIterator(beginRowInChannel, offset);
			}
		}

		private void gotoVariableLengthRow(int beginRow) throws IOException {
			// Find which channel contains the row.
			int beginChannel = upperBound(beginRow, spilledChannelRowOffsets);
			// Find the row number in its own channel (0-indexed).
			int beginRowInChannel = getBeginIndexInChannel(beginRow, beginChannel);
			if (beginRow == numRows) {
				// Row number out of range! Should return an "empty" iterator.
				newMemoryIterator(beginRowInChannel, inMemoryBuffer.getCurrentDataBufferOffset());
				return;
			}

			if (beginChannel < spilledChannelRowOffsets.size()) {
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
			ChannelWithMeta channel = spilledChannelIDs.get(channelID);
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
				fileReader, getReadMemory(), channel.getBlockCount() - segmentNum,
				channel.getNumBytesInLastBlock(), false, offset - seekPosition
			);
			this.currentIterator = new BinaryRowChannelInputViewIterator(inView, binaryRowSerializer);
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
				reusableMemoryIterator.reset(inMemoryBuffer.recordCount, offset);
			}
			this.currentIterator = reusableMemoryIterator;
		}

		private int getBeginIndexInChannel(int beginRow, int beginChannel) {
			if (beginChannel > 0) {
				return beginRow - spilledChannelRowOffsets.get(beginChannel - 1);
			} else {
				return beginRow;
			}
		}

		private List<MemorySegment> getReadMemory() {
			if (freeMemory == null) {
				freeMemory = new ArrayList<>();
				for (int i = 0; i < READ_BUFFER; i++) {
					freeMemory.add(MemorySegmentFactory.allocateUnpooledSegment(segmentSize));
				}
			}
			return freeMemory;
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
		return spilledChannelIDs;
	}

	/**
	 * In memory buffer that stores records to memorySegments, returns a iterator that map from memory.
	 */
	private class InMemoryBuffer implements Closeable {

		private final AbstractRowDataSerializer serializer;
		private final ArrayList<MemorySegment> recordBufferSegments;
		private final SimpleCollectingOutputView recordCollector;

		// Can't use recordCollector.getCurrentOffset(), maybe the offset of recordCollector is
		// disrupted by the attempt of record writing.
		private long currentDataBufferOffset;
		private int numBytesInLastBuffer;

		private int recordCount;

		private InMemoryBuffer(AbstractRowDataSerializer serializer) {
			// serializer has states, so we must duplicate
			this.serializer = (AbstractRowDataSerializer) serializer.duplicate();
			this.recordBufferSegments = new ArrayList<>();
			this.recordCollector = new SimpleCollectingOutputView(
					this.recordBufferSegments, pool, segmentSize);
			this.recordCount = 0;
		}

		private void reset() {
			this.currentDataBufferOffset = 0;
			this.recordCount = 0;

			returnToSegmentPool();

			this.recordCollector.reset();
		}

		@Override
		public void close() {
			returnToSegmentPool();
		}

		private void returnToSegmentPool() {
			pool.returnAll(this.recordBufferSegments);
			this.recordBufferSegments.clear();
		}

		public boolean write(RowData row) throws IOException {
			try {
				this.serializer.serializeToPages(row, this.recordCollector);
				currentDataBufferOffset = this.recordCollector.getCurrentOffset();
				numBytesInLastBuffer = this.recordCollector.getCurrentPositionInSegment();
				recordCount++;
				return true;
			} catch (EOFException e) {
				return false;
			}
		}

		private ArrayList<MemorySegment> getRecordBufferSegments() {
			return recordBufferSegments;
		}

		private long getCurrentDataBufferOffset() {
			return currentDataBufferOffset;
		}

		private int getNumRecordBuffers() {
			int result = (int) (currentDataBufferOffset / segmentSize);
			long mod = currentDataBufferOffset % segmentSize;
			if (mod != 0) {
				result += 1;
			}
			return result;
		}

		private int getNumBytesInLastBuffer() {
			return numBytesInLastBuffer;
		}

		private InMemoryBufferIterator newIterator(int beginRow, long offset) {
			checkArgument(offset >= 0, "`offset` can't be negative!");

			RandomAccessInputView recordBuffer = new RandomAccessInputView(
					this.recordBufferSegments, segmentSize, numBytesInLastBuffer);
			return new InMemoryBufferIterator(recordCount, beginRow, offset, recordBuffer);
		}

		/**
		 * Iterator of in memory buffer.
		 */
		public class InMemoryBufferIterator implements MutableObjectIterator<BinaryRowData>, Closeable {
			private final int beginRow;
			private int nextRow;
			private RandomAccessInputView recordBuffer;
			private int expectedRecordCount;

			private InMemoryBufferIterator(int expectedRecordCount, int beginRow, long offset, RandomAccessInputView recordBuffer) {
				this.beginRow = beginRow;
				this.recordBuffer = recordBuffer;
				reset(expectedRecordCount, offset);
			}

			public void reset(int expectedRecordCount, long offset) {
				this.nextRow = beginRow;
				this.expectedRecordCount = expectedRecordCount;
				recordBuffer.setReadPosition(offset);
			}

			@Override
			public BinaryRowData next(BinaryRowData reuse) throws IOException {
				try {
					if (expectedRecordCount != recordCount) {
						throw new ConcurrentModificationException();
					}
					if (nextRow >= recordCount) {
						return null;
					}
					nextRow++;
					return (BinaryRowData) serializer.mapFromPages(reuse, recordBuffer);
				} catch (EOFException e) {
					return null;
				}
			}

			@Override
			public BinaryRowData next() throws IOException {
				throw new RuntimeException("Not support!");
			}

			@Override
			public void close() {
			}
		}
	}
}
