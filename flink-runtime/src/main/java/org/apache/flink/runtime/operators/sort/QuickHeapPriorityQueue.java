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
package org.apache.flink.runtime.operators.sort;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.ChannelReaderInputViewIterator;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelReader;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelWriter;
import org.apache.flink.runtime.io.disk.iomanager.ChannelReaderInputView;
import org.apache.flink.runtime.io.disk.iomanager.ChannelWriterOutputView;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.util.PriorityQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Deque;
import java.util.List;
import java.util.Random;

/**
 * This is an implementation of {@link PriorityQueue} based on QuickSelect algorithm. All elements
 * are serialized and stored on Flink memory pages, and it choose some partitions to spill to disk
 * while memory is full, and load back in future for further computing.
 * NOTE: After poll element out, this implementation does not use it's memory circularly due to the
 * complexity, so this class is more suitable for the scenario "insert all the elements at first, and
 * then poll out".
 *
 * This implementation refers to the algorithm described in <a href="www.dcc.uchile.cl/TR/2008/TR_DCC-2008-006.pdf">
 * "Quickheaps: Simple, Efficient, and Cache-Oblivious"</a>.
 *
 * @param <T>
 */
public class QuickHeapPriorityQueue<T> implements PriorityQueue<T>{

	private static final Logger LOG = LoggerFactory.getLogger(QuickHeapPriorityQueue.class);

	/**
	 * Fix length records with a length below this threshold will be in-place sorted, if possible.
	 */
	private static final int THRESHOLD_FOR_IN_PLACE_SORTING = 32;

	/**
	 * The threshold of pivot size, make sure it does not store to much objects on heap.
	 */
	private static final int PIVOTS_STACK_MAX_SIZE = 100;

	/**
	 * Fail after flushing 10 time, make sure it does not block Flink for very long time while
	 * data size is more more lager than assigned memory.
	 */
	private static final int MAX_FLUSH_TIME = 10;

	/**
	 * The minimal number of buffers to use by the writers.
	 */
	protected static final int IO_BUFFERS_SIZE = 6;

	/**
	 * This class require at least 9 memory segments, 6 for potential IO buffer, 3 for InMemorySorter.
	 */
	private static final int MIN_REQUIRED_BUFFERS = 9;

	private final InMemorySorter<T> inMemorySorter;
	private final TypeSerializer<T> typeSerializer;
	private final TypeComparator<T> typeComparator;
	private final MemoryManager memoryManager;
	private final IOManager ioManager;
	private final FileIOChannel.Enumerator currentEnumerator;
	private final Deque<Integer> pivots;
	private final List<MemorySegment> ioBuffers;
	private final List<MemorySegment> availableMemories;
	private final Random random;
	private ChannelWriterOutputView spilledOutputView;
	private BlockChannelWriter<MemorySegment> spillWriter;
	private int skipped;
	private T spillPivot;
	private int totalCount;
	private int flushCount;

	public QuickHeapPriorityQueue(
		List<MemorySegment> availableMemories, TypeSerializer<T> typeSerializer,
		TypeComparator<T> typeComparator, MemoryManager memoryManager, IOManager ioManager) {

		if (availableMemories == null || typeSerializer == null || typeComparator == null || ioManager == null) {
			throw new NullPointerException();
		}

		if (availableMemories.size() < MIN_REQUIRED_BUFFERS) {
			throw new IllegalArgumentException("PriorityQueue sorter requires at least " +
				MIN_REQUIRED_BUFFERS + " memory buffers.");
		}

		this.availableMemories = availableMemories;
		this.typeSerializer = typeSerializer;
		this.typeComparator = typeComparator;
		this.memoryManager = memoryManager;
		this.ioManager = ioManager;
		this.ioBuffers = Lists.newArrayList();
		this.currentEnumerator = this.ioManager.createChannelEnumerator();

		for (int i = 0; i < IO_BUFFERS_SIZE; i++) {
			this.ioBuffers.add(availableMemories.remove(availableMemories.size() - 1));
		}

		// If element size is fixed and less than 32 byte, instantiate a fix-length in-place sorter,
		// otherwise the out-of-place sorter
		if (this.typeComparator.supportsSerializationWithKeyNormalization() &&
			this.typeSerializer.getLength() > 0 && this.typeSerializer.getLength() <= THRESHOLD_FOR_IN_PLACE_SORTING) {
			this.inMemorySorter = new FixedLengthRecordSorter<>(typeSerializer, typeComparator, availableMemories);
		} else {
			this.inMemorySorter = new NormalizedKeySorter<>(typeSerializer, typeComparator, availableMemories);
		}

		this.pivots = Lists.newLinkedList();
		this.random = new Random();
		this.skipped = 0;
		this.totalCount = 0;
		this.flushCount = 0;
	}

	/**
	 * Insert a element into heap.
	 *
	 * @param element
	 * @throws IOException
	 */
	public void insert(T element) throws IOException {
		put(element);
		this.totalCount++;
	}

	/**
	 * Get the next element with the order defined by TypeComparator. Return null if no more element available.
	 * Create a new instance for next element.
	 *
	 * @return
	 * @throws IOException
	 */
	public T next() throws IOException {
		T instance = this.typeSerializer.createInstance();
		return next(instance);
	}

	/**
	 * Get the next element with the order defined by TypeComparator. Return null if no more element available.
	 * Reuse the input instance for next element.
	 *
	 * @return
	 * @throws IOException
	 */
	public T next(T reuse) throws IOException {
		if (this.totalCount == 0) {
			return null;
		}

		if (pivots.isEmpty()) {
			// While there are remain records in memory, use quick QuickSelect to find pivots on all records in memory.
			quickSelect();
		} else {
			Integer lastPivot = pivots.peekLast();
			if (lastPivot != skipped) {
				// While the last pivot is not at "skipped" index, use QuickSelect algorithm to find pivots on records before last index.
				quickSelect(skipped, lastPivot - 1);
			}
		}

		// While pivots is empty, and some partitions has been spilled before, reload the elements, and do QuickSelect.
		while (pivots.isEmpty() && !onMemoryOnly()) {
			reload();
			quickSelect();
		}

		if (pivots.isEmpty()) {
			throw new IOException("Can not find any more pivots while there should still have " + this.totalCount + " records.");
		}

		if (pivots.pollLast() != skipped) {
			throw new IOException("The last pivot index is not 0 after quick select.");
		}

		reuse = this.inMemorySorter.getRecord(reuse, skipped);

		this.skipped++;
		this.totalCount--;
		return reuse;
	}

	/**
	 * Return the element number in heap.
	 *
	 * @return
	 */
	public int size() {
		return this.totalCount;
	}

	public void close() throws IOException {
		this.totalCount = 0;
		this.skipped = 0;
		this.pivots.clear();
		this.inMemorySorter.dispose();
		this.availableMemories.addAll(this.ioBuffers);
		if (spilledOutputView != null) {
			this.availableMemories.addAll(spilledOutputView.close());
		}

		this.memoryManager.release(this.availableMemories);
	}

	private void put(T element) throws IOException {
		boolean success = true;
		if (onMemoryOnly()) {
			if (!putInMemory(element)) {
				flush();
				if (this.typeComparator.compare(element, spillPivot) < 0) {
					success = putInMemory(element);
				} else {
					putInDisk(element);
				}
			}
		} else {
			if (this.typeComparator.compare(element, spillPivot) < 0) {
				if (!putInMemory(element)) {
					flush();
					if (this.typeComparator.compare(element, spillPivot) < 0) {
						success = putInMemory(element);
					} else {
						putInDisk(element);
					}
				}
			} else {
				putInDisk(element);
			}
		}

		if (!success) {
			throw new IOException("Failed to insert element after flush.");
		}
	}

	private boolean putInMemory(T element) throws IOException {
		boolean success = this.inMemorySorter.write(element);
		if (success) {
			// After writing new element to inMemorySorter, the pivots may be invalid.
			this.pivots.clear();
		}
		return success;
	}

	private void putInDisk(T element) throws IOException {
		this.typeSerializer.serialize(element, this.spilledOutputView);
	}


	private void flush() throws IOException {
		if (this.flushCount > MAX_FLUSH_TIME) {
			throw new RuntimeException("Flush to disk more than " + MAX_FLUSH_TIME + " times. "
				+ "Probably cause: Too many duplicate keys.");
		}

		if (this.pivots.isEmpty()) {
			quickSelect();
		}

		spillPivot = this.typeSerializer.createInstance();
		// Take the first pivot as spill pivot, spill all elements after this pivot.
		Integer spillIndex = this.pivots.getFirst();
		this.spillPivot = this.inMemorySorter.getRecord(spillPivot, spillIndex);

		// Flush elements before and after spill pivot into different files, then reload elements before spillPivot back to memory.
		// Further optimization: For fixed length element, may not need flush elements before spill pivot to disk.

		// Flush all elements before spill pivot to disk.
		FileIOChannel.ID flushedId = this.currentEnumerator.next();
		BlockChannelWriter<MemorySegment> blockChannelWriter = this.ioManager.createBlockChannelWriter(flushedId);
		List<MemorySegment> writeBuffers = getIOBuffers(2);
		final ChannelWriterOutputView output = new ChannelWriterOutputView(blockChannelWriter, writeBuffers,
			writeBuffers.get(0).size());
		this.inMemorySorter.writeToOutput(output, skipped, spillIndex - skipped);
		this.ioBuffers.addAll(output.close());

		// Flush all elements after spill pivot to disk.
		if (this.spilledOutputView == null && this.spillWriter == null) {
			FileIOChannel.ID spillId = this.currentEnumerator.next();
			this.spillWriter = this.ioManager.createBlockChannelWriter(spillId);
			List<MemorySegment> spillWriteBuffers = getIOBuffers(2);
			this.spilledOutputView = new ChannelWriterOutputView(this.spillWriter, spillWriteBuffers,
				spillWriteBuffers.get(0).size());
		}
		this.inMemorySorter.writeToOutput(spilledOutputView, spillIndex, this.inMemorySorter.size() - spillIndex);
		// /---clean memory
		this.inMemorySorter.reset();
		this.pivots.clear();
		this.skipped = 0;

		// Reload elements before spill pivot back to memory.
		List<MemorySegment> readBuffers = getIOBuffers(2);
		final BlockChannelReader<MemorySegment> blockChannelReader = this.ioManager.createBlockChannelReader(flushedId);
		final ChannelReaderInputView inputView = new ChannelReaderInputView(blockChannelReader, readBuffers, false);
		ChannelReaderInputViewIterator<T> readerInputViewIterator = new ChannelReaderInputViewIterator<T>(inputView, this.ioBuffers, this.typeSerializer);
		T next = readerInputViewIterator.next();
		while (next != null) {
			putInMemory(next);
			next = readerInputViewIterator.next();
		}

		// Delete temporary file after reload.
		File pathFile = flushedId.getPathFile();
		if (pathFile.exists()) {
			pathFile.delete();
		}

		this.flushCount++;
	}

	private List<MemorySegment> getIOBuffers(int num) throws IOException {
		if (this.ioBuffers.size() < num) {
			throw new IOException("Not enough memory for write buffer.");
		}
		List<MemorySegment> buffers = Lists.newArrayList();
		for (int i=0; i<num; i++) {
			buffers.add(this.ioBuffers.remove(this.ioBuffers.size() - 1));
		}
		return buffers;
	}

	private void reload() throws IOException {
		FileIOChannel.ID spilledID = this.spillWriter.getChannelID();
		// Clear all state information.
		this.inMemorySorter.reset();
		this.pivots.clear();
		this.skipped = 0;
		this.spillPivot = null;
		this.ioBuffers.addAll(this.spilledOutputView.close());
		this.spilledOutputView = null;
		this.spillWriter = null;

		// Read elements from disk, and insert back to memory.
		List<MemorySegment> readBuffers = getIOBuffers(2);
		final BlockChannelReader<MemorySegment> blockChannelReader = this.ioManager.createBlockChannelReader(spilledID);
		final ChannelReaderInputView inputView = new ChannelReaderInputView(blockChannelReader, readBuffers, false);
		ChannelReaderInputViewIterator<T> readerInputViewIterator =
			new ChannelReaderInputViewIterator<>(inputView, this.ioBuffers, this.typeSerializer);
		try {
			T next = readerInputViewIterator.next();
			while (next != null) {
				put(next);
				next = readerInputViewIterator.next();
			}
		} catch(Throwable e) {
			this.ioBuffers.addAll(inputView.close());
			throw e;
		}

		// Delete spilled file after reload.
		File pathFile = spilledID.getPathFile();
		if (pathFile.exists()) {
			pathFile.delete();
		}
	}

	private boolean onMemoryOnly() {
		return this.spillPivot == null;
	}

	/**
	 * Use QuickSelect algorithm to find the pivot at index 0, this pivot would be the next poll element.
	 * @throws IOException
	 */
	private void quickSelect() throws IOException {
		if (skipped < this.inMemorySorter.size()) {
			quickSelect(skipped, this.inMemorySorter.size() - 1);
		}
	}

	/**
	 * Use QuickSelect algorithm to find the pivot at start index between start index and end index.
	 * @throws IOException
	 */
	private void quickSelect(int start, int end) throws IOException {
		int pivotIndex = swapAtPivot(start, end);
		pivots.addLast(pivotIndex);
		while (pivotIndex > start) {
			pivotIndex = swapAtPivot(start, pivotIndex - 1);
			pivots.addLast(pivotIndex);
			if (pivots.size() > PIVOTS_STACK_MAX_SIZE) {
				pivots.removeFirst();
			}
		}
	}

	// Find a random pivot between start and end, and swap elements according to the pivot.
	private int swapAtPivot(int start, int end) throws IOException {
		Preconditions.checkArgument(start >= 0, "Element index should be positive.");
		Preconditions.checkArgument(end >= 0, "Element index should be positive.");
		Preconditions.checkArgument(end >= start, "The second index should not less than the first index.");
		if (start == end) {
			return start;
		}
		int pivotIndex = start + random.nextInt(end - start);
		if (pivotIndex != end) {
			// Swap the pivot to the last index.
			inMemorySorter.swap(pivotIndex, end);
		}
		int i = start, j = end - 1;
		while (true) {
			while (this.inMemorySorter.compare(i, end) < 0 && i < end) {
				i++;
			}

			while (this.inMemorySorter.compare(end, j) < 0 && j > i) {
				j--;
			}

			if (i < j) {
				inMemorySorter.swap(i, j);
			} else {
				break;
			}
			i++;
			j--;
		}
		if (i < end) {
			// Swap the pivot with the first element which is larger than pivot.
			inMemorySorter.swap(i, end);
		}
		return i;
	}
}

