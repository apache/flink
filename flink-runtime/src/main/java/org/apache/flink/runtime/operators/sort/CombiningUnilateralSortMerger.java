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

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.util.EmptyMutableObjectIterator;
import org.apache.flink.util.MutableObjectIterator;
import org.apache.flink.util.TraversableOnceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;

/**
 * The CombiningUnilateralSortMerger is part of a merge-sort implementation.
 * Conceptually, a merge sort with combining works as follows:
 * (1) Divide the unsorted list into n sublists of about 1/n the size. (2) Sort each sublist recursively by re-applying
 * merge sort. (3) Combine all tuples with the same key within a sublist (4) Merge the two sublists back into one sorted
 * list.
 * Internally, the {@link CombiningUnilateralSortMerger} logic is factored into four threads (read, sort, combine,
 * spill) which communicate through a set of blocking queues (forming a closed loop).
 * Memory is allocated using the {@link MemoryManager} interface. Thus the component will most likely not exceed the
 * user-provided memory limits.
 */
public class CombiningUnilateralSortMerger<E> extends UnilateralSortMerger<E> {

	// ------------------------------------------------------------------------
	// Constants & Fields
	// ------------------------------------------------------------------------

	/**
	 * Logging.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(CombiningUnilateralSortMerger.class);

	private final GroupCombineFunction<E, E> combineStub;	// the user code stub that does the combining

	private Configuration udfConfig;


	// ------------------------------------------------------------------------
	// Constructor
	// ------------------------------------------------------------------------

	/**
	 *Creates a new sorter that reads the data from a given reader and provides an iterator returning that
	 * data in a sorted manner. The memory is divided among sort buffers, write buffers and read buffers
	 * automatically.
	 *
	 * @param combineStub The stub used to combine values with the same key.
	 * @param memoryManager The memory manager from which to allocate the memory.
	 * @param ioManager The I/O manager, which is used to write temporary files to disk.
	 * @param input The input that is sorted by this sorter.
	 * @param parentTask The parent task, which owns all resources used by this sorter.
	 * @param serializerFactory The type serializer.
	 * @param comparator The type comparator establishing the order relation.
	 * @param memoryFraction The fraction of memory dedicated to sorting, merging and I/O.
	 * @param maxNumFileHandles The maximum number of files to be merged at once.
	 * @param startSpillingFraction The faction of the buffers that have to be filled before the spilling thread
	 *                              actually begins spilling data to disk.
	 *
	 * @throws IOException Thrown, if an error occurs initializing the resources for external sorting.
	 * @throws MemoryAllocationException Thrown, if not enough memory can be obtained from the memory manager to
	 *                                   perform the sort.
	 */
	public CombiningUnilateralSortMerger(SortedDataFileFactory<E> sortedDataFileFactory, GroupCombineFunction<E, E> combineStub,
			 MemoryManager memoryManager, IOManager ioManager,
			 MutableObjectIterator<E> input, AbstractInvokable parentTask,
			 TypeSerializerFactory<E> serializerFactory, TypeComparator<E> comparator,
			 double memoryFraction, int maxNumFileHandles, boolean inMemoryResultEnabled, float startSpillingFraction,
			 boolean handleLargeRecords, boolean objectReuseEnabled)
		throws IOException, MemoryAllocationException
	{
		this(sortedDataFileFactory, combineStub, memoryManager, ioManager, input, parentTask, serializerFactory, comparator,
			memoryFraction, -1, maxNumFileHandles, inMemoryResultEnabled, startSpillingFraction, handleLargeRecords, objectReuseEnabled);
	}


	/**
	 * Creates a new sorter that reads the data from a given reader and provides an iterator returning that
	 * data in a sorted manner. The memory is divided among sort buffers, write buffers and read buffers
	 * automatically.
	 *
	 * @param combineStub The stub used to combine values with the same key.
	 * @param memoryManager The memory manager from which to allocate the memory.
	 * @param ioManager The I/O manager, which is used to write temporary files to disk.
	 * @param input The input that is sorted by this sorter.
	 * @param parentTask The parent task, which owns all resources used by this sorter.
	 * @param serializerFactory The type serializer.
	 * @param comparator The type comparator establishing the order relation.
	 * @param memoryFraction The fraction of memory dedicated to sorting, merging and I/O.
	 * @param numSortBuffers The number of distinct buffers to use creation of the initial runs.
	 * @param maxNumFileHandles The maximum number of files to be merged at once.
	 * @param startSpillingFraction The faction of the buffers that have to be filled before the spilling thread
	 *                              actually begins spilling data to disk.
	 *
	 * @throws IOException Thrown, if an error occurs initializing the resources for external sorting.
	 * @throws MemoryAllocationException Thrown, if not enough memory can be obtained from the memory manager to
	 *                                   perform the sort.
	 */
	public CombiningUnilateralSortMerger(SortedDataFileFactory<E> sortedDataFileFactory, GroupCombineFunction<E, E> combineStub, MemoryManager memoryManager, IOManager ioManager,
										 MutableObjectIterator<E> input, AbstractInvokable parentTask,
										 TypeSerializerFactory<E> serializerFactory, TypeComparator<E> comparator,
										 double memoryFraction, int numSortBuffers, int maxNumFileHandles, boolean inMemoryResultEnabled,
										 float startSpillingFraction, boolean handleLargeRecords, boolean objectReuseEnabled)
		throws IOException, MemoryAllocationException
	{
		super(sortedDataFileFactory,
			new CombiningRecordComparisonMerger<>(combineStub, sortedDataFileFactory, ioManager,
				serializerFactory.getSerializer(), comparator, maxNumFileHandles, objectReuseEnabled),
			memoryManager, ioManager, input, parentTask, serializerFactory, comparator,
			memoryFraction, numSortBuffers, maxNumFileHandles, inMemoryResultEnabled, startSpillingFraction, false,
			handleLargeRecords, objectReuseEnabled);

		this.combineStub = combineStub;
	}

	public void setUdfConfiguration(Configuration config) {
		this.udfConfig = config;
	}

	// ------------------------------------------------------------------------
	// Factory Methods
	// ------------------------------------------------------------------------

	@Override
	protected ThreadBase<E> getSpillingThread(SortedDataFileFactory<E> sortedDataFileFactory,
											  BlockingQueue<SortedDataFileElement<E>> spilledFiles, SortedDataFileMerger<E> merger,
											  ExceptionHandler<IOException> exceptionHandler, CircularQueues<E> queues,
											  AbstractInvokable parentTask, MemoryManager memoryManager, IOManager ioManager,
											  TypeSerializerFactory<E> serializerFactory, TypeComparator<E> comparator,
											  List<MemorySegment> sortReadMemory, List<MemorySegment> writeMemory, int maxFileHandles) {
		return new CombiningSpillingThread(sortedDataFileFactory, spilledFiles, merger,
			exceptionHandler, queues, parentTask, memoryManager, ioManager, serializerFactory.getSerializer(),
			comparator, sortReadMemory, writeMemory, maxFileHandles, objectReuseEnabled);
	}

	// ------------------------------------------------------------------------
	// Threads
	// ------------------------------------------------------------------------

	protected class CombiningSpillingThread extends SpillingThread {

		private final TypeComparator<E> comparator2;

		private final boolean objectReuseEnabled;

		public CombiningSpillingThread(SortedDataFileFactory<E> sortedDataFileFactory,
									   BlockingQueue<SortedDataFileElement<E>> spilledFiles, SortedDataFileMerger<E> merger,
									   ExceptionHandler<IOException> exceptionHandler, CircularQueues<E> queues,
									   AbstractInvokable parentTask, MemoryManager memManager, IOManager ioManager,
									   TypeSerializer<E> serializer, TypeComparator<E> comparator,
									   List<MemorySegment> sortReadMemory, List<MemorySegment> writeMemory,
									   int maxNumFileHandles, boolean objectReuseEnabled) {
			super(sortedDataFileFactory, spilledFiles, merger, exceptionHandler, queues, parentTask,
				memManager, ioManager, serializer, comparator, sortReadMemory, writeMemory, maxNumFileHandles);

			this.comparator2 = comparator.duplicate();
			this.objectReuseEnabled = objectReuseEnabled;
		}

		/**
		 * Entry point of the thread.
		 */
		public void go() throws IOException {
			// ------------------- In-Memory Cache ------------------------

			final Queue<CircularElement<E>> cache = new ArrayDeque<CircularElement<E>>();
			CircularElement<E> element;

			// fill cache
			while (isRunning()) {
				// take next element from queue
				try {
					element = this.queues.spill.take();
				}
				catch (InterruptedException iex) {
					if (isRunning()) {
						LOG.error("Sorting thread was interrupted (without being shut down) while grabbing a buffer. " +
								"Retrying to grab buffer...");
						continue;
					} else {
						return;
					}
				}
				if (element == spillingMarker()) {
					break;
				}
				else if (element == endMarker()) {
					cacheOnly.set(true);
					break;
				}
				cache.add(element);
			}

			// check whether the thread was canceled
			if (!isRunning()) {
				return;
			}

			// ------------------- In-Memory Merge ------------------------
			if (cacheOnly.get()) {
				/* operates on in-memory segments only */
				if (LOG.isDebugEnabled()) {
					LOG.debug("Initiating in memory merge.");
				}

				List<MutableObjectIterator<E>> iterators = new ArrayList<MutableObjectIterator<E>>(cache.size());

				// iterate buffers and collect a set of iterators
				for (CircularElement<E> cached : cache) {
					iterators.add(cached.buffer.getIterator());
				}

				// release the remaining sort-buffers
				if (LOG.isDebugEnabled()) {
					LOG.debug("Releasing unused sort-buffer memory.");
				}
				disposeSortBuffers(true);

				// set lazy iterator
				MutableObjectIterator<E> resIter = iterators.isEmpty() ? EmptyMutableObjectIterator.<E>get() :
						iterators.size() == 1 ? iterators.get(0) :
						new MergeIterator<E>(iterators, this.comparator);

				setResult(new ArrayList<>(), resIter);
				// notify the merging thread
				spilledFiles.add(mergingMarker());
				return;
			}

			// ------------------- Spilling Phase ------------------------

			final GroupCombineFunction<E, E> combineStub = CombiningUnilateralSortMerger.this.combineStub;

			// now that we are actually spilling, take the combiner, and open it
			try {
				Configuration conf = CombiningUnilateralSortMerger.this.udfConfig;
				FunctionUtils.openFunction (combineStub, (conf == null ? new Configuration() : conf));
			}
			catch (Throwable t) {
				throw new IOException("The user-defined combiner failed in its 'open()' method.", t);
			}

			// loop as long as the thread is marked alive and we do not see the final element
			while (isRunning())	{
				try {
					element = takeNext(this.queues.spill, cache);
				}
				catch (InterruptedException iex) {
					if (isRunning()) {
						LOG.error("Sorting thread was interrupted (without being shut down) while grabbing a buffer. " +
								"Retrying to grab buffer...");
						continue;
					} else {
						return;
					}
				}

				// check if we are still running
				if (!isRunning()) {
					return;
				}
				// check if this is the end-of-work buffer
				if (element == endMarker()) {
					break;
				}

				// open next channel
				SortedDataFile<E> output = sortedDataFileFactory.createFile(writeMemory);
				channelDeleteRegistry.registerChannelToBeDelete(output.getChannelID());
				channelDeleteRegistry.registerOpenChannel(output.getWriteChannel());

				if (LOG.isDebugEnabled()) {
					LOG.debug("Creating temp file " + output.getChannelID().toString() + '.');
				}

				// write sort-buffer to channel
				if (LOG.isDebugEnabled()) {
					LOG.debug("Combining buffer " + element.id + '.');
				}

				// set up the combining helpers
				final InMemorySorter<E> buffer = element.buffer;
				final CombineValueIterator<E> iter = new CombineValueIterator<E>(
						buffer, this.serializer.createInstance(), this.objectReuseEnabled);
				final WriterCollector<E> collector = new WriterCollector<E>(output);

				int i = 0;
				int stop = buffer.size() - 1;

				try {
					while (i < stop) {
						int seqStart = i;
						while (i < stop && 0 == buffer.compare(i, i + 1)) {
							i++;
						}

						if (i == seqStart) {
							// no duplicate key, no need to combine. simply copy
							buffer.writeToOutput(output, seqStart, 1);
						} else {
							// get the iterator over the values
							iter.set(seqStart, i);
							// call the combiner to combine
							combineStub.combine(iter, collector);
						}
						i++;
					}
				}
				catch (Exception ex) {
					throw new IOException("An error occurred in the combiner user code.", ex);
				}

				// write the last pair, if it has not yet been included in the last iteration
				if (i == stop) {
					buffer.writeToOutput(output, stop, 1);
				}

				// done combining and writing out
				if (LOG.isDebugEnabled()) {
					LOG.debug("Combined and spilled buffer " + element.id + ".");
				}

				output.finishWriting();
				channelDeleteRegistry.unregisterOpenChannel(output.getWriteChannel());

				spilledFiles.add(new SortedDataFileElement<>(output));

				// pass empty sort-buffer to reading thread
				element.buffer.reset();
				this.queues.empty.add(element);
			}

			// done with the spilling
			if (LOG.isDebugEnabled()) {
				LOG.debug("Spilling done.");
				LOG.debug("Releasing sort-buffer memory.");
			}

			// clear the sort buffers, but do not return the memory to the manager, as we use it for merging
			disposeSortBuffers(false);


			if (LOG.isDebugEnabled()) {
				LOG.debug("Closing combiner user code.");
			}

			// close the user code
			try {
				FunctionUtils.closeFunction(combineStub);
			}
			catch (Throwable t) {
				throw new IOException("The user-defined combiner failed in its 'close()' method.", t);
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug("User code closed.");
			}

			// spilling finishes, offer the sort and spill memory to merging phase
			synchronized (mergeMemoryLock) {
				readMemoryForMerging.addAll(mergeReadMemory);
				writeMemoryForMerging.addAll(writeMemory);
				writeMemory.clear();
				mergeReadMemory.clear();
			}

			// notify the merging thread
			largeRecords = null;
			spilledFiles.add(mergingMarker());

			// done
			LOG.info("Spilling thread done.");
		}
	} // end spilling thread

	// ------------------------------------------------------------------------

	/**
	 * This class implements an iterator over values from a sort buffer. The iterator returns the values of a given
	 * interval.
	 */
	private static final class CombineValueIterator<E> implements Iterator<E>, Iterable<E> {

		private final InMemorySorter<E> buffer; // the buffer from which values are returned

		private E recordReuse;

		private final boolean objectReuseEnabled;

		private int last; // the position of the last value to be returned

		private int position; // the position of the next value to be returned

		private boolean iteratorAvailable;

		/**
		 * Creates an iterator over the values in a <tt>BufferSortable</tt>.
		 *
		 * @param buffer
		 *        The buffer to get the values from.
		 */
		public CombineValueIterator(InMemorySorter<E> buffer, E instance, boolean objectReuseEnabled) {
			this.buffer = buffer;
			this.recordReuse = instance;
			this.objectReuseEnabled = objectReuseEnabled;
		}

		/**
		 * Sets the interval for the values that are to be returned by this iterator.
		 *
		 * @param first
		 *        The position of the first value to be returned.
		 * @param last
		 *        The position of the last value to be returned.
		 */
		public void set(int first, int last) {
			this.last = last;
			this.position = first;
			this.iteratorAvailable = true;
		}

		@Override
		public boolean hasNext() {
			return this.position <= this.last;
		}

		@Override
		public E next() {
			if (this.position <= this.last) {
				try {
					E record;
					if (objectReuseEnabled) {
						record = this.buffer.getRecord(this.recordReuse, this.position);
					} else {
						record = this.buffer.getRecord(this.position);
					}
					this.position++;
					return record;
				}
				catch (IOException ioex) {
					LOG.error("Error retrieving a value from a buffer.", ioex);
					throw new RuntimeException("Could not load the next value: " + ioex.getMessage(), ioex);
				}
			}
			else {
				throw new NoSuchElementException();
			}
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Iterator<E> iterator() {
			if (iteratorAvailable) {
				iteratorAvailable = false;
				return this;
			} else {
				throw new TraversableOnceException();
			}
		}
	}
}
