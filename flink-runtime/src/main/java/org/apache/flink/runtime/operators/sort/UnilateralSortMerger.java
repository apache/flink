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

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.util.EmptyMutableObjectIterator;
import org.apache.flink.util.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@link UnilateralSortMerger} is a full fledged sorter. It implements a multi-way merge sort. Internally,
 * the logic is factored into three threads (read, sort, spill) which communicate through a set of blocking queues,
 * forming a closed loop.  Memory is allocated using the {@link MemoryManager} interface. Thus the component will
 * not exceed the provided memory limits.
 */
public class UnilateralSortMerger<E> implements Sorter<E> {

	// ------------------------------------------------------------------------
	//                              Constants
	// ------------------------------------------------------------------------

	/** Logging. */
	private static final Logger LOG = LoggerFactory.getLogger(UnilateralSortMerger.class);

	/** Fix length records with a length below this threshold will be in-place sorted, if possible. */
	private static final int THRESHOLD_FOR_IN_PLACE_SORTING = 32;

	/** The minimal number of buffers to use by the writers. */
	protected static final int MIN_NUM_WRITE_BUFFERS = 2;

	/** The maximal number of buffers to use by the writers. */
	protected static final int MAX_NUM_WRITE_BUFFERS = 4;

	/** The minimum number of segments that are required for the sort to operate. */
	protected static final int MIN_NUM_SORT_MEM_SEGMENTS = 10;

	/** The minimum number of segments that are required for the async merge reader to operate. */
	protected static final int MIN_NUM_ASYNC_MERGE_READ_MEM_SEGMENTS = 4;

	/** The ratio of async merge read memory. */
	protected static final double ASYNC_MERGE_MEMORY_RATIO = 0.1;

	// ------------------------------------------------------------------------
	//                                  Threads
	// ------------------------------------------------------------------------

	/** The thread that reads the input channels into buffers and passes them on to the merger. */
	private final ThreadBase<E> readThread;

	/** The thread that merges the buffer handed from the reading thread. */
	private final ThreadBase<E> sortThread;

	/** The thread that handles spilling to secondary storage. */
	private final ThreadBase<E> spillThread;

	/** The thread that handles merging of spilled files. */
	private final ThreadBase<E> mergingThread;

	// ------------------------------------------------------------------------
	//                                  context
	// ------------------------------------------------------------------------
	private final SortedDataFileFactory<E> sortedDataFileFactory;

	private final SortedDataFileMerger<E> merger;

	protected final boolean inMemoryResultEnabled;

	// ------------------------------------------------------------------------
	//                                   Memory
	// ------------------------------------------------------------------------

	/** The memory segments used first for sorting and later for reading/pre-fetching
	 * during the external merge. */
	protected final List<MemorySegment> sortReadMemory;

	/** The memory segments used to storage data to be written in spilling phase. */
	protected final List<MemorySegment> writeMemoryForSpilling;

	/**
	 * The spilling memory will be added to the merging memory after the finishing of
	 * spilling phase which was guarded by this lock.
	 */
	protected final Object mergeMemoryLock = new Object();

	/**
	 * The memory segments used to storage data to be written in merging phase. The
	 * memory is released after the merging phase finishes.
	 */
	@GuardedBy("mergeMemoryLock")
	protected final List<MemorySegment> writeMemoryForMerging;

	/**
	 * The memory segments used to storage data to be written in merging phase. The
	 * memory is released when the task finishes (the memory may be used by iterator
	 * after merging).
	 */
	@GuardedBy("mergeMemoryLock")
	protected final List<MemorySegment> readMemoryForMerging;

	/** The memory manager through which memory is allocated and released. */
	protected final MemoryManager memoryManager;

	// ------------------------------------------------------------------------
	//                            Miscellaneous Fields
	// ------------------------------------------------------------------------

	protected final CircularQueues<E> circularQueues;

	protected final long startSpillingBytes;

	/**
	 * The handler for large records, that do not go though the in-memory sorter as a whole, but
	 * directly go to disk.
	 */
	protected final LargeRecordHandler<E> largeRecordHandler;

	/** Sorted large records iterator. */
	protected volatile MutableObjectIterator<E> largeRecords;

	/** Maintains files to be delete when closing the sorter. */
	protected final ChannelDeleteRegistry<E> channelDeleteRegistry;

	/**
	 * The monitor which guards the iterator field.
	 */
	protected final Object iteratorLock = new Object();

	/** The files left after merging. */
	protected volatile List<SortedDataFile<E>> remainingSortedDataFiles;

	/**
	 * The iterator to be returned by the sort-merger. This variable is null, while receiving and merging is still in
	 * progress and it will be set once we have &lt; merge factor sorted sub-streams that will then be streamed sorted.
	 */
	protected volatile MutableObjectIterator<E> iterator;

	/**
	 * The exception that is set, if there is any unhandled error.
	 */
	protected volatile IOException unhandledException;

	/**
	 * Flag indicating that the sorter was closed.
	 */
	protected volatile boolean closed;

	/**
	 * Whether to reuse objects during deserialization.
	 */
	protected final boolean objectReuseEnabled;

	protected final AtomicBoolean cacheOnly;

	// ------------------------------------------------------------------------
	//                         Constructor & Shutdown
	// ------------------------------------------------------------------------

	public UnilateralSortMerger(SortedDataFileFactory<E> sortedDataFileFactory, SortedDataFileMerger<E> merger,
			MemoryManager memoryManager, IOManager ioManager,
			MutableObjectIterator<E> input, AbstractInvokable parentTask,
			TypeSerializerFactory<E> serializerFactory, TypeComparator<E> comparator,
			double memoryFraction, int maxNumFileHandles, boolean inMemoryResultEnabled, float startSpillingFraction,
			boolean handleLargeRecords, boolean objectReuseEnabled)
	throws IOException, MemoryAllocationException {
		this(sortedDataFileFactory, merger, memoryManager, ioManager, input, parentTask, serializerFactory, comparator,
			memoryFraction, -1, maxNumFileHandles, inMemoryResultEnabled, startSpillingFraction, handleLargeRecords, objectReuseEnabled);
	}

	public UnilateralSortMerger(SortedDataFileFactory<E> sortedDataFileFactory, SortedDataFileMerger<E> merger,
			MemoryManager memoryManager, IOManager ioManager,
			MutableObjectIterator<E> input, AbstractInvokable parentTask,
			TypeSerializerFactory<E> serializerFactory, TypeComparator<E> comparator,
			double memoryFraction, int numSortBuffers, int maxNumFileHandles, boolean inMemoryResultEnabled,
			float startSpillingFraction, boolean handleLargeRecords, boolean objectReuseEnabled)
	throws IOException, MemoryAllocationException {
		this(sortedDataFileFactory, merger, memoryManager, ioManager, input, parentTask, serializerFactory, comparator,
			memoryFraction, numSortBuffers, maxNumFileHandles, inMemoryResultEnabled, startSpillingFraction, false, handleLargeRecords,
			objectReuseEnabled);
	}

	public UnilateralSortMerger(SortedDataFileFactory<E> sortedDataFileFactory, SortedDataFileMerger<E> merger,
			MemoryManager memoryManager, List<MemorySegment> memory,
			IOManager ioManager,
			MutableObjectIterator<E> input, AbstractInvokable parentTask,
			TypeSerializerFactory<E> serializerFactory, TypeComparator<E> comparator,
			int numSortBuffers, int maxNumFileHandles, boolean inMemoryResultEnabled,
			float startSpillingFraction, boolean handleLargeRecords, boolean objectReuseEnabled)
	throws IOException {
		this(sortedDataFileFactory, merger, memoryManager, memory, ioManager, input, parentTask, serializerFactory, comparator,
			numSortBuffers, maxNumFileHandles, inMemoryResultEnabled, startSpillingFraction, false, handleLargeRecords,
			objectReuseEnabled, false);
	}

	protected UnilateralSortMerger(SortedDataFileFactory<E> sortedDataFileFactory, SortedDataFileMerger<E> merger,
			   MemoryManager memoryManager, IOManager ioManager,
			   MutableObjectIterator<E> input, AbstractInvokable parentTask,
			   TypeSerializerFactory<E> serializerFactory, TypeComparator<E> comparator,
			   double memoryFraction, int numSortBuffers, int maxNumFileHandles, boolean inMemoryResultEnabled,
			   float startSpillingFraction, boolean noSpillingMemory, boolean handleLargeRecords,
			   boolean objectReuseEnabled)
	throws IOException, MemoryAllocationException {
		this(sortedDataFileFactory, merger, memoryManager, memoryManager.allocatePages(parentTask, memoryManager.computeNumberOfPages(memoryFraction)),
			ioManager, input, parentTask, serializerFactory, comparator,
			numSortBuffers, maxNumFileHandles, inMemoryResultEnabled, startSpillingFraction, noSpillingMemory, handleLargeRecords,
			objectReuseEnabled, false);
	}

	protected UnilateralSortMerger(SortedDataFileFactory<E> sortedDataFileFactory, SortedDataFileMerger<E> merger,
			   MemoryManager memoryManager, List<MemorySegment> memory, IOManager ioManager,
			   MutableObjectIterator<E> input, AbstractInvokable parentTask,
			   TypeSerializerFactory<E> serializerFactory, TypeComparator<E> comparator,
			   int numSortBuffers, int maxNumFileHandles, boolean inMemoryResultEnabled,
			   float startSpillingFraction, boolean noSpillingMemory, boolean handleLargeRecords,
			   boolean objectReuseEnabled, boolean enableAsyncMerging)
	throws IOException {
		// sanity checks
		if (sortedDataFileFactory == null | merger == null | memoryManager == null | (ioManager == null && !noSpillingMemory) | serializerFactory == null | comparator == null) {
			throw new NullPointerException();
		}
		if (parentTask == null) {
			throw new NullPointerException("Parent Task must not be null.");
		}
		if (maxNumFileHandles < 2) {
			throw new IllegalArgumentException("Merger cannot work with less than two file handles.");
		}

		this.sortedDataFileFactory = sortedDataFileFactory;
		this.merger = merger;

		this.memoryManager = memoryManager;
		this.objectReuseEnabled = objectReuseEnabled;
		this.cacheOnly = new AtomicBoolean(false);

		// adjust the memory quotas to the page size
		final int numPagesTotal = memory.size();

		int minPagesNeeded = MIN_NUM_WRITE_BUFFERS + MIN_NUM_SORT_MEM_SEGMENTS ;
		if (enableAsyncMerging) {
			minPagesNeeded += MIN_NUM_ASYNC_MERGE_READ_MEM_SEGMENTS + MIN_NUM_WRITE_BUFFERS;
		}
		if (numPagesTotal < minPagesNeeded) {
			throw new IllegalArgumentException("Too little memory provided to sorter to perform task. " +
				"Required are at least " + minPagesNeeded + " pages. Current page size is " +
				memoryManager.getPageSize() + " bytes.");
		}

		// determine how many buffers to use for writing
		final int numWriteBuffers;
		final int numLargeRecordBuffers;

		if (noSpillingMemory && !handleLargeRecords) {
			numWriteBuffers = 0;
			numLargeRecordBuffers = 0;
		}
		else {
			int numWriteBufferConsumers = (noSpillingMemory ? 0 : 1) + (handleLargeRecords ? 2 : 0) + (enableAsyncMerging ? 1 : 0);

			// determine how many buffers we have when we do a full mere with maximal fan-in
			final int minBuffersForMerging = maxNumFileHandles + numWriteBufferConsumers * MIN_NUM_WRITE_BUFFERS;

			if (minBuffersForMerging > numPagesTotal) {
				numWriteBuffers = noSpillingMemory ? 0 : MIN_NUM_WRITE_BUFFERS;
				numLargeRecordBuffers = handleLargeRecords ? 2 * MIN_NUM_WRITE_BUFFERS : 0;

				maxNumFileHandles = numPagesTotal - numWriteBufferConsumers * MIN_NUM_WRITE_BUFFERS;
				if (LOG.isDebugEnabled()) {
					LOG.debug("Reducing maximal merge fan-in to " + maxNumFileHandles + " due to limited memory availability during merge");
				}
			}
			else {
				// we are free to choose. make sure that we do not eat up too much memory for writing
				final int fractionalAuxBuffers = numPagesTotal / (numWriteBufferConsumers * 100);

				if (fractionalAuxBuffers >= MAX_NUM_WRITE_BUFFERS) {
					numWriteBuffers = noSpillingMemory ? 0 : MAX_NUM_WRITE_BUFFERS;
					numLargeRecordBuffers = handleLargeRecords ? 2 * MAX_NUM_WRITE_BUFFERS : 0;
				}
				else {
					numWriteBuffers = noSpillingMemory ? 0 :
							Math.max(MIN_NUM_WRITE_BUFFERS, fractionalAuxBuffers);	// at least the lower bound

					numLargeRecordBuffers = handleLargeRecords ?
							Math.max(2 * MIN_NUM_WRITE_BUFFERS, fractionalAuxBuffers) // at least the lower bound
							: 0;
				}
			}
		}

		int sortMemPages = numPagesTotal - numWriteBuffers - numLargeRecordBuffers;
		int numMergeReadBuffers = 0;
		if (enableAsyncMerging) {
			sortMemPages = sortMemPages - numWriteBuffers;
			numMergeReadBuffers = Math.max((int) (sortMemPages * ASYNC_MERGE_MEMORY_RATIO), MIN_NUM_ASYNC_MERGE_READ_MEM_SEGMENTS);
			sortMemPages = sortMemPages - numMergeReadBuffers;
		}
		final long sortMemory = ((long) sortMemPages) * memoryManager.getPageSize();

		// decide how many sort buffers to use
		if (numSortBuffers < 1) {
			if (sortMemory > 100 * 1024 * 1024) {
				numSortBuffers = 2;
			}
			else {
				numSortBuffers = 1;
			}
		}
		final int numSegmentsPerSortBuffer = sortMemPages / numSortBuffers;

		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("Instantiating sorter with %d pages of sorting memory (="
					+ "%d bytes total) divided over %d sort buffers (%d pages per buffer). Using %d"
					+ " buffers for writing sorted results and merging maximally %d streams at once. "
					+ "Using %d memory segments for large record spilling.",
					sortMemPages, sortMemory, numSortBuffers, numSegmentsPerSortBuffer, numWriteBuffers,
					maxNumFileHandles, numLargeRecordBuffers));
		}

		this.sortReadMemory = memory;
		this.writeMemoryForSpilling = new ArrayList<MemorySegment>(numWriteBuffers);

		final TypeSerializer<E> serializer = serializerFactory.getSerializer();

		// move some pages from the sort memory to the write memory
		for (int i = 0; i < numWriteBuffers; i++) {
			this.writeMemoryForSpilling.add(this.sortReadMemory.remove(this.sortReadMemory.size() - 1));
		}

		this.readMemoryForMerging = new ArrayList<>();
		this.writeMemoryForMerging = new ArrayList<>(numWriteBuffers);
		if (enableAsyncMerging) {
			for (int i = 0; i < numWriteBuffers; i++) {
				this.writeMemoryForMerging.add(this.sortReadMemory.remove(this.sortReadMemory.size() - 1));
			}
			for (int i = 0; i < numMergeReadBuffers; i++) {
				this.readMemoryForMerging.add(this.sortReadMemory.remove(this.sortReadMemory.size() - 1));
			}
		}

		if (numLargeRecordBuffers > 0) {
			List<MemorySegment> mem = new ArrayList<MemorySegment>();
			for (int i = 0; i < numLargeRecordBuffers; i++) {
				mem.add(this.sortReadMemory.remove(this.sortReadMemory.size() - 1));
			}

			this.largeRecordHandler = new LargeRecordHandler<E>(serializer, comparator.duplicate(),
					ioManager, memoryManager, mem, parentTask, maxNumFileHandles);
		}
		else {
			this.largeRecordHandler = null;
		}

		// circular queues pass buffers between the threads
		this.circularQueues = new CircularQueues<E>();

		// allocate the sort buffers and fill empty queue with them
		final Iterator<MemorySegment> segments = this.sortReadMemory.iterator();
		for (int i = 0; i < numSortBuffers; i++)
		{
			// grab some memory
			final List<MemorySegment> sortSegments = new ArrayList<MemorySegment>(numSegmentsPerSortBuffer);
			for (int k = (i == numSortBuffers - 1 ? Integer.MAX_VALUE : numSegmentsPerSortBuffer); k > 0 && segments.hasNext(); k--) {
				sortSegments.add(segments.next());
			}

			final TypeComparator<E> comp = comparator.duplicate();
			final InMemorySorter<E> buffer;

			// instantiate a fix-length in-place sorter, if possible, otherwise the out-of-place sorter
			if (comp.supportsSerializationWithKeyNormalization() &&
					serializer.getLength() > 0 && serializer.getLength() <= THRESHOLD_FOR_IN_PLACE_SORTING)
			{
				buffer = new FixedLengthRecordSorter<E>(serializerFactory.getSerializer(), comp, sortSegments);
			} else {
				buffer = new NormalizedKeySorter<E>(serializerFactory.getSerializer(), comp, sortSegments);
			}

			// add to empty queue
			CircularElement<E> element = new CircularElement<E>(i, buffer, sortSegments);
			circularQueues.empty.add(element);
		}

		// exception handling
		ExceptionHandler<IOException> exceptionHandler = new ExceptionHandler<IOException>() {
			public void handleException(IOException exception) {
				// forward exception
				if (!closed) {
					setResultException(exception);
					close();
				}
			}
		};

		// create sets that track the channels we need to clean up when closing the sorter
		this.channelDeleteRegistry = new ChannelDeleteRegistry<>();

		this.inMemoryResultEnabled = inMemoryResultEnabled;
		// If un-spilled caches are not allowed, desert the caching.
		if (!inMemoryResultEnabled) {
			startSpillingFraction = 0;
		}

		BlockingQueue<SortedDataFileElement<E>> spilledFiles = new LinkedBlockingQueue<>();

		this.startSpillingBytes = ((long) (startSpillingFraction * sortMemory));

		// start the thread that reads the input channels
		this.readThread = getReadingThread(exceptionHandler, input, circularQueues, largeRecordHandler,
				parentTask, serializer, startSpillingBytes);

		// start the thread that sorts the buffers
		this.sortThread = getSortingThread(exceptionHandler, circularQueues, parentTask);

		// start the thread that handles spilling to secondary storage
		this.spillThread = getSpillingThread(sortedDataFileFactory, spilledFiles, merger,
			exceptionHandler, circularQueues, parentTask, memoryManager, ioManager, serializerFactory,
			comparator, this.sortReadMemory, this.writeMemoryForSpilling, maxNumFileHandles);

		// start the thread that handles merging of spilled files
		this.mergingThread = getMergingThread(merger, memoryManager, exceptionHandler, parentTask, spilledFiles);

		// propagate the context class loader to the spawned threads
		ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
		if (contextLoader != null) {
			if (this.readThread != null) {
				this.readThread.setContextClassLoader(contextLoader);
			}
			if (this.sortThread != null) {
				this.sortThread.setContextClassLoader(contextLoader);
			}
			if (this.spillThread != null) {
				this.spillThread.setContextClassLoader(contextLoader);
			}
			if (this.mergingThread != null) {
				this.mergingThread.setContextClassLoader(contextLoader);
			}
		}

		startThreads();
	}

	/**
	 * Starts all the threads that are used by this sort-merger.
	 */
	protected void startThreads() {
		if (this.readThread != null) {
			this.readThread.start();
		}
		if (this.sortThread != null) {
			this.sortThread.start();
		}
		if (this.spillThread != null) {
			this.spillThread.start();
		}
		if (this.mergingThread != null) {
			this.mergingThread.start();
		}
	}

	/**
	 * Shuts down all the threads initiated by this sort/merger. Also releases all previously allocated
	 * memory, if it has not yet been released by the threads, and closes and deletes all channels (removing
	 * the temporary files).
	 * <p>
	 * The threads are set to exit directly, but depending on their operation, it may take a while to actually happen.
	 * The sorting thread will for example not finish before the current batch is sorted. This method attempts to wait
	 * for the working thread to exit. If it is however interrupted, the method exits immediately and is not guaranteed
	 * how long the threads continue to exist and occupy resources afterwards.
	 *
	 * @see java.io.Closeable#close()
	 */
	@Override
	public void close() {
		// check if the sorter has been closed before
		synchronized (this) {
			if (this.closed) {
				return;
			}

			// mark as closed
			this.closed = true;
		}

		// from here on, the code is in a try block, because even through errors might be thrown in this block,
		// we need to make sure that all the memory is released.
		try {
			// if the result iterator has not been obtained yet, set the exception
			synchronized (this.iteratorLock) {
				if (this.unhandledException == null) {
					this.unhandledException = new IOException("The sorter has been closed.");
					this.iteratorLock.notifyAll();
				}
			}

			// stop all the threads
			if (this.readThread != null) {
				try {
					this.readThread.shutdown();
				} catch (Throwable t) {
					LOG.error("Error shutting down reader thread: " + t.getMessage(), t);
				}
			}
			if (this.sortThread != null) {
				try {
					this.sortThread.shutdown();
				} catch (Throwable t) {
					LOG.error("Error shutting down sorter thread: " + t.getMessage(), t);
				}
			}
			if (this.spillThread != null) {
				try {
					this.spillThread.shutdown();
				} catch (Throwable t) {
					LOG.error("Error shutting down spilling thread: " + t.getMessage(), t);
				}
			}
			if (this.mergingThread != null) {
				try {
					this.mergingThread.shutdown();
				} catch (Throwable t) {
					LOG.error("Error shutting down merging thread: " + t.getMessage(), t);
				}
			}

			try {
				if (this.readThread != null) {
					this.readThread.join();
				}

				if (this.sortThread != null) {
					this.sortThread.join();
				}

				if (this.spillThread != null) {
					this.spillThread.join();
				}

				if (this.mergingThread != null) {
					this.mergingThread.join();
				}
			}
			catch (InterruptedException iex) {
				LOG.debug("Closing of sort/merger was interrupted. " +
						"The reading/sorting/spilling threads may still be working.", iex);
			}
		}
		finally {

			// RELEASE ALL MEMORY. If the threads and channels are still running, this should cause
			// exceptions, because their memory segments are freed
			try {
				if (!this.writeMemoryForSpilling.isEmpty()) {
					this.memoryManager.release(this.writeMemoryForSpilling);
				}
				this.writeMemoryForSpilling.clear();

				if (!this.writeMemoryForMerging.isEmpty()) {
					this.memoryManager.release(this.writeMemoryForMerging);
				}
				this.writeMemoryForMerging.clear();
			}
			catch (Throwable t) {}

			try {
				if (!this.sortReadMemory.isEmpty()) {
					this.memoryManager.release(this.sortReadMemory);
				}
				this.sortReadMemory.clear();

				if (!this.readMemoryForMerging.isEmpty()) {
					this.memoryManager.release(this.readMemoryForMerging);
				}
				this.readMemoryForMerging.clear();
			}
			catch (Throwable t) {}

			channelDeleteRegistry.clearOpenFiles();
			channelDeleteRegistry.clearFiles();

			try {
				if (this.largeRecordHandler != null) {
					this.largeRecordHandler.close();
				}
			} catch (Throwable t) {}
		}
	}

	// ------------------------------------------------------------------------
	//                           Factory Methods
	// ------------------------------------------------------------------------

	/**
	 * Creates the reading thread. The reading thread simply reads the data off the input and puts it
	 * into the buffer where it will be sorted.
	 * <p>
	 * The returned thread is not yet started.
	 *
	 * @param exceptionHandler
	 *        The handler for exceptions in the thread.
	 * @param reader
	 *        The reader from which the thread reads.
	 * @param queues
	 *        The queues through which the thread communicates with the other threads.
	 * @param parentTask
	 *        The task at which the thread registers itself (for profiling purposes).
	 * @param serializer
	 *        The serializer used to serialize records.
	 * @param startSpillingBytes
	 *        The number of bytes after which the reader thread will send the notification to
	 *        start the spilling.
	 *
	 * @return The thread that reads data from an input, writes it into sort buffers and puts
	 *         them into a queue.
	 */
	protected ThreadBase<E> getReadingThread(ExceptionHandler<IOException> exceptionHandler,
											 MutableObjectIterator<E> reader, CircularQueues<E> queues,
											 LargeRecordHandler<E> largeRecordHandler, AbstractInvokable parentTask,
											 TypeSerializer<E> serializer, long startSpillingBytes) {
		return new ReadingThread<E>(exceptionHandler, reader, queues, largeRecordHandler,
				serializer.createInstance(),parentTask, startSpillingBytes);
	}

	protected ThreadBase<E> getSortingThread(ExceptionHandler<IOException> exceptionHandler,
											 CircularQueues<E> queues, AbstractInvokable parentTask) {
		return new SortingThread<E>(exceptionHandler, queues, parentTask);
	}


	protected ThreadBase<E> getSpillingThread(SortedDataFileFactory<E> sortedDataFileFactory, BlockingQueue<SortedDataFileElement<E>> spilledFiles,
											  SortedDataFileMerger<E> merger, ExceptionHandler<IOException> exceptionHandler,
											  CircularQueues<E> queues, AbstractInvokable parentTask, MemoryManager memoryManager,
											  IOManager ioManager, TypeSerializerFactory<E> serializerFactory,
											  TypeComparator<E> comparator, List<MemorySegment> sortReadMemory,
											  List<MemorySegment> writeMemory, int maxFileHandles) {
		return new SpillingThread(sortedDataFileFactory, spilledFiles, merger, exceptionHandler, queues, parentTask,
			memoryManager, ioManager, serializerFactory.getSerializer(), comparator, sortReadMemory, writeMemory, maxFileHandles);
	}

	protected ThreadBase<E> getMergingThread(SortedDataFileMerger<E> merger, MemoryManager memoryManager,
											 ExceptionHandler<IOException> exceptionHandler, AbstractInvokable parentTask,
											 BlockingQueue<SortedDataFileElement<E>> spilledFiles) {
		return new MergingThread(merger, memoryManager, spilledFiles, exceptionHandler, parentTask);
	}

	// ------------------------------------------------------------------------
	//                           Result Iterator
	// ------------------------------------------------------------------------

	@Override
	public List<SortedDataFile<E>> getRemainingSortedDataFiles() throws InterruptedException {
		synchronized (this.iteratorLock) {
			// wait while both the result and the exception are not set
			while (this.remainingSortedDataFiles == null && this.unhandledException == null) {
				this.iteratorLock.wait();
			}

			if (this.unhandledException != null) {
				throw new RuntimeException("Error obtaining the sorted input: " + this.unhandledException.getMessage(),
					this.unhandledException);
			}
			else {
				return this.remainingSortedDataFiles;
			}
		}
	}

	@Override
	public MutableObjectIterator<E> getIterator() throws InterruptedException {
		synchronized (this.iteratorLock) {
			// wait while both the result and the exception are not set
			while (this.iterator == null && this.unhandledException == null) {
				this.iteratorLock.wait();
			}

			if (this.unhandledException != null) {
				throw new RuntimeException("Error obtaining the sorted input: " + this.unhandledException.getMessage(),
					this.unhandledException);
			}
			else {
				return this.iterator;
			}
		}
	}

	/**
	 * Sets the result iterator and remaining file list. By setting the result iterator, all threads that are waiting for the result
	 * iterator are notified and will obtain it.
	 *
	 * @param iterator The result iterator to set.
	 */
	protected final void setResult(List<SortedDataFile<E>> mergedDataFiles, MutableObjectIterator<E> iterator) {
		synchronized (this.iteratorLock) {
			// set the result only, if no exception has occurred
			if (this.unhandledException == null) {
				this.remainingSortedDataFiles = mergedDataFiles;
				this.iterator = iterator;
				this.iteratorLock.notifyAll();
			}
		}
	}

	/**
	 * Reports an exception to all threads that are waiting for the result iterator.
	 *
	 * @param ioex The exception to be reported to the threads that wait for the result iterator.
	 */
	protected final void setResultException(IOException ioex) {
		synchronized (this.iteratorLock) {
			if (this.unhandledException == null) {
				this.unhandledException = ioex;
				this.iteratorLock.notifyAll();

				LOG.error("Unhandled exception occurs.", this.unhandledException);
			}
		}
	}

	// ------------------------------------------------------------------------
	// Inter-Thread Communication
	// ------------------------------------------------------------------------

	/**
	 * The element that is passed as marker for the end of data.
	 */
	private static final CircularElement<Object> EOF_MARKER = new CircularElement<Object>();

	/**
	 * The element that is passed as marker for signal beginning of spilling.
	 */
	private static final CircularElement<Object> SPILLING_MARKER = new CircularElement<Object>();

	/**
	 * The element that is passed as marker for signal beginning of sync merging (ending of spilling).
	 */
	private static final  SortedDataFileElement<Object> MERGING_MARKER = new SortedDataFileElement<>();

	/**
	 * Gets the element that is passed as marker for the end of data.
	 *
	 * @return The element that is passed as marker for the end of data.
	 */
	protected static <T> CircularElement<T> endMarker() {
		@SuppressWarnings("unchecked")
		CircularElement<T> c = (CircularElement<T>) EOF_MARKER;
		return c;
	}

	/**
	 * Gets the element that is passed as marker for signal beginning of spilling.
	 *
	 * @return The element that is passed as marker for signal beginning of spilling.
	 */
	protected static <T> CircularElement<T> spillingMarker() {
		@SuppressWarnings("unchecked")
		CircularElement<T> c = (CircularElement<T>) SPILLING_MARKER;
		return c;
	}

	/**
	 * Gets the element that is passed as marker for signal beginning of sync merging.
	 *
	 * @return The element that is passed as marker for signal beginning of sync merging.
	 */
	protected static <T> SortedDataFileElement<T> mergingMarker() {
		@SuppressWarnings("unchecked")
		SortedDataFileElement<T> s = (SortedDataFileElement<T>) MERGING_MARKER;
		return s;
	}

	/**
	 * The sorted data file element added to the sorted data file blocking queue. A
	 * special merging marker object will be created and added to the queue to indicate
	 * the finish of spilling phase.
	 */
	protected static final class SortedDataFileElement<E> {
		final SortedDataFile<E> sortedDataFile;

		public SortedDataFileElement() {
			sortedDataFile = null;
		}

		public SortedDataFileElement(SortedDataFile<E> sortedDataFile) {
			this.sortedDataFile = sortedDataFile;
		}
	}

	/**
	 * Class representing buffers that circulate between the reading, sorting and spilling thread.
	 */
	protected static final class CircularElement<E> {

		final int id;
		final InMemorySorter<E> buffer;
		final List<MemorySegment> memory;

		public CircularElement() {
			this.id = -1;
			this.buffer = null;
			this.memory = null;
		}

		public CircularElement(int id, InMemorySorter<E> buffer, List<MemorySegment> memory) {
			this.id = id;
			this.buffer = buffer;
			this.memory = memory;
		}
	}

	/**
	 * Collection of queues that are used for the communication between the threads.
	 */
	protected static final class CircularQueues<E> {

		final BlockingQueue<CircularElement<E>> empty;

		final BlockingQueue<CircularElement<E>> sort;

		final BlockingQueue<CircularElement<E>> spill;

		public CircularQueues() {
			this.empty = new LinkedBlockingQueue<CircularElement<E>>();
			this.sort = new LinkedBlockingQueue<CircularElement<E>>();
			this.spill = new LinkedBlockingQueue<CircularElement<E>>();
		}

		public CircularQueues(int numElements) {
			this.empty = new ArrayBlockingQueue<CircularElement<E>>(numElements);
			this.sort = new ArrayBlockingQueue<CircularElement<E>>(numElements);
			this.spill = new ArrayBlockingQueue<CircularElement<E>>(numElements);
		}
	}

	// ------------------------------------------------------------------------
	// Threads
	// ------------------------------------------------------------------------

	/**
	 * Base class for all working threads in this sort-merger. The specific threads for reading, sorting, spilling,
	 * merging, etc... extend this subclass.
	 * <p>
	 * The threads are designed to terminate themselves when the task they are set up to do is completed. Further more,
	 * they terminate immediately when the <code>shutdown()</code> method is called.
	 */
	protected static abstract class ThreadBase<E> extends Thread implements Thread.UncaughtExceptionHandler {

		/**
		 * The queue of empty buffer that can be used for reading;
		 */
		protected final CircularQueues<E> queues;

		/**
		 * The exception handler for any problems.
		 */
		private final ExceptionHandler<IOException> exceptionHandler;

		/**
		 * The flag marking this thread as alive.
		 */
		private final AtomicBoolean alive;

		/**
		 * Creates a new thread.
		 *
		 * @param exceptionHandler The exception handler to call for all exceptions.
		 * @param name The name of the thread.
		 * @param queues The queues used to pass buffers between the threads.
		 * @param parentTask The task that started this thread. If non-null, it is used to register this thread.
		 */
		protected ThreadBase(ExceptionHandler<IOException> exceptionHandler, String name, CircularQueues<E> queues,
				AbstractInvokable parentTask)
		{
			// thread setup
			super(name);
			this.setDaemon(true);

			// exception handling
			this.exceptionHandler = exceptionHandler;
			this.setUncaughtExceptionHandler(this);

			this.queues = queues;
			this.alive = new AtomicBoolean(true);
		}

		/**
		 * Implements exception handling and delegates to go().
		 */
		public void run() {
			try {
				go();
			}
			catch (Throwable t) {
				internalHandleException(new IOException("Thread '" + getName() + "' terminated due to an exception: "
					+ t.getMessage(), t));
			}
		}

		/**
		 * Equivalent to the run() method.
		 *
		 * @throws IOException Exceptions that prohibit correct completion of the work may be thrown by the thread.
		 */
		protected abstract void go() throws IOException;

		/**
		 * Checks whether this thread is still alive.
		 *
		 * @return true, if the thread is alive, false otherwise.
		 */
		public boolean isRunning() {
			return this.alive.get();
		}

		public AtomicBoolean getRunningFlag() {
			return alive;
		}

		/**
		 * Forces an immediate shutdown of the thread. Looses any state and all buffers that the thread is currently
		 * working on. This terminates cleanly for the JVM, but looses intermediate results.
		 */
		public void shutdown() {
			this.alive.set(false);
			this.interrupt();
		}

		/**
		 * Internally handles an exception and makes sure that this method returns without a problem.
		 *
		 * @param ioex
		 *        The exception to handle.
		 */
		protected final void internalHandleException(IOException ioex) {
			if (!isRunning()) {
				// discard any exception that occurs when after the thread is killed.
				return;
			}
			if (this.exceptionHandler != null) {
				try {
					this.exceptionHandler.handleException(ioex);
				}
				catch (Throwable t) {}
			}
		}

		/* (non-Javadoc)
		 * @see java.lang.Thread.UncaughtExceptionHandler#uncaughtException(java.lang.Thread, java.lang.Throwable)
		 */
		@Override
		public void uncaughtException(Thread t, Throwable e) {
			internalHandleException(new IOException("Thread '" + t.getName()
				+ "' terminated due to an uncaught exception: " + e.getMessage(), e));
		}
	}

	/**
	 * The thread that consumes the input data and puts it into a buffer that will be sorted.
	 */
	protected static class ReadingThread<E> extends ThreadBase<E> {

		/** The input channels to read from. */
		private final MutableObjectIterator<E> reader;

		private final LargeRecordHandler<E> largeRecords;

		/** The fraction of the buffers that must be full before the spilling starts. */
		private final long startSpillingBytes;

		/** The object into which the thread reads the data from the input. */
		private final E readTarget;

		/**
		 * Creates a new reading thread.
		 *
		 * @param exceptionHandler The exception handler to call for all exceptions.
		 * @param reader The reader to pull the data from.
		 * @param queues The queues used to pass buffers between the threads.
		 * @param parentTask The task that started this thread. If non-null, it is used to register this thread.
		 */
		public ReadingThread(ExceptionHandler<IOException> exceptionHandler,
				MutableObjectIterator<E> reader, CircularQueues<E> queues,
				LargeRecordHandler<E> largeRecordsHandler, E readTarget,
				AbstractInvokable parentTask, long startSpillingBytes)
		{
			super(exceptionHandler, "SortMerger Reading Thread", queues, parentTask);

			// members
			this.reader = reader;
			this.readTarget = readTarget;
			this.startSpillingBytes = startSpillingBytes;
			this.largeRecords = largeRecordsHandler;
		}

		/**
		 * The entry point for the thread. Gets a buffer for all threads and then loops as long as there is input
		 * available.
		 */
		public void go() throws IOException {

			final MutableObjectIterator<E> reader = this.reader;

			E current = this.readTarget;
			E leftoverRecord = null;

			CircularElement<E> element = null;
			long bytesUntilSpilling = this.startSpillingBytes;
			boolean done = false;

			// check if we should directly spill
			if (bytesUntilSpilling < 1) {
				bytesUntilSpilling = 0;

				// add the spilling marker
				this.queues.sort.add(UnilateralSortMerger.<E>spillingMarker());
			}

			// now loop until all channels have no more input data
			while (!done && isRunning())
			{
				// grab the next buffer
				while (element == null) {
					try {
						element = this.queues.empty.take();
					}
					catch (InterruptedException iex) {
						throw new IOException(iex);
					}
				}

				// get the new buffer and check it
				final InMemorySorter<E> buffer = element.buffer;
				if (!buffer.isEmpty()) {
					throw new IOException("New buffer is not empty.");
				}

				if (LOG.isDebugEnabled()) {
					LOG.debug("Retrieved empty read buffer " + element.id + ".");
				}

				// write the last leftover pair, if we have one
				if (leftoverRecord != null) {
					if (!buffer.write(leftoverRecord)) {

						// did not fit in a fresh buffer, must be large...
						if (this.largeRecords != null) {
							if (LOG.isDebugEnabled()) {
								LOG.debug("Large record did not fit into a fresh sort buffer. Putting into large record store.");
							}
							this.largeRecords.addRecord(leftoverRecord);
						}
						else {
							throw new IOException("The record exceeds the maximum size of a sort buffer (current maximum: "
									+ buffer.getCapacity() + " bytes).");
						}
						buffer.reset();
					}

					leftoverRecord = null;
				}

				// we have two distinct code paths, depending on whether the spilling
				// threshold will be crossed in the current buffer, or not.
				boolean available = true;
				if (bytesUntilSpilling > 0 && buffer.getCapacity() >= bytesUntilSpilling)
				{
					boolean fullBuffer = false;

					// spilling will be triggered while this buffer is filled
					// loop until the buffer is full or the reader is exhausted
					E newCurrent;
					while (isRunning() && (available = (newCurrent = reader.next(current)) != null))
					{
						current = newCurrent;
						if (!buffer.write(current)) {
							leftoverRecord = current;
							fullBuffer = true;
							break;
						}

						// successfully added record

						if (bytesUntilSpilling - buffer.getOccupancy() <= 0) {
							bytesUntilSpilling = 0;

							// send the spilling marker
							final CircularElement<E> SPILLING_MARKER = spillingMarker();
							this.queues.sort.add(SPILLING_MARKER);

							// we drop out of this loop and continue with the loop that
							// does not have the check
							break;
						}
					}

					if (fullBuffer) {
						// buffer is full. it may be that the last element would have crossed the
						// spilling threshold, so check it
						if (bytesUntilSpilling > 0) {
							bytesUntilSpilling -= buffer.getCapacity();
							if (bytesUntilSpilling <= 0) {
								bytesUntilSpilling = 0;
								// send the spilling marker
								final CircularElement<E> SPILLING_MARKER = spillingMarker();
								this.queues.sort.add(SPILLING_MARKER);
							}
						}

						// send the buffer
						if (LOG.isDebugEnabled()) {
							LOG.debug("Emitting full buffer from reader thread: " + element.id + ".");
						}
						this.queues.sort.add(element);
						element = null;
						continue;
					}
				}
				else if (bytesUntilSpilling > 0) {
					// this block must not be entered, if the last loop dropped out because
					// the input is exhausted.
					bytesUntilSpilling -= buffer.getCapacity();
					if (bytesUntilSpilling <= 0) {
						bytesUntilSpilling = 0;
						// send the spilling marker
						final CircularElement<E> SPILLING_MARKER = spillingMarker();
						this.queues.sort.add(SPILLING_MARKER);
					}
				}

				// no spilling will be triggered (any more) while this buffer is being processed
				// loop until the buffer is full or the reader is exhausted
				if (available) {
					E newCurrent;
					while (isRunning() && ((newCurrent = reader.next(current)) != null)) {
						current = newCurrent;
						if (!buffer.write(current)) {
							leftoverRecord = current;
							break;
						}
					}
				}

				// check whether the buffer is exhausted or the reader is
				if (leftoverRecord != null) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Emitting full buffer from reader thread: " + element.id + ".");
					}
				}
				else {
					done = true;
					if (LOG.isDebugEnabled()) {
						LOG.debug("Emitting final buffer from reader thread: " + element.id + ".");
					}
				}


				// we can use add to add the element because we have no capacity restriction
				if (!buffer.isEmpty()) {
					this.queues.sort.add(element);
				}
				else {
					buffer.reset();
					this.queues.empty.add(element);
				}
				element = null;
			}

			// we read all there is to read, or we are no longer running
			if (!isRunning()) {
				return;
			}

			// add the sentinel to notify the receivers that the work is done
			// send the EOF marker
			final CircularElement<E> EOF_MARKER = endMarker();
			this.queues.sort.add(EOF_MARKER);
			LOG.info("Reading thread done.");
		}
	}

	/**
	 * The thread that sorts filled buffers.
	 */
	protected static class SortingThread<E> extends ThreadBase<E> {

		private final IndexedSorter sorter;

		/**
		 * Creates a new sorting thread.
		 *
		 * @param exceptionHandler The exception handler to call for all exceptions.
		 * @param queues The queues used to pass buffers between the threads.
		 * @param parentTask The task that started this thread. If non-null, it is used to register this thread.
		 */
		public SortingThread(ExceptionHandler<IOException> exceptionHandler, CircularQueues<E> queues,
				AbstractInvokable parentTask) {
			super(exceptionHandler, "SortMerger sorting thread", queues, parentTask);

			// members
			this.sorter = new QuickSort();
		}

		/**
		 * Entry point of the thread.
		 */
		public void go() throws IOException {
			boolean alive = true;

			// loop as long as the thread is marked alive
			while (isRunning() && alive) {
				CircularElement<E> element = null;
				try {
					element = this.queues.sort.take();
				}
				catch (InterruptedException iex) {
					if (isRunning()) {
						if (LOG.isErrorEnabled()) {
							LOG.error(
								"Sorting thread was interrupted (without being shut down) while grabbing a buffer. " +
								"Retrying to grab buffer...");
						}
						continue;
					}
					else {
						return;
					}
				}

				if (element != EOF_MARKER && element != SPILLING_MARKER) {

					if (element.buffer.size() == 0) {
						element.buffer.reset();
						this.queues.empty.add(element);
						continue;
					}

					if (LOG.isDebugEnabled()) {
						LOG.debug("Sorting buffer " + element.id + ".");
					}

					this.sorter.sort(element.buffer);

					if (LOG.isDebugEnabled()) {
						LOG.debug("Sorted buffer " + element.id + ".");
					}
				}
				else if (element == EOF_MARKER) {
					LOG.info("Sorting thread done.");
					alive = false;
				}
				this.queues.spill.add(element);
			}
		}
	}

	/**
	 * The thread that handles the spilling of intermediate results and sets up the merging.
	 */
	protected class SpillingThread extends ThreadBase<E> {

		protected final SortedDataFileFactory<E> sortedDataFileFactory;

		protected final SortedDataFileMerger<E> merger;

		protected final MemoryManager memManager;			// memory manager to release memory

		protected final IOManager ioManager;				// I/O manager to create channels

		protected final TypeSerializer<E> serializer;		// The serializer for the data type

		protected final TypeComparator<E> comparator;		// The comparator that establishes the order relation.

		protected final List<MemorySegment> writeMemory;	// memory segments for writing

		protected final List<MemorySegment> mergeReadMemory;	// memory segments for sorting/reading

		protected final BlockingQueue<SortedDataFileElement<E>> spilledFiles;

		protected final int mergeFactor;

		protected final int numWriteBuffersToCluster;

		protected int numSpilledFiles;

		/**
		 * Creates the spilling thread.
		 *
		 * @param exceptionHandler The exception handler to call for all exceptions.
		 * @param queues The queues used to pass buffers between the threads.
		 * @param parentTask The task that started this thread. If non-null, it is used to register this thread.
		 * @param memManager The memory manager used to allocate buffers for the readers and writers.
		 * @param ioManager The I/I manager used to instantiate readers and writers from.
		 * @param serializer
		 * @param comparator
		 * @param sortReadMemory
		 * @param writeMemory
		 * @param maxNumFileHandles
		 */
		public SpillingThread(SortedDataFileFactory<E> sortedDataFileFactory, BlockingQueue<SortedDataFileElement<E>> spilledFiles,
							  SortedDataFileMerger<E> merger, ExceptionHandler<IOException> exceptionHandler,
							  CircularQueues<E> queues, AbstractInvokable parentTask, MemoryManager memManager,
							  IOManager ioManager, TypeSerializer<E> serializer, TypeComparator<E> comparator,
							  List<MemorySegment> sortReadMemory, List<MemorySegment> writeMemory, int maxNumFileHandles)
		{
			super(exceptionHandler, "SortMerger spilling thread", queues, parentTask);
			this.sortedDataFileFactory = checkNotNull(sortedDataFileFactory);
			this.spilledFiles = checkNotNull(spilledFiles);
			this.merger = checkNotNull(merger);
			this.memManager = checkNotNull(memManager);
			this.ioManager = checkNotNull(ioManager);
			this.serializer = checkNotNull(serializer);
			this.comparator = checkNotNull(comparator);
			this.mergeReadMemory = checkNotNull(sortReadMemory);
			this.writeMemory = checkNotNull(writeMemory);
			this.mergeFactor = maxNumFileHandles;
			this.numWriteBuffersToCluster = writeMemory.size() >= 4 ? writeMemory.size() / 2 : 1;
			this.numSpilledFiles = 0;
		}

		/**
		 * Entry point of the thread.
		 */
		public void go() throws IOException {
			final Queue<CircularElement<E>> cache = new ArrayDeque<CircularElement<E>>();
			CircularElement<E> element;

			// ------------------- In-Memory Cache ------------------------
			// fill cache
			while (isRunning()) {
				// take next element from queue
				try {
					element = this.queues.spill.take();
				}
				catch (InterruptedException iex) {
					throw new IOException("The spilling thread was interrupted.");
				}

				if (element == SPILLING_MARKER) {
					break;
				}
				else if (element == EOF_MARKER) {
					cacheOnly.set(true);
					break;
				}
				cache.add(element);
			}

			// check whether the thread was canceled
			if (!isRunning()) {
				return;
			}

			// check if we can stay in memory with the large record handler
			if (cacheOnly.get() && largeRecordHandler != null && largeRecordHandler.hasData()) {
				List<MemorySegment> memoryForLargeRecordSorting = new ArrayList<MemorySegment>();

				CircularElement<E> circElement;
				while ((circElement = this.queues.empty.poll()) != null) {
					circElement.buffer.dispose();
					memoryForLargeRecordSorting.addAll(circElement.memory);
				}

				if (memoryForLargeRecordSorting.isEmpty()) {
					cacheOnly.set(false);
					LOG.debug("Going to disk-based merge because of large records.");

				} else {
					LOG.debug("Sorting large records, to add them to in-memory merge.");
					largeRecords = largeRecordHandler.finishWriteAndSortKeys(memoryForLargeRecordSorting);
				}
			}

			// ------------------- In-Memory Merge ------------------------
			if (cacheOnly.get()) {
				// operates on in-memory buffers only
				if (LOG.isDebugEnabled()) {
					LOG.debug("Initiating in memory merge.");
				}

				List<MutableObjectIterator<E>> iterators = new ArrayList<MutableObjectIterator<E>>(cache.size() + 1);

				// iterate buffers and collect a set of iterators
				for (CircularElement<E> cached : cache) {
					// note: the yielded iterator only operates on the buffer heap (and disregards the stack)
					iterators.add(cached.buffer.getIterator());
				}

				if (largeRecords != null) {
					iterators.add(largeRecords);
				}

				// release the remaining sort-buffers
				if (LOG.isDebugEnabled()) {
					LOG.debug("Releasing unused sort-buffer memory.");
				}
				disposeSortBuffers(true);

				// set lazy iterator
				setResult(new ArrayList<>(), iterators.isEmpty() ? EmptyMutableObjectIterator.<E>get() :
						iterators.size() == 1 ? iterators.get(0) : new MergeIterator<E>(iterators, this.comparator));
				// notify the merging thread
				spilledFiles.add(mergingMarker());
				return;
			}

			// ------------------- Spilling Phase ------------------------

			// loop as long as the thread is marked alive and we do not see the final element
			while (isRunning()) {
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
				if (element == EOF_MARKER) {
					break;
				}

				SortedDataFile<E> output = sortedDataFileFactory.createFile(writeMemory);
				channelDeleteRegistry.registerChannelToBeDelete(output.getWriteChannel().getChannelID());
				channelDeleteRegistry.registerOpenChannel(output.getWriteChannel());

				// write sort-buffer to channel
				if (LOG.isDebugEnabled()) {
					LOG.debug("Spilling buffer " + element.id + ".");
				}

				element.buffer.writeToOutput(output, largeRecordHandler);

				if (LOG.isDebugEnabled()) {
					LOG.debug("Spilled buffer " + element.id + ".");
				}

				output.finishWriting();
				channelDeleteRegistry.unregisterOpenChannel(output.getWriteChannel());

				if (output.getBytesWritten() > 0) {
					spilledFiles.add(new SortedDataFileElement<>(output));
					++numSpilledFiles;
				}

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

			// make sure we have enough memory to merge and for large record handling
			List<MemorySegment> mergeReadMemory = null;
			List<MemorySegment> longRecMem = null;

			if (largeRecordHandler != null && largeRecordHandler.hasData()) {
				if (numSpilledFiles == 0) {
					// only long records
					longRecMem = this.mergeReadMemory;
					mergeReadMemory = new ArrayList<MemorySegment>();
				} else {
					int maxMergedStreams = Math.min(this.mergeFactor, numSpilledFiles);

					int pagesPerStream = Math.max(MIN_NUM_WRITE_BUFFERS,
							Math.min(MAX_NUM_WRITE_BUFFERS, this.mergeReadMemory.size() / 2 / maxMergedStreams));

					int totalMergeReadMemory = maxMergedStreams * pagesPerStream;

					// grab the merge memory
					mergeReadMemory = new ArrayList<MemorySegment>(totalMergeReadMemory);
					for (int i = 0; i < totalMergeReadMemory; i++) {
						mergeReadMemory.add(this.mergeReadMemory.get(i));
					}

					// the remainder of the memory goes to the long record sorter
					longRecMem = new ArrayList<MemorySegment>();
					for (int i = totalMergeReadMemory; i < this.mergeReadMemory.size(); i++) {
						longRecMem.add(this.mergeReadMemory.get(i));
					}
				}

				if (LOG.isDebugEnabled()) {
					LOG.debug("Sorting keys for large records.");
				}
				largeRecords = largeRecordHandler.finishWriteAndSortKeys(longRecMem);
			}
			else {
				mergeReadMemory = this.mergeReadMemory;
			}

			// If not allowed in-memory result, write the large records to disk.
			if (largeRecords != null && !inMemoryResultEnabled) {
				SortedDataFile<E> sortedDataFile = sortedDataFileFactory.createFile(writeMemory);

				if (!objectReuseEnabled) {
					E rec;
					while ((rec = largeRecords.next()) != null) {
						sortedDataFile.writeRecord(rec);
					}
				} else {
					E rec = this.serializer.createInstance();
					while ((rec = largeRecords.next(rec)) != null) {
						sortedDataFile.writeRecord(rec);
					}
				}

				sortedDataFile.finishWriting();

				spilledFiles.add(new SortedDataFileElement<>(sortedDataFile));

				largeRecords = null;
			}

			// spilling finishes, give the sort, spill and largeRecords memory to merging phase
			synchronized (mergeMemoryLock) {
				readMemoryForMerging.addAll(mergeReadMemory);
				writeMemoryForMerging.addAll(writeMemory);
				writeMemory.clear();
				mergeReadMemory.clear();
				// if the large record has been spilled, the memory can be reused by merging phase
				if (longRecMem != null && !inMemoryResultEnabled) {
					mergeReadMemory.addAll(longRecMem);
					longRecMem.clear();
				}
			}

			// notify the merging thread
			spilledFiles.add(mergingMarker());

			// done
			LOG.info("Spilling thread done.");
		}

		/**
		 * Releases the memory that is registered for in-memory sorted run generation.
		 */
		protected final void disposeSortBuffers(boolean releaseMemory) {
			while (!this.queues.empty.isEmpty()) {
				try {
					CircularElement<E> element = this.queues.empty.take();
					element.buffer.dispose();
					if (releaseMemory) {
						this.memManager.release(element.memory);
					}
				}
				catch (InterruptedException iex) {
					if (isRunning()) {
						LOG.error("Spilling thread was interrupted (without being shut down) while collecting empty buffers to release them. " +
								"Retrying to collect buffers...");
					}
					else {
						return;
					}
				}
			}
		}

		protected final CircularElement<E> takeNext(BlockingQueue<CircularElement<E>> queue, Queue<CircularElement<E>> cache)
				throws InterruptedException {
			return cache.isEmpty() ? queue.take() : cache.poll();
		}
	}

	/**
	 * The thread that merges the spilled data files. It merges the channels until sufficiently few channels
	 * remain to perform the final streamed merge.
	 */
	protected class MergingThread extends ThreadBase<E> {

		private final SortedDataFileMerger<E> merger;

		private final MemoryManager memManager;

		/**
		 * The spilled files to be merged. This queue is shared between spilling thread and merging thread,
		 * and the spilling thread will add element to this queue after finishing a new file.
		 */
		private final BlockingQueue<SortedDataFileElement<E>> spilledFiles;

		public MergingThread(SortedDataFileMerger<E> merger, MemoryManager memManager,
							 BlockingQueue<SortedDataFileElement<E>> spilledFiles,
							 ExceptionHandler<IOException> exceptionHandler,
							 AbstractInvokable parentTask) {
			super(exceptionHandler, "SortMerger merging thread", null, parentTask);
			this.merger = merger;
			this.memManager = memManager;
			this.spilledFiles = spilledFiles;
		}

		@Override
		protected void go() throws IOException {
			List<SortedDataFile<E>> sortedDataFiles = new ArrayList<>();
			while (isRunning()) {
				try {
					SortedDataFileElement<E> sortedDataFile = spilledFiles.poll(200, TimeUnit.MILLISECONDS);

					if (sortedDataFile == null) {
						continue;
					}

					List<MemorySegment> writeMemory = new ArrayList<>();
					List<MemorySegment> mergeReadMemory = new ArrayList<>();
					synchronized (mergeMemoryLock) {
						writeMemory.addAll(writeMemoryForMerging);
						mergeReadMemory.addAll(readMemoryForMerging);
					}

					if (sortedDataFile == MERGING_MARKER) {
						sortedDataFiles = merger.finishMerging(
							writeMemory, mergeReadMemory, channelDeleteRegistry, getRunningFlag());
						LOG.info("Finish merging.");
						break;
					} else if (sortedDataFile != null) {
						merger.notifyNewSortedDataFile(
							sortedDataFile.sortedDataFile, writeMemory, mergeReadMemory, channelDeleteRegistry, getRunningFlag());
						LOG.info("Notified new file {}.", sortedDataFile.sortedDataFile.getChannelID().getPath());
					}
				} catch (InterruptedException e) {
					LOG.warn("Interrupted when polling spilled files.", e);
				}
			}

			// from here on, we won't write again
			synchronized (mergeMemoryLock) {
				this.memManager.release(writeMemoryForMerging);
				writeMemoryForMerging.clear();
			}

			if (cacheOnly.get()) {
				// the result has been set
				return;
			}

			// check if we have spilled some data at all
			if (sortedDataFiles.isEmpty()) {
				if (!inMemoryResultEnabled || largeRecords == null) {
					setResult(sortedDataFiles, EmptyMutableObjectIterator.<E>get());
				} else {
					setResult(sortedDataFiles, largeRecords);
				}
			}
			else {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Beginning final merge.");
				}
				// the merge read memory will be released when task finishes
				List<MemorySegment> mergeReadMemory = new ArrayList<>();
				synchronized (mergeMemoryLock) {
					mergeReadMemory.addAll(readMemoryForMerging);
				}
				MutableObjectIterator<E> finalResultIterator =
					merger.getMergingIterator(sortedDataFiles, mergeReadMemory, largeRecords, channelDeleteRegistry);
				setResult(sortedDataFiles, finalResultIterator);
			}

			// done
			LOG.info("Merging thread done.");
		}
	}
}
