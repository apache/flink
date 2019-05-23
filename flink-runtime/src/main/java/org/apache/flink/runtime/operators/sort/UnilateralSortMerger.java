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

import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.ChannelReaderInputViewIterator;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelReader;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelWriter;
import org.apache.flink.runtime.io.disk.iomanager.ChannelReaderInputView;
import org.apache.flink.runtime.io.disk.iomanager.ChannelWriterOutputView;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel.ID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.util.EmptyMutableObjectIterator;
import org.apache.flink.util.MutableObjectIterator;

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
	
	// ------------------------------------------------------------------------
	//                                  Threads
	// ------------------------------------------------------------------------

	/** The thread that reads the input channels into buffers and passes them on to the merger. */
	private final ThreadBase<E> readThread;

	/** The thread that merges the buffer handed from the reading thread. */
	private final ThreadBase<E> sortThread;

	/** The thread that handles spilling to secondary storage. */
	private final ThreadBase<E> spillThread;
	
	// ------------------------------------------------------------------------
	//                                   Memory
	// ------------------------------------------------------------------------
	
	/** The memory segments used first for sorting and later for reading/pre-fetching
	 * during the external merge. */
	protected final List<MemorySegment> sortReadMemory;
	
	/** The memory segments used to stage data to be written. */
	protected final List<MemorySegment> writeMemory;
	
	/** The memory manager through which memory is allocated and released. */
	protected final MemoryManager memoryManager;
	
	// ------------------------------------------------------------------------
	//                            Miscellaneous Fields
	// ------------------------------------------------------------------------
	
	/**
	 * The handler for large records, that do not go though the in-memory sorter as a whole, but
	 * directly go to disk.
	 */
	private final LargeRecordHandler<E> largeRecordHandler;
	
	/**
	 * Collection of all currently open channels, to be closed and deleted during cleanup.
	 */
	private final HashSet<FileIOChannel> openChannels;
	
	/**
	 * Collection of all temporary files created and to be removed when closing the sorter.
	 */
	private final HashSet<FileIOChannel.ID> channelsToDeleteAtShutdown;
	
	/**
	 * The monitor which guards the iterator field.
	 */
	protected final Object iteratorLock = new Object();
	
	/**
	 * The iterator to be returned by the sort-merger. This variable is null, while receiving and merging is still in
	 * progress and it will be set once we have &lt; merge factor sorted sub-streams that will then be streamed sorted.
	 */
	protected volatile MutableObjectIterator<E> iterator;
	
	/**
	 * The exception that is set, if the iterator cannot be created.
	 */
	protected volatile IOException iteratorException;
	
	/**
	 * Flag indicating that the sorter was closed.
	 */
	protected volatile boolean closed;

	/**
	 * Whether to reuse objects during deserialization.
	 */
	protected final boolean objectReuseEnabled;

	private final Collection<InMemorySorter<?>> inMemorySorters;

	// ------------------------------------------------------------------------
	//                         Constructor & Shutdown
	// ------------------------------------------------------------------------

	public UnilateralSortMerger(MemoryManager memoryManager, IOManager ioManager,
			MutableObjectIterator<E> input, AbstractInvokable parentTask,
			TypeSerializerFactory<E> serializerFactory, TypeComparator<E> comparator,
			double memoryFraction, int maxNumFileHandles, float startSpillingFraction,
			boolean handleLargeRecords, boolean objectReuseEnabled)
	throws IOException, MemoryAllocationException
	{
		this(memoryManager, ioManager, input, parentTask, serializerFactory, comparator,
			memoryFraction, -1, maxNumFileHandles, startSpillingFraction, handleLargeRecords, objectReuseEnabled);
	}

	public UnilateralSortMerger(MemoryManager memoryManager, IOManager ioManager,
			MutableObjectIterator<E> input, AbstractInvokable parentTask,
			TypeSerializerFactory<E> serializerFactory, TypeComparator<E> comparator,
			double memoryFraction, int numSortBuffers, int maxNumFileHandles,
			float startSpillingFraction, boolean handleLargeRecords, boolean objectReuseEnabled)
	throws IOException, MemoryAllocationException
	{
		this(memoryManager, ioManager, input, parentTask, serializerFactory, comparator,
			memoryFraction, numSortBuffers, maxNumFileHandles, startSpillingFraction, false, handleLargeRecords,
			objectReuseEnabled);
	}

	public UnilateralSortMerger(MemoryManager memoryManager, List<MemorySegment> memory,
			IOManager ioManager,
			MutableObjectIterator<E> input, AbstractInvokable parentTask, 
			TypeSerializerFactory<E> serializerFactory, TypeComparator<E> comparator,
			int numSortBuffers, int maxNumFileHandles,
			float startSpillingFraction, boolean handleLargeRecords, boolean objectReuseEnabled)
	throws IOException
	{
		this(memoryManager, memory, ioManager, input, parentTask, serializerFactory, comparator,
			numSortBuffers, maxNumFileHandles, startSpillingFraction, false, handleLargeRecords,
			objectReuseEnabled);
	}
	
	protected UnilateralSortMerger(MemoryManager memoryManager,
			IOManager ioManager,
			MutableObjectIterator<E> input, AbstractInvokable parentTask, 
			TypeSerializerFactory<E> serializerFactory, TypeComparator<E> comparator,
			double memoryFraction, int numSortBuffers, int maxNumFileHandles,
			float startSpillingFraction, boolean noSpillingMemory, boolean handleLargeRecords,
			boolean objectReuseEnabled)
	throws IOException, MemoryAllocationException
	{
		this(memoryManager, memoryManager.allocatePages(parentTask, memoryManager.computeNumberOfPages(memoryFraction)),
				ioManager, input, parentTask, serializerFactory, comparator,
				numSortBuffers, maxNumFileHandles, startSpillingFraction, noSpillingMemory, handleLargeRecords,
				objectReuseEnabled);
	}
	
	protected UnilateralSortMerger(MemoryManager memoryManager, List<MemorySegment> memory,
			IOManager ioManager,
			MutableObjectIterator<E> input, AbstractInvokable parentTask, 
			TypeSerializerFactory<E> serializerFactory, TypeComparator<E> comparator,
			int numSortBuffers, int maxNumFileHandles,
			float startSpillingFraction, boolean noSpillingMemory, boolean handleLargeRecords,
			boolean objectReuseEnabled) throws IOException {
		this (
			memoryManager,
			memory,
			ioManager,
			input,
			parentTask,
			serializerFactory,
			comparator,
			numSortBuffers,
			maxNumFileHandles,
			startSpillingFraction,
			noSpillingMemory,
			handleLargeRecords,
			objectReuseEnabled,
			new DefaultInMemorySorterFactory<>(serializerFactory, comparator, THRESHOLD_FOR_IN_PLACE_SORTING));
	}

	protected UnilateralSortMerger(
			MemoryManager memoryManager,
			List<MemorySegment> memory,
			IOManager ioManager,
			MutableObjectIterator<E> input,
			AbstractInvokable parentTask,
			TypeSerializerFactory<E> serializerFactory,
			TypeComparator<E> comparator,
			int numSortBuffers,
			int maxNumFileHandles,
			float startSpillingFraction,
			boolean noSpillingMemory,
			boolean handleLargeRecords,
			boolean objectReuseEnabled,
			InMemorySorterFactory<E> inMemorySorterFactory) throws IOException {
		// sanity checks
		if (memoryManager == null || (ioManager == null && !noSpillingMemory) || serializerFactory == null || comparator == null) {
			throw new NullPointerException();
		}
		if (parentTask == null) {
			throw new NullPointerException("Parent Task must not be null.");
		}
		if (maxNumFileHandles < 2) {
			throw new IllegalArgumentException("Merger cannot work with less than two file handles.");
		}
		
		this.memoryManager = memoryManager;
		this.objectReuseEnabled = objectReuseEnabled;

		// adjust the memory quotas to the page size
		final int numPagesTotal = memory.size();

		if (numPagesTotal < MIN_NUM_WRITE_BUFFERS + MIN_NUM_SORT_MEM_SEGMENTS) {
			throw new IllegalArgumentException("Too little memory provided to sorter to perform task. " +
				"Required are at least " + (MIN_NUM_WRITE_BUFFERS + MIN_NUM_SORT_MEM_SEGMENTS) + 
				" pages. Current page size is " + memoryManager.getPageSize() + " bytes.");
		}
		
		// determine how many buffers to use for writing
		final int numWriteBuffers;
		final int numLargeRecordBuffers;
		
		if (noSpillingMemory && !handleLargeRecords) {
			numWriteBuffers = 0;
			numLargeRecordBuffers = 0;
		}
		else {
			int numConsumers = (noSpillingMemory ? 0 : 1) + (handleLargeRecords ? 2 : 0);
			
			// determine how many buffers we have when we do a full mere with maximal fan-in 
			final int minBuffersForMerging = maxNumFileHandles + numConsumers * MIN_NUM_WRITE_BUFFERS;

			if (minBuffersForMerging > numPagesTotal) {
				numWriteBuffers = noSpillingMemory ? 0 : MIN_NUM_WRITE_BUFFERS;
				numLargeRecordBuffers = handleLargeRecords ? 2*MIN_NUM_WRITE_BUFFERS : 0;

				maxNumFileHandles = numPagesTotal - numConsumers * MIN_NUM_WRITE_BUFFERS;
				if (LOG.isDebugEnabled()) {
					LOG.debug("Reducing maximal merge fan-in to " + maxNumFileHandles + " due to limited memory availability during merge");
				}
			}
			else {
				// we are free to choose. make sure that we do not eat up too much memory for writing
				final int fractionalAuxBuffers = numPagesTotal / (numConsumers * 100);
				
				if (fractionalAuxBuffers >= MAX_NUM_WRITE_BUFFERS) {
					numWriteBuffers = noSpillingMemory ? 0 : MAX_NUM_WRITE_BUFFERS;
					numLargeRecordBuffers = handleLargeRecords ? 2*MAX_NUM_WRITE_BUFFERS : 0;
				}
				else {
					numWriteBuffers = noSpillingMemory ? 0 :
							Math.max(MIN_NUM_WRITE_BUFFERS, fractionalAuxBuffers);	// at least the lower bound
					
					numLargeRecordBuffers = handleLargeRecords ? 
							Math.max(2*MIN_NUM_WRITE_BUFFERS, fractionalAuxBuffers) // at least the lower bound
							: 0;
				}
			}
		}
		
		final int sortMemPages = numPagesTotal - numWriteBuffers - numLargeRecordBuffers;
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
		this.writeMemory = new ArrayList<MemorySegment>(numWriteBuffers);
		
		final TypeSerializer<E> serializer = serializerFactory.getSerializer();
		
		// move some pages from the sort memory to the write memory
		if (numWriteBuffers > 0) {
			for (int i = 0; i < numWriteBuffers; i++) {
				this.writeMemory.add(this.sortReadMemory.remove(this.sortReadMemory.size() - 1));
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
		final CircularQueues<E> circularQueues = new CircularQueues<E>();

		inMemorySorters = new ArrayList<>(numSortBuffers);
		
		// allocate the sort buffers and fill empty queue with them
		final Iterator<MemorySegment> segments = this.sortReadMemory.iterator();
		for (int i = 0; i < numSortBuffers; i++)
		{
			// grab some memory
			final List<MemorySegment> sortSegments = new ArrayList<MemorySegment>(numSegmentsPerSortBuffer);
			for (int k = (i == numSortBuffers - 1 ? Integer.MAX_VALUE : numSegmentsPerSortBuffer); k > 0 && segments.hasNext(); k--) {
				sortSegments.add(segments.next());
			}
			
			final InMemorySorter<E> inMemorySorter = inMemorySorterFactory.create(sortSegments);
			inMemorySorters.add(inMemorySorter);

			// add to empty queue
			CircularElement<E> element = new CircularElement<E>(i, inMemorySorter, sortSegments);
			circularQueues.empty.add(element);
		}

		// exception handling
		ExceptionHandler<IOException> exceptionHandler = new ExceptionHandler<IOException>() {
			public void handleException(IOException exception) {
				// forward exception
				if (!closed) {
					setResultIteratorException(exception);
					close();
				}
			}
		};
		
		// create sets that track the channels we need to clean up when closing the sorter
		this.channelsToDeleteAtShutdown = new HashSet<FileIOChannel.ID>(64);
		this.openChannels = new HashSet<FileIOChannel>(64);

		// start the thread that reads the input channels
		this.readThread = getReadingThread(exceptionHandler, input, circularQueues, largeRecordHandler,
				parentTask, serializer, ((long) (startSpillingFraction * sortMemory)));

		// start the thread that sorts the buffers
		this.sortThread = getSortingThread(exceptionHandler, circularQueues, parentTask);

		// start the thread that handles spilling to secondary storage
		this.spillThread = getSpillingThread(exceptionHandler, circularQueues, parentTask, 
				memoryManager, ioManager, serializerFactory, comparator, this.sortReadMemory, this.writeMemory, 
				maxNumFileHandles);
		
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
				if (this.iteratorException == null) {
					this.iteratorException = new IOException("The sorter has been closed.");
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
			}
			catch (InterruptedException iex) {
				LOG.debug("Closing of sort/merger was interrupted. " +
						"The reading/sorting/spilling threads may still be working.", iex);
			}
		}
		finally {

			// Dispose all in memory sorter in order to clear memory references
			for (InMemorySorter<?> inMemorySorter : inMemorySorters) {
				inMemorySorter.dispose();
			}

			// RELEASE ALL MEMORY. If the threads and channels are still running, this should cause
			// exceptions, because their memory segments are freed
			try {
				if (!this.writeMemory.isEmpty()) {
					this.memoryManager.release(this.writeMemory);
				}
				this.writeMemory.clear();
			}
			catch (Throwable t) {}
			
			try {
				if (!this.sortReadMemory.isEmpty()) {
					this.memoryManager.release(this.sortReadMemory);
				}
				this.sortReadMemory.clear();
			}
			catch (Throwable t) {}
			
			// we have to loop this, because it may fail with a concurrent modification exception
			while (!this.openChannels.isEmpty()) {
				try {
					for (Iterator<FileIOChannel> channels = this.openChannels.iterator(); channels.hasNext(); ) {
						final FileIOChannel channel = channels.next();
						channels.remove();
						channel.closeAndDelete();
					}
				}
				catch (Throwable t) {}
			}
			
			// we have to loop this, because it may fail with a concurrent modification exception
			while (!this.channelsToDeleteAtShutdown.isEmpty()) {
				try {
					for (Iterator<FileIOChannel.ID> channels = this.channelsToDeleteAtShutdown.iterator(); channels.hasNext(); ) {
						final FileIOChannel.ID channel = channels.next();
						channels.remove();
						try {
							final File f = new File(channel.getPath());
							if (f.exists()) {
								f.delete();
							}
						} catch (Throwable t) {}
					}
				}
				catch (Throwable t) {}
			}
			
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
			TypeSerializer<E> serializer, long startSpillingBytes)
	{
		return new ReadingThread<E>(exceptionHandler, reader, queues, largeRecordHandler, 
				serializer.createInstance(),parentTask, startSpillingBytes);
	}

	protected ThreadBase<E> getSortingThread(ExceptionHandler<IOException> exceptionHandler, CircularQueues<E> queues,
			AbstractInvokable parentTask)
	{
		return new SortingThread<E>(exceptionHandler, queues, parentTask);
	}


	protected ThreadBase<E> getSpillingThread(ExceptionHandler<IOException> exceptionHandler, CircularQueues<E> queues,
			AbstractInvokable parentTask, MemoryManager memoryManager, IOManager ioManager, 
			TypeSerializerFactory<E> serializerFactory, TypeComparator<E> comparator,
			List<MemorySegment> sortReadMemory, List<MemorySegment> writeMemory, int maxFileHandles)
	{
		return new SpillingThread(exceptionHandler, queues, parentTask,
			memoryManager, ioManager, serializerFactory.getSerializer(), comparator, sortReadMemory, writeMemory, maxFileHandles);
	}

	// ------------------------------------------------------------------------
	//                           Result Iterator
	// ------------------------------------------------------------------------


	@Override
	public MutableObjectIterator<E> getIterator() throws InterruptedException {
		synchronized (this.iteratorLock) {
			// wait while both the iterator and the exception are not set
			while (this.iterator == null && this.iteratorException == null) {
				this.iteratorLock.wait();
			}
			
			if (this.iteratorException != null) {
				throw new RuntimeException("Error obtaining the sorted input: " + this.iteratorException.getMessage(),
					this.iteratorException);
			}
			else {
				return this.iterator;
			}
		}
	}
	
	/**
	 * Sets the result iterator. By setting the result iterator, all threads that are waiting for the result
	 * iterator are notified and will obtain it.
	 * 
	 * @param iterator The result iterator to set.
	 */
	protected final void setResultIterator(MutableObjectIterator<E> iterator) {
		synchronized (this.iteratorLock) {
			// set the result iterator only, if no exception has occurred
			if (this.iteratorException == null) {
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
	protected final void setResultIteratorException(IOException ioex) {
		synchronized (this.iteratorLock) {
			if (this.iteratorException == null) {
				this.iteratorException = ioex;
				this.iteratorLock.notifyAll();
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
		private volatile boolean alive;

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
			this.alive = true;
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
			return this.alive;
		}

		/**
		 * Forces an immediate shutdown of the thread. Looses any state and all buffers that the thread is currently
		 * working on. This terminates cleanly for the JVM, but looses intermediate results.
		 */
		public void shutdown() {
			this.alive = false;
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
			LOG.debug("Reading thread done.");
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
					if (LOG.isDebugEnabled()) {
						LOG.debug("Sorting thread done.");
					}
					alive = false;
				}
				this.queues.spill.add(element);
			}
		}
	}

	/**
	 * The thread that handles the spilling of intermediate results and sets up the merging. It also merges the 
	 * channels until sufficiently few channels remain to perform the final streamed merge. 
	 */
	protected class SpillingThread extends ThreadBase<E> {
		
		protected final MemoryManager memManager;			// memory manager to release memory
		
		protected final IOManager ioManager;				// I/O manager to create channels
		
		protected final TypeSerializer<E> serializer;		// The serializer for the data type
		
		protected final TypeComparator<E> comparator;		// The comparator that establishes the order relation.
		
		protected final List<MemorySegment> writeMemory;	// memory segments for writing
		
		protected final List<MemorySegment> mergeReadMemory;	// memory segments for sorting/reading
		
		protected final int maxFanIn;
		
		protected final int numWriteBuffersToCluster;
		
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
		public SpillingThread(ExceptionHandler<IOException> exceptionHandler, CircularQueues<E> queues,
				AbstractInvokable parentTask, MemoryManager memManager, IOManager ioManager, 
				TypeSerializer<E> serializer, TypeComparator<E> comparator, 
				List<MemorySegment> sortReadMemory, List<MemorySegment> writeMemory, int maxNumFileHandles)
		{
			super(exceptionHandler, "SortMerger spilling thread", queues, parentTask);
			this.memManager = memManager;
			this.ioManager = ioManager;
			this.serializer = serializer;
			this.comparator = comparator;
			this.mergeReadMemory = sortReadMemory;
			this.writeMemory = writeMemory;
			this.maxFanIn = maxNumFileHandles;
			this.numWriteBuffersToCluster = writeMemory.size() >= 4 ? writeMemory.size() / 2 : 1;
		}

		/**
		 * Entry point of the thread.
		 */
		public void go() throws IOException {
			
			final Queue<CircularElement<E>> cache = new ArrayDeque<CircularElement<E>>();
			CircularElement<E> element;
			boolean cacheOnly = false;
			
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
					cacheOnly = true;
					break;
				}
				cache.add(element);
			}
			
			// check whether the thread was canceled
			if (!isRunning()) {
				return;
			}
			
			MutableObjectIterator<E> largeRecords = null;
			
			// check if we can stay in memory with the large record handler
			if (cacheOnly && largeRecordHandler != null && largeRecordHandler.hasData()) {
				List<MemorySegment> memoryForLargeRecordSorting = new ArrayList<MemorySegment>();
				
				CircularElement<E> circElement;
				while ((circElement = this.queues.empty.poll()) != null) {
					circElement.buffer.dispose();
					memoryForLargeRecordSorting.addAll(circElement.memory);
				}
				
				if (memoryForLargeRecordSorting.isEmpty()) {
					cacheOnly = false;
					LOG.debug("Going to disk-based merge because of large records.");
					
				} else {
					LOG.debug("Sorting large records, to add them to in-memory merge.");
					largeRecords = largeRecordHandler.finishWriteAndSortKeys(memoryForLargeRecordSorting);
				}
			}
			
			// ------------------- In-Memory Merge ------------------------
			if (cacheOnly) {
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
				setResultIterator(iterators.isEmpty() ? EmptyMutableObjectIterator.<E>get() :
						iterators.size() == 1 ? iterators.get(0) : 
						new MergeIterator<E>(iterators, this.comparator));
				return;
			}
			
			// ------------------- Spilling Phase ------------------------
			
			final FileIOChannel.Enumerator enumerator = this.ioManager.createChannelEnumerator();
			List<ChannelWithBlockCount> channelIDs = new ArrayList<ChannelWithBlockCount>();
			
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
				
				// open next channel
				FileIOChannel.ID channel = enumerator.next();
				registerChannelToBeRemovedAtShudown(channel);

				// create writer
				final BlockChannelWriter<MemorySegment> writer = this.ioManager.createBlockChannelWriter(channel);
				registerOpenChannelToBeRemovedAtShudown(writer);
				final ChannelWriterOutputView output = new ChannelWriterOutputView(writer, this.writeMemory,
																			this.memManager.getPageSize());

				// write sort-buffer to channel
				if (LOG.isDebugEnabled()) {
					LOG.debug("Spilling buffer " + element.id + ".");
				}
				
				element.buffer.writeToOutput(output, largeRecordHandler);
				
				if (LOG.isDebugEnabled()) {
					LOG.debug("Spilled buffer " + element.id + ".");
				}

				output.close();
				unregisterOpenChannelToBeRemovedAtShudown(writer);
				
				if (output.getBytesWritten() > 0) {
					channelIDs.add(new ChannelWithBlockCount(channel, output.getBlockCount()));
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

			
			// ------------------- Merging Phase ------------------------
			
			// make sure we have enough memory to merge and for large record handling
			List<MemorySegment> mergeReadMemory;
			
			if (largeRecordHandler != null && largeRecordHandler.hasData()) {
				
				List<MemorySegment> longRecMem;
				if (channelIDs.isEmpty()) {
					// only long records
					longRecMem = this.mergeReadMemory;
					mergeReadMemory = Collections.emptyList();
				}
				else {
					int maxMergedStreams = Math.min(this.maxFanIn, channelIDs.size());
					
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
			
			// merge channels until sufficient file handles are available
			while (isRunning() && channelIDs.size() > this.maxFanIn) {
				channelIDs = mergeChannelList(channelIDs, mergeReadMemory, this.writeMemory);
			}
			
			// from here on, we won't write again
			this.memManager.release(this.writeMemory);
			this.writeMemory.clear();
			
			// check if we have spilled some data at all
			if (channelIDs.isEmpty()) {
				if (largeRecords == null) {
					setResultIterator(EmptyMutableObjectIterator.<E>get());
				} else {
					setResultIterator(largeRecords);
				}
			}
			else {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Beginning final merge.");
				}
				
				// allocate the memory for the final merging step
				List<List<MemorySegment>> readBuffers = new ArrayList<List<MemorySegment>>(channelIDs.size());
				
				// allocate the read memory and register it to be released
				getSegmentsForReaders(readBuffers, mergeReadMemory, channelIDs.size());
				
				// get the readers and register them to be released
				setResultIterator(getMergingIterator(channelIDs, readBuffers, new ArrayList<FileIOChannel>(channelIDs.size()), largeRecords));
			}

			// done
			if (LOG.isDebugEnabled()) {
				LOG.debug("Spilling and merging thread done.");
			}
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
		
		// ------------------------------------------------------------------------
		//                             Result Merging
		// ------------------------------------------------------------------------
		
		/**
		 * Returns an iterator that iterates over the merged result from all given channels.
		 * 
		 * @param channelIDs The channels that are to be merged and returned.
		 * @param inputSegments The buffers to be used for reading. The list contains for each channel one
		 *                      list of input segments. The size of the <code>inputSegments</code> list must be equal to
		 *                      that of the <code>channelIDs</code> list.
		 * @return An iterator over the merged records of the input channels.
		 * @throws IOException Thrown, if the readers encounter an I/O problem.
		 */
		protected final MergeIterator<E> getMergingIterator(final List<ChannelWithBlockCount> channelIDs,
				final List<List<MemorySegment>> inputSegments, List<FileIOChannel> readerList, MutableObjectIterator<E> largeRecords)
			throws IOException
		{
			// create one iterator per channel id
			if (LOG.isDebugEnabled()) {
				LOG.debug("Performing merge of " + channelIDs.size() + " sorted streams.");
			}
			
			final List<MutableObjectIterator<E>> iterators = new ArrayList<MutableObjectIterator<E>>(channelIDs.size() + 1);
			
			for (int i = 0; i < channelIDs.size(); i++) {
				final ChannelWithBlockCount channel = channelIDs.get(i);
				final List<MemorySegment> segsForChannel = inputSegments.get(i);
				
				// create a reader. if there are multiple segments for the reader, issue multiple together per I/O request
				final BlockChannelReader<MemorySegment> reader = this.ioManager.createBlockChannelReader(channel.getChannel());
					
				readerList.add(reader);
				registerOpenChannelToBeRemovedAtShudown(reader);
				unregisterChannelToBeRemovedAtShudown(channel.getChannel());
				
				// wrap channel reader as a view, to get block spanning record deserialization
				final ChannelReaderInputView inView = new ChannelReaderInputView(reader, segsForChannel, 
																			channel.getBlockCount(), false);
				iterators.add(new ChannelReaderInputViewIterator<E>(inView, null, this.serializer));
			}
			
			if (largeRecords != null) {
				iterators.add(largeRecords);
			}

			return new MergeIterator<E>(iterators, this.comparator);
		}

		/**
		 * Merges the given sorted runs to a smaller number of sorted runs.
		 *
		 * @param channelIDs The IDs of the sorted runs that need to be merged.
		 * @param allReadBuffers
		 * @param writeBuffers The buffers to be used by the writers.
		 * @return A list of the IDs of the merged channels.
		 * @throws IOException Thrown, if the readers or writers encountered an I/O problem.
		 */
		protected final List<ChannelWithBlockCount> mergeChannelList(final List<ChannelWithBlockCount> channelIDs,
					final List<MemorySegment> allReadBuffers, final List<MemorySegment> writeBuffers)
		throws IOException
		{
			// A channel list with length maxFanIn<sup>i</sup> can be merged to maxFanIn files in i-1 rounds where every merge
			// is a full merge with maxFanIn input channels. A partial round includes merges with fewer than maxFanIn
			// inputs. It is most efficient to perform the partial round first.
			final double scale = Math.ceil(Math.log(channelIDs.size()) / Math.log(this.maxFanIn)) - 1;

			final int numStart = channelIDs.size();
			final int numEnd = (int) Math.pow(this.maxFanIn, scale);

			final int numMerges = (int) Math.ceil((numStart - numEnd) / (double) (this.maxFanIn - 1));

			final int numNotMerged = numEnd - numMerges;
			final int numToMerge = numStart - numNotMerged;

			// unmerged channel IDs are copied directly to the result list
			final List<ChannelWithBlockCount> mergedChannelIDs = new ArrayList<ChannelWithBlockCount>(numEnd);
			mergedChannelIDs.addAll(channelIDs.subList(0, numNotMerged));

			final int channelsToMergePerStep = (int) Math.ceil(numToMerge / (double) numMerges);

			// allocate the memory for the merging step
			final List<List<MemorySegment>> readBuffers = new ArrayList<List<MemorySegment>>(channelsToMergePerStep);
			getSegmentsForReaders(readBuffers, allReadBuffers, channelsToMergePerStep);

			final List<ChannelWithBlockCount> channelsToMergeThisStep = new ArrayList<ChannelWithBlockCount>(channelsToMergePerStep);
			int channelNum = numNotMerged;
			while (isRunning() && channelNum < channelIDs.size()) {
				channelsToMergeThisStep.clear();

				for (int i = 0; i < channelsToMergePerStep && channelNum < channelIDs.size(); i++, channelNum++) {
					channelsToMergeThisStep.add(channelIDs.get(channelNum));
				}

				mergedChannelIDs.add(mergeChannels(channelsToMergeThisStep, readBuffers, writeBuffers));
			}

			return mergedChannelIDs;
		}

		/**
		 * Merges the sorted runs described by the given Channel IDs into a single sorted run. The merging process
		 * uses the given read and write buffers.
		 * 
		 * @param channelIDs The IDs of the runs' channels.
		 * @param readBuffers The buffers for the readers that read the sorted runs.
		 * @param writeBuffers The buffers for the writer that writes the merged channel.
		 * @return The ID and number of blocks of the channel that describes the merged run.
		 */
		protected ChannelWithBlockCount mergeChannels(List<ChannelWithBlockCount> channelIDs, List<List<MemorySegment>> readBuffers,
				List<MemorySegment> writeBuffers)
		throws IOException
		{
			// the list with the readers, to be closed at shutdown
			final List<FileIOChannel> channelAccesses = new ArrayList<FileIOChannel>(channelIDs.size());

			// the list with the target iterators
			final MergeIterator<E> mergeIterator = getMergingIterator(channelIDs, readBuffers, channelAccesses, null);

			// create a new channel writer
			final FileIOChannel.ID mergedChannelID = this.ioManager.createChannel();
			registerChannelToBeRemovedAtShudown(mergedChannelID);
			final BlockChannelWriter<MemorySegment> writer = this.ioManager.createBlockChannelWriter(mergedChannelID);
			registerOpenChannelToBeRemovedAtShudown(writer);
			final ChannelWriterOutputView output = new ChannelWriterOutputView(writer, writeBuffers, 
																			this.memManager.getPageSize());

			// read the merged stream and write the data back
			if (objectReuseEnabled) {
				final TypeSerializer<E> serializer = this.serializer;
				E rec = serializer.createInstance();
				while ((rec = mergeIterator.next(rec)) != null) {
					serializer.serialize(rec, output);
				}
			} else {
				E rec;
				while ((rec = mergeIterator.next()) != null) {
					serializer.serialize(rec, output);
				}
			}
			output.close();
			final int numBlocksWritten = output.getBlockCount();
			
			// register merged result to be removed at shutdown
			unregisterOpenChannelToBeRemovedAtShudown(writer);
			
			// remove the merged channel readers from the clear-at-shutdown list
			for (int i = 0; i < channelAccesses.size(); i++) {
				FileIOChannel access = channelAccesses.get(i);
				access.closeAndDelete();
				unregisterOpenChannelToBeRemovedAtShudown(access);
			}

			return new ChannelWithBlockCount(mergedChannelID, numBlocksWritten);
		}
		
		/**
		 * Divides the given collection of memory buffers among {@code numChannels} sublists.
		 * 
		 * @param target The list into which the lists with buffers for the channels are put.
		 * @param memory A list containing the memory buffers to be distributed. The buffers are not
		 *               removed from this list.
		 * @param numChannels The number of channels for which to allocate buffers. Must not be zero.
		 */
		protected final void getSegmentsForReaders(List<List<MemorySegment>> target,
			List<MemorySegment> memory, int numChannels)
		{
			// determine the memory to use per channel and the number of buffers
			final int numBuffers = memory.size();
			final int buffersPerChannelLowerBound = numBuffers / numChannels;
			final int numChannelsWithOneMore = numBuffers % numChannels;
			
			final Iterator<MemorySegment> segments = memory.iterator();
			
			// collect memory for the channels that get one segment more
			for (int i = 0; i < numChannelsWithOneMore; i++) {
				final ArrayList<MemorySegment> segs = new ArrayList<MemorySegment>(buffersPerChannelLowerBound + 1);
				target.add(segs);
				for (int k = buffersPerChannelLowerBound; k >= 0; k--) {
					segs.add(segments.next());
				}
			}
			
			// collect memory for the remaining channels
			for (int i = numChannelsWithOneMore; i < numChannels; i++) {
				final ArrayList<MemorySegment> segs = new ArrayList<MemorySegment>(buffersPerChannelLowerBound);
				target.add(segs);
				for (int k = buffersPerChannelLowerBound; k > 0; k--) {
					segs.add(segments.next());
				}
			}
		}
		
		// ------------------------------------------------------------------------
		//              Cleanup of Temp Files and Allocated Memory
		// ------------------------------------------------------------------------
		
		/**
		 * Adds a channel to the list of channels that are to be removed at shutdown.
		 * 
		 * @param channel The channel id.
		 */
		protected void registerChannelToBeRemovedAtShudown(FileIOChannel.ID channel) {
			UnilateralSortMerger.this.channelsToDeleteAtShutdown.add(channel);
		}

		/**
		 * Removes a channel from the list of channels that are to be removed at shutdown.
		 * 
		 * @param channel The channel id.
		 */
		protected void unregisterChannelToBeRemovedAtShudown(FileIOChannel.ID channel) {
			UnilateralSortMerger.this.channelsToDeleteAtShutdown.remove(channel);
		}
		
		/**
		 * Adds a channel reader/writer to the list of channels that are to be removed at shutdown.
		 * 
		 * @param channel The channel reader/writer.
		 */
		protected void registerOpenChannelToBeRemovedAtShudown(FileIOChannel channel) {
			UnilateralSortMerger.this.openChannels.add(channel);
		}

		/**
		 * Removes a channel reader/writer from the list of channels that are to be removed at shutdown.
		 * 
		 * @param channel The channel reader/writer.
		 */
		protected void unregisterOpenChannelToBeRemovedAtShudown(FileIOChannel channel) {
			UnilateralSortMerger.this.openChannels.remove(channel);
		}
	}
	
	protected static final class ChannelWithBlockCount {
		
		private final FileIOChannel.ID channel;
		private final int blockCount;
		
		public ChannelWithBlockCount(ID channel, int blockCount) {
			this.channel = channel;
			this.blockCount = blockCount;
		}

		public FileIOChannel.ID getChannel() {
			return channel;
		}

		public int getBlockCount() {
			return blockCount;
		}
	}
}
