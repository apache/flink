/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.sort;

import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.services.iomanager.BlockChannelAccess;
import eu.stratosphere.nephele.services.iomanager.BlockChannelReader;
import eu.stratosphere.nephele.services.iomanager.BlockChannelWriter;
import eu.stratosphere.nephele.services.iomanager.Channel;
import eu.stratosphere.nephele.services.iomanager.ChannelReaderInputView;
import eu.stratosphere.nephele.services.iomanager.ChannelWriterOutputView;
import eu.stratosphere.nephele.services.iomanager.Channel.ID;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.common.generic.types.TypeComparator;
import eu.stratosphere.pact.common.generic.types.TypeSerializer;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.io.ChannelReaderInputViewIterator;
import eu.stratosphere.pact.runtime.util.EmptyMutableObjectIterator;
import eu.stratosphere.pact.runtime.util.MathUtils;

/**
 * The {@link UnilateralSortMerger} is a full fledged sorter. It implements a multi-way merge sort. Internally, 
 * the logic is factored into three threads (read, sort, spill) which communicate through a set of blocking queues,
 * forming a closed loop.  Memory is allocated using the {@link MemoryManager} interface. Thus the component will
 * not exceed the provided memory limits.
 * 
 * @author Stephan Ewen
 * @author Erik Nijkamp
 */
public class UnilateralSortMerger<E> implements Sorter<E>
{	
	// ------------------------------------------------------------------------
	//                              Constants
	// ------------------------------------------------------------------------

	/**
	 * Logging.
	 */
	private static final Log LOG = LogFactory.getLog(UnilateralSortMerger.class);
	
	/**
	 * The minimal number of buffers to use by the writers.
	 */
	protected static final int MIN_NUM_WRITE_BUFFERS = 2;
	
	/**
	 * The maximal number of buffers to use by the writers.
	 */
	protected static final int MAX_NUM_WRITE_BUFFERS = 64;
	
	/**
	 * The minimum number of segments that are required for the sort to operate.
	 */
	protected static final int MIN_NUM_SORT_MEM_SEGMENTS = 32;

	// ------------------------------------------------------------------------
	//                                  Threads
	// ------------------------------------------------------------------------

	/**
	 * The thread that reads the input channels into buffers and passes them on to the merger.
	 */
	private final ThreadBase<E> readThread;

	/**
	 * The thread that merges the buffer handed from the reading thread.
	 */
	private final ThreadBase<E> sortThread;

	/**
	 * The thread that handles spilling to secondary storage.
	 */
	private final ThreadBase<E> spillThread;
	
	// ------------------------------------------------------------------------
	//                                   Memory
	// ------------------------------------------------------------------------
	
	/**
	 * The memory segments used first for sorting and later for reading/pre-fetching
	 * during the external merge.
	 */
	protected final ArrayList<MemorySegment> sortReadMemory;
	
	/**
	 * The memory segments used to stage data to be written.
	 */
	protected final ArrayList<MemorySegment> writeMemory;
	
	/**
	 * The memory manager through which memory is allocated and released.
	 */
	protected final MemoryManager memoryManager;
	
	// ------------------------------------------------------------------------
	//                            Miscellaneous Fields
	// ------------------------------------------------------------------------
	
	/**
	 * Collection of all currently open channels, to be closed and deleted during cleanup.
	 */
	private final HashSet<BlockChannelAccess<?, ?>> openChannels;
	
	/**
	 * Collection of all temporary files created and to be removed when closing the sorter.
	 */
	private final HashSet<Channel.ID> channelsToDeleteAtShutdown;
	
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

	// ------------------------------------------------------------------------
	//                         Constructor & Shutdown
	// ------------------------------------------------------------------------

	/**
	 * Creates a new sorter that reads the data from a given reader and provides an iterator returning that
	 * data in a sorted manner. The memory is divided among sort buffers, write buffers and read buffers
	 * automatically.
	 * 
	 * @param memoryManager The memory manager from which to allocate the memory.
	 * @param ioManager The I/O manager, which is used to write temporary files to disk.
	 * @param input The input that is sorted by this sorter.
	 * @param parentTask The parent task, which owns all resources used by this sorter.
	 * @param serializer The type serializer.
	 * @param comparator The type comparator establishing the order relation.
	 * @param totalMemory The total amount of memory dedicated to sorting, merging and I/O.
	 * @param maxNumFileHandles The maximum number of files to be merged at once.
	 * @param startSpillingFraction The faction of the buffers that have to be filled before the spilling thread
	 *                              actually begins spilling data to disk.
	 * 
	 * @throws IOException Thrown, if an error occurs initializing the resources for external sorting.
	 * @throws MemoryAllocationException Thrown, if not enough memory can be obtained from the memory manager to
	 *                                   perform the sort.
	 */
	public UnilateralSortMerger(MemoryManager memoryManager, IOManager ioManager,
			MutableObjectIterator<E> input, AbstractInvokable parentTask, 
			TypeSerializer<E> serializer, TypeComparator<E> comparator,
			long totalMemory, int maxNumFileHandles, float startSpillingFraction)
	throws IOException, MemoryAllocationException
	{
		this(memoryManager, ioManager, input, parentTask, serializer, comparator,
			totalMemory, -1, maxNumFileHandles, startSpillingFraction);
	}
	
	/**
	 * Creates a new sorter that reads the data from a given reader and provides an iterator returning that
	 * data in a sorted manner. The memory is divided among sort buffers, write buffers and read buffers
	 * automatically.
	 * 
	 * @param memoryManager The memory manager from which to allocate the memory.
	 * @param ioManager The I/O manager, which is used to write temporary files to disk.
	 * @param input The input that is sorted by this sorter.
	 * @param parentTask The parent task, which owns all resources used by this sorter.
	 * @param serializer The type serializer.
	 * @param comparator The type comparator establishing the order relation.
	 * @param totalMemory The total amount of memory dedicated to sorting, merging and I/O.
	 * @param numSortBuffers The number of distinct buffers to use creation of the initial runs.
	 * @param maxNumFileHandles The maximum number of files to be merged at once.
	 * @param startSpillingFraction The faction of the buffers that have to be filled before the spilling thread
	 *                              actually begins spilling data to disk.
	 * 
	 * @throws IOException Thrown, if an error occurs initializing the resources for external sorting.
	 * @throws MemoryAllocationException Thrown, if not enough memory can be obtained from the memory manager to
	 *                                   perform the sort.
	 */
	public UnilateralSortMerger(MemoryManager memoryManager, IOManager ioManager,
			MutableObjectIterator<E> input, AbstractInvokable parentTask, 
			TypeSerializer<E> serializer, TypeComparator<E> comparator,
			long totalMemory, int numSortBuffers, int maxNumFileHandles, 
			float startSpillingFraction)
	throws IOException, MemoryAllocationException
	{
		this(memoryManager, ioManager, input, parentTask, serializer, comparator,
			totalMemory, numSortBuffers, maxNumFileHandles, startSpillingFraction, false);
	}
	
	/**
	 * Internal constructor and constructor for subclasses that want to circumvent the spilling.
	 * 
	 * @param memoryManager The memory manager from which to allocate the memory.
	 * @param ioManager The I/O manager, which is used to write temporary files to disk.
	 * @param input The input that is sorted by this sorter.
	 * @param parentTask The parent task, which owns all resources used by this sorter.
	 * @param serializer The type serializer.
	 * @param comparator The type comparator establishing the order relation.
	 * @param totalMemory The total amount of memory dedicated to sorting, merging and I/O.
	 * @param numSortBuffers The number of distinct buffers to use creation of the initial runs.
	 * @param maxNumFileHandles The maximum number of files to be merged at once.
	 * @param startSpillingFraction The faction of the buffers that have to be filled before the spilling thread
	 *                              actually begins spilling data to disk.
	 * @param noSpillingMemory When set to true, no memory will be allocated for writing and no spilling thread
	 *                   will be spawned.
	 * 
	 * @throws IOException Thrown, if an error occurs initializing the resources for external sorting.
	 * @throws MemoryAllocationException Thrown, if not enough memory can be obtained from the memory manager to
	 *                                   perform the sort.
	 */
	protected UnilateralSortMerger(MemoryManager memoryManager, IOManager ioManager,
			MutableObjectIterator<E> input, AbstractInvokable parentTask, 
			TypeSerializer<E> serializer, TypeComparator<E> comparator,
			long totalMemory, int numSortBuffers, int maxNumFileHandles, 
			float startSpillingFraction, boolean noSpillingMemory)
	throws IOException, MemoryAllocationException
	{
		// sanity checks
		if (memoryManager == null | (ioManager == null && !noSpillingMemory) | serializer == null | comparator == null) {
			throw new NullPointerException();
		}
		if (parentTask == null) {
			throw new NullPointerException("Parent Task must not be null.");
		}
		if (maxNumFileHandles < 2) {
			throw new IllegalArgumentException("Merger cannot work with less than two file handles.");
		}
		
		this.memoryManager = memoryManager;
		
		// adjust the memory quotas to the page size
		totalMemory = memoryManager.roundDownToPageSizeMultiple(totalMemory);
		final int numPagesTotal = MathUtils.checkedDownCast(totalMemory / memoryManager.getPageSize());

		if (numPagesTotal < MIN_NUM_WRITE_BUFFERS + MIN_NUM_SORT_MEM_SEGMENTS) {
			throw new IllegalArgumentException("Too little memory provided to sorter to perform task. " +
				"Required are at least " + (MIN_NUM_WRITE_BUFFERS + MIN_NUM_SORT_MEM_SEGMENTS) + 
				" pages. Current page size is " + memoryManager.getPageSize() + " bytes.");
		}
		
		// determine how many buffers to use for writing
		final int numWriteBuffers;
		if (noSpillingMemory) {
			numWriteBuffers = 0;
		} else {
			// determine how many buffers we have when we do a full mere with maximal fan-in 
			final int minBuffers = MIN_NUM_WRITE_BUFFERS + maxNumFileHandles;
			final int desiredBuffers = MIN_NUM_WRITE_BUFFERS + 2 * maxNumFileHandles;
			
			if (desiredBuffers > numPagesTotal) {
				numWriteBuffers = MIN_NUM_WRITE_BUFFERS;
				if (minBuffers > numPagesTotal) {
					maxNumFileHandles = numPagesTotal - MIN_NUM_WRITE_BUFFERS;
					if (LOG.isWarnEnabled()) {
						LOG.warn("Reducing maximal merge fan-in to " + maxNumFileHandles + " due to limited memory availability during merge");
					}
				}
			}
			else {
				// we are free to choose. make sure that we do not eat up too much memory for writing
				final int designatedWriteBuffers = numPagesTotal / (maxNumFileHandles + 1);
				final int fractional = numPagesTotal / 64;
				final int maximal = numPagesTotal - MIN_NUM_SORT_MEM_SEGMENTS;
				
				numWriteBuffers = Math.max(MIN_NUM_WRITE_BUFFERS,	// at least the lower bound
					Math.min(Math.min(MAX_NUM_WRITE_BUFFERS, maximal), 		// at most the lower of the upper bounds
					Math.min(designatedWriteBuffers, fractional)));			// the lower of the average
			}
		}
		
		final int sortMemPages = numPagesTotal - numWriteBuffers;
		final long sortMemory = ((long) sortMemPages) * memoryManager.getPageSize();
		
		// decide how many sort buffers to use
		if (numSortBuffers < 1) {
			if (sortMemory > 96 * 1024 * 1024) {
				numSortBuffers = 3;
			}
			else if (sortMemPages >= 2 * MIN_NUM_SORT_MEM_SEGMENTS) {
				numSortBuffers = 2;
			}
			else {
				numSortBuffers = 1;
			}
		}
		final int numSegmentsPerSortBuffer = sortMemPages / numSortBuffers;
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("Instantiating sorter with " + sortMemPages + " pages of sorting memory (=" +
				sortMemory + " bytes total) divided over " + numSortBuffers + " sort buffers (" + 
				numSegmentsPerSortBuffer + " pages per buffer). Using " + numWriteBuffers + 
				" buffers for writing sorted results and merging maximally " + maxNumFileHandles +
				" streams at once.");
		}
		
		this.writeMemory = new ArrayList<MemorySegment>(numWriteBuffers);
		this.sortReadMemory = new ArrayList<MemorySegment>(sortMemPages);
		
		// allocate the memory
		memoryManager.allocatePages(parentTask, this.sortReadMemory, sortMemPages);
		if (numWriteBuffers > 0) {
			memoryManager.allocatePages(parentTask, this.writeMemory, numWriteBuffers);
		}
		
		// circular queues pass buffers between the threads
		final CircularQueues<E> circularQueues = new CircularQueues<E>();
		
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
			final NormalizedKeySorter<E> buffer = new NormalizedKeySorter<E>(serializer, comp, sortSegments);

			// add to empty queue
			CircularElement<E> element = new CircularElement<E>(i, buffer);
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
		this.channelsToDeleteAtShutdown = new HashSet<Channel.ID>(64);
		this.openChannels = new HashSet<BlockChannelAccess<?,?>>(64);

		// start the thread that reads the input channels
		this.readThread = getReadingThread(exceptionHandler, input, circularQueues, parentTask,
			serializer, ((long) (startSpillingFraction * sortMemory)));

		// start the thread that sorts the buffers
		this.sortThread = getSortingThread(exceptionHandler, circularQueues, parentTask);

		// start the thread that handles spilling to secondary storage
		this.spillThread = getSpillingThread(exceptionHandler, circularQueues, parentTask, 
				memoryManager, ioManager, serializer, comparator, this.sortReadMemory, this.writeMemory, 
				maxNumFileHandles);
		
		startThreads();
	}
	
	/**
	 * Starts all the threads that are used by this sort-merger.
	 */
	protected void startThreads()
	{
		if (this.readThread != null)
			this.readThread.start();
		if (this.sortThread != null)
			this.sortThread.start();
		if (this.spillThread != null)
			this.spillThread.start();
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
	public void close()
	{
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
					for (Iterator<BlockChannelAccess<?, ?>> channels = this.openChannels.iterator(); channels.hasNext(); ) {
						final BlockChannelAccess<?, ?> channel = channels.next();
						channels.remove();
						channel.closeAndDelete();
					}
				}
				catch (Throwable t) {}
			}
			
			// we have to loop this, because it may fail with a concurrent modification exception
			while (!this.channelsToDeleteAtShutdown.isEmpty()) {
				try {
					for (Iterator<Channel.ID> channels = this.channelsToDeleteAtShutdown.iterator(); channels.hasNext(); ) {
						final Channel.ID channel = channels.next();
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
			MutableObjectIterator<E> reader, CircularQueues<E> queues, AbstractInvokable parentTask,
			TypeSerializer<E> serializer, long startSpillingBytes)
	{
		return new ReadingThread<E>(exceptionHandler, reader, queues, serializer.createInstance(),
			parentTask, startSpillingBytes);
	}

	/**
	 * Creates the sorting thread. This thread takes the buffers from the sort queue, sorts them and
	 * puts them into the spill queue.
	 * <p>
	 * The returned thread is not yet started.
	 * 
	 * @param exceptionHandler
	 *        The handler for exceptions in the thread.
	 * @param queues
	 *        The queues through which the thread communicates with the other threads.
	 * @param parentTask
	 *        The task at which the thread registers itself (for profiling purposes).
	 * @return The sorting thread.
	 */
	protected ThreadBase<E> getSortingThread(ExceptionHandler<IOException> exceptionHandler, CircularQueues<E> queues,
			AbstractInvokable parentTask)
	{
		return new SortingThread<E>(exceptionHandler, queues, parentTask);
	}

	/**
	 * Creates the spilling thread. This thread also merges the number of sorted streams until a sufficiently
	 * small number of streams is produced that can be merged on the fly while returning the results.
	 * 
	 * @param exceptionHandler The handler for exceptions in the thread.
	 * @param queues The queues through which the thread communicates with the other threads.
	 * @param memoryManager The memory manager from which the memory is allocated.
	 * @param ioManager The I/O manager that creates channel readers and writers.
	 * @param readMemSize The amount of memory to be dedicated to reading / pre-fetching buffers. This memory must
	 *                    only be allocated once the sort buffers have been freed.
	 * @param parentTask The task at which the thread registers itself (for profiling purposes).
	 * @return The thread that does the spilling and pre-merging.
	 */
	protected ThreadBase<E> getSpillingThread(ExceptionHandler<IOException> exceptionHandler, CircularQueues<E> queues,
			AbstractInvokable parentTask, MemoryManager memoryManager, IOManager ioManager, 
			TypeSerializer<E> serializer, TypeComparator<E> comparator,
			List<MemorySegment> sortReadMemory, List<MemorySegment> writeMemory, int maxFileHandles)
	{
		return new SpillingThread(exceptionHandler, queues, parentTask,
			memoryManager, ioManager, serializer, comparator, sortReadMemory, writeMemory, maxFileHandles);
	}

	// ------------------------------------------------------------------------
	//                           Result Iterator
	// ------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.sort.SortMerger#getIterator()
	 */
	@Override
	public MutableObjectIterator<E> getIterator() throws InterruptedException
	{
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
	protected final void setResultIterator(MutableObjectIterator<E> iterator)
	{
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
	protected final void setResultIteratorException(IOException ioex)
	{
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
	protected static <T> CircularElement<T> endMarker()
	{
		@SuppressWarnings("unchecked")
		CircularElement<T> c = (CircularElement<T>) EOF_MARKER;
		return c;
	}
	
	/**
	 * Gets the element that is passed as marker for signal beginning of spilling.
	 * 
	 * @return The element that is passed as marker for signal beginning of spilling.
	 */
	protected static <T> CircularElement<T> spillingMarker()
	{
		@SuppressWarnings("unchecked")
		CircularElement<T> c = (CircularElement<T>) SPILLING_MARKER;
		return c;
	}
	
	/**
	 * Class representing buffers that circulate between the reading, sorting and spilling thread.
	 */
	protected static final class CircularElement<E>
	{
		final int id;
		final NormalizedKeySorter<E> buffer;

		public CircularElement() {
			this.buffer = null;
			this.id = -1;
		}

		public CircularElement(int id, NormalizedKeySorter<E> buffer) {
			this.id = id;
			this.buffer = buffer;
		}
	}

	/**
	 * Collection of queues that are used for the communication between the threads.
	 */
	protected static final class CircularQueues<E>
	{
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
	protected static abstract class ThreadBase<E> extends Thread implements Thread.UncaughtExceptionHandler
	{
		/**
		 * The queue of empty buffer that can be used for reading;
		 */
		protected final CircularQueues<E> queues;

		/**
		 * The exception handler for any problems.
		 */
		private final ExceptionHandler<IOException> exceptionHandler;

		/**
		 * The parent task at whom the thread needs to register.
		 */
		private final AbstractInvokable parentTask;

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
			this.parentTask = parentTask;
			this.alive = true;
		}

		/**
		 * Implements exception handling and delegates to go().
		 */
		public void run() {
			try {
				if (this.parentTask != null) {
					this.parentTask.userThreadStarted(this);
				}
				go();
			}
			catch (Throwable t) {
				internalHandleException(new IOException("Thread '" + getName() + "' terminated due to an exception: "
					+ t.getMessage(), t));
			}
			finally {
				if (this.parentTask != null) {
					this.parentTask.userThreadFinished(this);
				}
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
	protected static class ReadingThread<E> extends ThreadBase<E>
	{
		/**
		 * The input channels to read from.
		 */
		private final MutableObjectIterator<E> reader;
		
		/**
		 * The fraction of the buffers that must be full before the spilling starts.
		 */
		private final long startSpillingBytes;
		
		/**
		 * The object into which the thread reads the data from the input.
		 */
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
				E readTarget,
				AbstractInvokable parentTask, long startSpillingBytes)
		{
			super(exceptionHandler, "SortMerger Reading Thread", queues, parentTask);

			// members
			this.reader = reader;
			this.readTarget = readTarget;
			this.startSpillingBytes = startSpillingBytes;
		}

		/**
		 * The entry point for the thread. Gets a buffer for all threads and then loops as long as there is input
		 * available.
		 */
		public void go() throws IOException
		{	
			final MutableObjectIterator<E> reader = this.reader;
			
			final E current = this.readTarget;
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
						if (isRunning()) {
							LOG.error("Reading thread was interrupted (without being shut down) while grabbing a buffer. " +
									"Retrying to grab buffer...");
						} else {
							return;
						}
					}
				}
				
				// get the new buffer and check it
				final NormalizedKeySorter<E> buffer = element.buffer;
				if (!buffer.isEmpty()) {
					throw new IOException("New buffer is not empty.");
				}
				
				if (LOG.isDebugEnabled()) {
					LOG.debug("Retrieved empty read buffer " + element.id + ".");
				}
				
				// write the last leftover pair, if we have one
				if (leftoverRecord != null) {
					if (!buffer.write(leftoverRecord)) {
						throw new IOException("Record could not be written to empty buffer: Serialized record exceeds buffer capacity.");
					}
					leftoverRecord = null;
				}
				
				// we have two distinct code paths, depending on whether the spilling
				// threshold will be crossed in the current buffer, or not.
				if (bytesUntilSpilling > 0 && buffer.getCapacity() >= bytesUntilSpilling)
				{
					boolean fullBuffer = false;
					
					// spilling will be triggered while this buffer is filled
					// loop until the buffer is full or the reader is exhausted
					while (isRunning() && reader.next(current))
					{
						if (!buffer.write(current)) {
							leftoverRecord = current;
							fullBuffer = true;
							break;
						}
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
				while (isRunning() && reader.next(current)) {
					if (!buffer.write(current)) {
						leftoverRecord = current;
						break;
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
	protected static class SortingThread<E> extends ThreadBase<E>
	{		
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
		public void go() throws IOException
		{			
			boolean alive = true;

			// loop as long as the thread is marked alive
			while (isRunning() && alive) {
				CircularElement<E> element = null;
				try {
					element = this.queues.sort.take();
				}
				catch (InterruptedException iex) {
					if (isRunning()) {
						if (LOG.isErrorEnabled())
							LOG.error(
								"Sorting thread was interrupted (without being shut down) while grabbing a buffer. " +
								"Retrying to grab buffer...");
						continue;
					}
					else {
						return;
					}
				}

				if (element != EOF_MARKER && element != SPILLING_MARKER) {
					if (LOG.isDebugEnabled())
						LOG.debug("Sorting buffer " + element.id + ".");
					
					this.sorter.sort(element.buffer);
					
					if (LOG.isDebugEnabled())
						LOG.debug("Sorted buffer " + element.id + ".");
				}
				else if (element == EOF_MARKER) {
					if (LOG.isDebugEnabled())
						LOG.debug("Sorting thread done.");
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
	protected class SpillingThread extends ThreadBase<E>
	{		
		protected final MemoryManager memManager;			// memory manager to release memory
		
		protected final IOManager ioManager;				// I/O manager to create channels
		
		protected final TypeSerializer<E> serializer;		// The serializer for the data type
		
		protected final TypeComparator<E> comparator;		// The comparator that establishes the order relation.
		
		protected final List<MemorySegment> writeMemory;	// memory segments for writing
		
		protected final List<MemorySegment> sortReadMemory;	// memory segments for sorting/reading
		
		protected final int maxNumFileHandles;
		
		protected final int numWriteBuffersToCluster;
		
		/**
		 * Creates the spilling thread.
		 * 
		 * @param exceptionHandler The exception handler to call for all exceptions.
		 * @param queues The queues used to pass buffers between the threads.
		 * @param parentTask The task that started this thread. If non-null, it is used to register this thread.
		 * @param memoryManager The memory manager used to allocate buffers for the readers and writers.
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
			this.sortReadMemory = sortReadMemory;
			this.writeMemory = writeMemory;
			this.maxNumFileHandles = maxNumFileHandles;
			this.numWriteBuffersToCluster = writeMemory.size() >= 4 ? writeMemory.size() / 2 : 1;
		}

		/**
		 * Entry point of the thread.
		 */
		public void go() throws IOException
		{
			// ------------------- In-Memory Cache ------------------------
			
			final Queue<CircularElement<E>> cache = new ArrayDeque<CircularElement<E>>();
			CircularElement<E> element = null;
			boolean cacheOnly = false;
			
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
					}
					else
						return;
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
			
			// ------------------- In-Memory Merge ------------------------
			if (cacheOnly) {
				/* operates on in-memory segments only */
				if (LOG.isDebugEnabled())
					LOG.debug("Initiating in memory merge.");
				
				List<MutableObjectIterator<E>> iterators = new ArrayList<MutableObjectIterator<E>>(cache.size());
								
				// iterate buffers and collect a set of iterators
				for (CircularElement<E> cached : cache)
				{
					// note: the yielded iterator only operates on the buffer heap (and disregards the stack)
					iterators.add(cached.buffer.getIterator());
				}
				
				// release the remaining sort-buffers
				if (LOG.isDebugEnabled())
					LOG.debug("Releasing unused sort-buffer memory.");
				disposeSortBuffers(true);
				
				// set lazy iterator
				setResultIterator(iterators.isEmpty() ? EmptyMutableObjectIterator.<E>get() :
						iterators.size() == 1 ? iterators.get(0) : 
						new MergeIterator<E>(iterators,	this.serializer, this.comparator));
				return;
			}			
			
			// ------------------- Spilling Phase ------------------------
			
			final Channel.Enumerator enumerator = this.ioManager.createChannelEnumerator();			
			List<ChannelWithBlockCount> channelIDs = new ArrayList<ChannelWithBlockCount>();

			
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
					}
					else return;
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
				Channel.ID channel = enumerator.next();
				registerChannelToBeRemovedAtShudown(channel);

				// create writer
				final BlockChannelWriter writer = this.ioManager.createBlockChannelWriter(
																channel, this.numWriteBuffersToCluster);
				registerOpenChannelToBeRemovedAtShudown(writer);
				final ChannelWriterOutputView output = new ChannelWriterOutputView(writer, this.writeMemory,
																			this.memManager.getPageSize());

				// write sort-buffer to channel
				if (LOG.isDebugEnabled())
					LOG.debug("Spilling buffer " + element.id + ".");
				element.buffer.writeToOutput(output);
				if (LOG.isDebugEnabled())
					LOG.debug("Spilled buffer " + element.id + ".");

				output.close();
				unregisterOpenChannelToBeRemovedAtShudown(writer);
				
				channelIDs.add(new ChannelWithBlockCount(channel, output.getBlockCount()));

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
			
			// merge channels until sufficient file handles are available
			while (isRunning() && channelIDs.size() > this.maxNumFileHandles) {
				channelIDs = mergeChannelList(channelIDs, this.sortReadMemory, this.writeMemory);
			}
			
			// from here on, we won't write again
			this.memManager.release(this.writeMemory);
			this.writeMemory.clear();
			
			// check if we have spilled some data at all
			if (channelIDs.isEmpty()) {
				setResultIterator(EmptyMutableObjectIterator.<E>get());
			}
			else {
				if (LOG.isDebugEnabled())
					LOG.debug("Beginning final merge.");
				
				// allocate the memory for the final merging step
				List<List<MemorySegment>> readBuffers = new ArrayList<List<MemorySegment>>(channelIDs.size());
				
				// allocate the read memory and register it to be released
				getSegmentsForReaders(readBuffers, this.sortReadMemory, channelIDs.size());
				
				// get the readers and register them to be released
				setResultIterator(getMergingIterator(channelIDs, readBuffers, new ArrayList<BlockChannelAccess<?, ?>>(channelIDs.size())));
			}

			// done
			if (LOG.isDebugEnabled())
				LOG.debug("Spilling and merging thread done.");
		}
		
		/**
		 * Releases the memory that is registered for in-memory sorted run generation.
		 */
		protected final void disposeSortBuffers(boolean releaseMemory)
		{
			while (!this.queues.empty.isEmpty()) {
				try {
					final NormalizedKeySorter<?> sorter = this.queues.empty.take().buffer;
					final List<MemorySegment> sorterMem = sorter.dispose();
					if (releaseMemory) {
						this.memManager.release(sorterMem);
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
		throws InterruptedException
		{
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
			final List<List<MemorySegment>> inputSegments, List<BlockChannelAccess<?, ?>> readerList)
		throws IOException
		{
			// create one iterator per channel id
			if (LOG.isDebugEnabled())
				LOG.debug("Performing merge of " + channelIDs.size() + " sorted streams.");
			
			final List<MutableObjectIterator<E>> iterators = new ArrayList<MutableObjectIterator<E>>(channelIDs.size());
			
			for (int i = 0; i < channelIDs.size(); i++) {
				final ChannelWithBlockCount channel = channelIDs.get(i);
				final List<MemorySegment> segsForChannel = inputSegments.get(i);
				
				// create a reader. if there are multiple segments for the reader, issue multiple together per I/O request
				final BlockChannelReader reader = segsForChannel.size() >= 4 ? 
					this.ioManager.createBlockChannelReader(channel.getChannel(), segsForChannel.size() / 2) :
					this.ioManager.createBlockChannelReader(channel.getChannel());
					
				readerList.add(reader);
				registerOpenChannelToBeRemovedAtShudown(reader);
				unregisterChannelToBeRemovedAtShudown(channel.getChannel());
				
				// wrap channel reader as a view, to get block spanning record deserialization
				final ChannelReaderInputView inView = new ChannelReaderInputView(reader, segsForChannel, 
																			channel.getBlockCount(), false);
				iterators.add(new ChannelReaderInputViewIterator<E>(inView, null, this.serializer));
			}

			return new MergeIterator<E>(iterators, this.serializer, this.comparator);
		}

		/**
		 * Merges the given sorted runs to a smaller number of sorted runs. 
		 * 
		 * @param channelIDs The IDs of the sorted runs that need to be merged.
		 * @param writeBuffers The buffers to be used by the writers.
		 * @param writeBufferSize The size of the write buffers.
		 * @param  readMemorySize The amount of memory dedicated to the readers.
		 * @return A list of the IDs of the merged channels.
		 * @throws IOException Thrown, if the readers or writers encountered an I/O problem.
		 * @throws MemoryAllocationException Thrown, if the specified memory is insufficient to merge the channels
		 *                                   or if the memory manager could not provide the requested memory.
		 */
		protected final List<ChannelWithBlockCount> mergeChannelList(final List<ChannelWithBlockCount> channelIDs,
					final List<MemorySegment> allReadBuffers, final List<MemorySegment> writeBuffers)
		throws IOException
		{
			final double numMerges = Math.ceil(channelIDs.size() / ((double) this.maxNumFileHandles));
			final int channelsToMergePerStep = (int) Math.ceil(channelIDs.size() / numMerges);
			
			// allocate the memory for the merging step
			final List<List<MemorySegment>> readBuffers = new ArrayList<List<MemorySegment>>(channelsToMergePerStep);
			getSegmentsForReaders(readBuffers, allReadBuffers, channelsToMergePerStep);
			
			// the list containing the IDs of the merged channels
			final ArrayList<ChannelWithBlockCount> mergedChannelIDs = new ArrayList<ChannelWithBlockCount>((int) (numMerges + 1));

			final ArrayList<ChannelWithBlockCount> channelsToMergeThisStep = new ArrayList<ChannelWithBlockCount>(channelsToMergePerStep);
			int channelNum = 0;
			while (isRunning() && channelNum < channelIDs.size()) {
				channelsToMergeThisStep.clear();

				for (int i = 0; i < channelsToMergePerStep && channelNum < channelIDs.size(); i++, channelNum++) {
					channelsToMergeThisStep.add(channelIDs.get(channelNum));
				}
				
				// merge only, if there is more than one channel
				if (channelsToMergeThisStep.size() < 2)  {
					mergedChannelIDs.addAll(channelsToMergeThisStep);
				}
				else {
					mergedChannelIDs.add(mergeChannels(channelsToMergeThisStep, readBuffers, writeBuffers));
				}
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
			final List<BlockChannelAccess<?, ?>> channelAccesses = new ArrayList<BlockChannelAccess<?, ?>>(channelIDs.size());

			// the list with the target iterators
			final MergeIterator<E> mergeIterator = getMergingIterator(channelIDs, readBuffers, channelAccesses);

			// create a new channel writer
			final Channel.ID mergedChannelID = this.ioManager.createChannel();
			registerChannelToBeRemovedAtShudown(mergedChannelID);
			final BlockChannelWriter writer = this.ioManager.createBlockChannelWriter(
															mergedChannelID, this.numWriteBuffersToCluster);
			registerOpenChannelToBeRemovedAtShudown(writer);
			final ChannelWriterOutputView output = new ChannelWriterOutputView(writer, writeBuffers, 
																			this.memManager.getPageSize());

			// read the merged stream and write the data back
			final TypeSerializer<E> serializer = this.serializer;
			final E rec = serializer.createInstance();
			while (mergeIterator.next(rec)) {
				serializer.serialize(rec, output);
			}
			output.close();
			final int numBlocksWritten = output.getBlockCount();
			
			// register merged result to be removed at shutdown
			unregisterOpenChannelToBeRemovedAtShudown(writer);
			
			// remove the merged channel readers from the clear-at-shutdown list
			for (int i = 0; i < channelAccesses.size(); i++) {
				BlockChannelAccess<?, ?> access = channelAccesses.get(i);
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
		 * @return A list with all memory segments that were allocated.
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
		 * @param s The channel id.
		 */
		protected void registerChannelToBeRemovedAtShudown(Channel.ID channel) {
			UnilateralSortMerger.this.channelsToDeleteAtShutdown.add(channel);
		}

		/**
		 * Removes a channel from the list of channels that are to be removed at shutdown.
		 * 
		 * @param s The channel id.
		 */
		protected void unregisterChannelToBeRemovedAtShudown(Channel.ID channel) {
			UnilateralSortMerger.this.channelsToDeleteAtShutdown.remove(channel);
		}
		
		/**
		 * Adds a channel reader/writer to the list of channels that are to be removed at shutdown.
		 * 
		 * @param s The channel reader/writer.
		 */
		protected void registerOpenChannelToBeRemovedAtShudown(BlockChannelAccess<?, ?> channel) {
			UnilateralSortMerger.this.openChannels.add(channel);
		}

		/**
		 * Removes a channel reader/writer from the list of channels that are to be removed at shutdown.
		 * 
		 * @param s The channel reader/writer.
		 */
		protected void unregisterOpenChannelToBeRemovedAtShudown(BlockChannelAccess<?, ?> channel) {
			UnilateralSortMerger.this.openChannels.remove(channel);
		}
	}
	
	/**
	 *
	 */
	public static final class InputDataCollector<E> implements Collector<E>
	{
		private final CircularQueues<E> queues;		// the queues used to pass buffers
		
		private NormalizedKeySorter<E> currentBuffer;
		
		private CircularElement<E> currentElement;
		
		private long bytesUntilSpilling;			// number of bytes left before we signal to spill
		
		private boolean spillingInThisBuffer;
		
		private volatile boolean running;
		

		public InputDataCollector(CircularQueues<E> queues, long startSpillingBytes)
		{
			this.queues = queues;
			this.bytesUntilSpilling = startSpillingBytes;
			this.running = true;
			
			grabBuffer();
		}
		
		private void grabBuffer()
		{
			while (this.currentElement == null) {
				try {
					this.currentElement = this.queues.empty.take();
				}
				catch (InterruptedException iex) {
					if (this.running) {
						LOG.error("Reading thread was interrupted (without being shut down) while grabbing a buffer. " +
								"Retrying to grab buffer...");
					} else {
						return;
					}
				}
			}
			
			this.currentBuffer = this.currentElement.buffer;
			if (!this.currentBuffer.isEmpty()) {
				throw new RuntimeException("New sort-buffer is not empty.");
			}
			
			if (LOG.isDebugEnabled()) {
				LOG.debug("Retrieved empty read buffer " + this.currentElement.id + ".");
			}
			
			this.spillingInThisBuffer = this.currentBuffer.getCapacity() <= this.bytesUntilSpilling;
		}
		
		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stubs.Collector#collect(eu.stratosphere.pact.common.type.PactRecord)
		 */
		@Override
		public void collect(E record)
		{
			try {
				if (this.spillingInThisBuffer) {
					if (this.currentBuffer.write(record)) {
						if (this.bytesUntilSpilling - this.currentBuffer.getOccupancy() <= 0) {
							this.bytesUntilSpilling = 0;
							// send the sentinel
							this.queues.sort.add(UnilateralSortMerger.<E>spillingMarker());
						}
						return;
					}
				}
				else {
					// no spilling in this buffer
					if (this.currentBuffer.write(record))
						return;
				}
				
				if (this.bytesUntilSpilling > 0) {
					this.bytesUntilSpilling -= this.currentBuffer.getCapacity();
					if (this.bytesUntilSpilling <= 0) {
						this.bytesUntilSpilling = 0;
						// send the sentinel
						this.queues.sort.add(UnilateralSortMerger.<E>spillingMarker());
					}
				}
				
				// we came here when the buffer could not be written. send it to the sorter
				// send the buffer
				if (LOG.isDebugEnabled()) {
					LOG.debug("Emitting full buffer from reader thread: " + this.currentElement.id + ".");
				}
				this.queues.sort.add(this.currentElement);
				this.currentElement = null;
				
				// we need a new buffer. grab the next one
				while (this.running && this.currentElement == null) {
					try {
						this.currentElement = this.queues.empty.take();
					}
					catch (InterruptedException iex) {
						if (this.running) {
							LOG.error("Reading thread was interrupted (without being shut down) while grabbing a buffer. " +
									"Retrying to grab buffer...");
						} else {
							return;
						}
					}
				}
				if (!this.running)
					return;
				
				this.currentBuffer = this.currentElement.buffer;
				if (!this.currentBuffer.isEmpty()) {
					throw new RuntimeException("BUG: New sort-buffer is not empty.");
				}
				
				if (LOG.isDebugEnabled()) {
					LOG.debug("Retrieved empty read buffer " + this.currentElement.id + ".");
				}
				// write the record
				if (!this.currentBuffer.write(record)) {
					throw new RuntimeException("Record could not be written to empty sort-buffer: Serialized record exceeds buffer capacity.");
				}
			}
			catch (IOException ioex) {
				throw new RuntimeException("BUG: An error occurred while writing a record to the sort buffer: " + 
						ioex.getMessage(), ioex);
			}
		}
		
		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stubs.Collector#close()
		 */
		@Override
		public void close()
		{
			if (this.running) {
				this.running = false;
				
				if (this.currentBuffer != null && this.currentElement != null) {
					if (this.currentBuffer.isEmpty()) {
						this.queues.empty.add(this.currentElement);
					}
					else {
						this.queues.sort.add(this.currentElement);
						if (LOG.isDebugEnabled()) {
							LOG.debug("Emitting last buffer from input collector: " + this.currentElement.id + ".");
						}
					}
				}
				
				this.currentBuffer = null;
				this.currentElement = null;
				
				this.queues.sort.add(UnilateralSortMerger.<E>endMarker());
			}
		}
	}
	
	protected static final class ChannelWithBlockCount
	{
		private final Channel.ID channel;
		private final int blockCount;
		
		public ChannelWithBlockCount(ID channel, int blockCount) {
			this.channel = channel;
			this.blockCount = blockCount;
		}

		public Channel.ID getChannel() {
			return channel;
		}

		public int getBlockCount() {
			return blockCount;
		}
	}
}