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

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.services.iomanager.BlockChannelAccess;
import eu.stratosphere.nephele.services.iomanager.BlockChannelReader;
import eu.stratosphere.nephele.services.iomanager.BlockChannelWriter;
import eu.stratosphere.nephele.services.iomanager.Channel;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.io.ChannelReaderInputView;
import eu.stratosphere.pact.runtime.io.ChannelWriterOutputView;
import eu.stratosphere.pact.runtime.plugable.PactRecordAccessors;
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
public class UnilateralSortMerger implements SortMerger
{	
	// ------------------------------------------------------------------------
	//                              Constants
	// ------------------------------------------------------------------------

	/**
	 * Logging.
	 */
	private static final Log LOG = LogFactory.getLog(UnilateralSortMerger.class);
	
	/**
	 * The minimal size of an IO buffer. Currently set to 32 KiBytes
	 */
	protected static final int MIN_IO_BUFFER_SIZE = 32 * 1024;

	/**
	 * The maximal size of an IO buffer. Currently set to 512 KiBytes
	 */
	protected static final int MAX_IO_BUFFER_SIZE = 512 * 1024;
	
	/**
	 * The number of buffers to use by the writers.
	 */
	protected static final int NUM_WRITE_BUFFERS = 2;
	
	/**
	 * The size of a memory segment used for sorting.
	 */
	protected static final int SORT_MEM_SEGMENT_SIZE = 64 * 1024;
	
	/**
	 * The minimum number of segments that are required for the sort to operate.
	 */
	protected static final int MIN_NUM_SORT_MEM_SEGMENTS = 32;
	
	/**
	 * The minimal size of a sort buffer, currently 2 MiBytes.
	 */
	protected static final int MIN_SORT_MEM = SORT_MEM_SEGMENT_SIZE * MIN_NUM_SORT_MEM_SEGMENTS;
	
	/**
	 * The minimal amount of memory required for writing.
	 */
	protected static final int MIN_WRITE_MEM = NUM_WRITE_BUFFERS * MIN_IO_BUFFER_SIZE;

	// ------------------------------------------------------------------------
	//                               Fields
	// ------------------------------------------------------------------------

	/**
	 * This list contains all segments of allocated memory. They will be freed the latest in the
	 * shutdown method. If some segments have been freed before, they will not be freed again.
	 */
	private final List<List<MemorySegment>> memoryToReleaseAtShutdown;
	
	/**
	 * A list of currently open channels, which need to be closed and removed when the sorter is closed.
	 */
	private final List<BlockChannelAccess<?, ?>> openChannels;
	
	/**
	 * A list of lists containing channel readers and writers that will be closed at shutdown.
	 */
	private final List<Channel.ID> channelsToDeleteAtShutdown;
	
	/**
	 * The segments for the sort buffers.
	 */
	protected final List<NormalizedKeySorter<?>> sortBuffers;

	/**
	 * The memory manager through which memory is allocated and released.
	 */
	protected final MemoryManager memoryManager;

	/**
	 * The I/O manager through which file reads and writes are performed.
	 */
	protected final IOManager ioManager;

	/**
	 * The comparator through which an order over the keys is established.
	 */
	protected final Comparator<Key>[] keyComparators;
	
	/**
	 * The positions of the keys in the records.
	 */
	protected final int[] keyPositions;
	
	/**
	 * The classes of the key types.
	 */
	protected final Class<? extends Key>[] keyClasses;
	
	/**
	 * The parent task that owns this sorter.
	 */
	protected final AbstractInvokable parent;
	
	/**
	 * The monitor which guards the iterator field.
	 */
	protected final Object iteratorLock = new Object();
	
	/**
	 * The iterator to be returned by the sort-merger. This variable is zero, while receiving and merging is still in
	 * progress and it will be set once we have &lt; merge factor sorted sub-streams that will then be streamed sorted.
	 */
	protected volatile MutableObjectIterator<PactRecord> iterator;
	
	/**
	 * The exception that is set, if the iterator cannot be created.
	 */
	protected volatile IOException iteratorException;

	/**
	 * The maximum number of file handles
	 */
	protected final int maxNumFileHandles;
	
	/**
	 * The size of the buffers used for I/O.
	 */
	protected final int ioBufferSize;
	
	/**
	 * Flag indicating that the sorter was closed.
	 */
	protected volatile boolean closed;
	
	// ------------------------------------------------------------------------
	//                                  Threads
	// ------------------------------------------------------------------------

	/**
	 * The thread that reads the input channels into buffers and passes them on to the merger.
	 */
	private final ThreadBase readThread;

	/**
	 * The thread that merges the buffer handed from the reading thread.
	 */
	private final ThreadBase sortThread;

	/**
	 * The thread that handles spilling to secondary storage.
	 */
	private final ThreadBase spillThread;

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
	 * @param totalMemory The total amount of memory dedicated to sorting and merging.
	 * @param keyComparators The comparator used to define the order among the keys.
	 * @param keyPositions The logical positions of the keys in the records.
	 * @param keyClasses The types of the keys.
	 * @param input The input that is sorted by this sorter.
	 * @param parentTask The parent task, which owns all resources used by this sorter.
	 * @param startSpillingFraction The faction of the buffers that have to be filled before the spilling thread
	 *                              actually begins spilling data to disk.
	 * 
	 * @throws IOException Thrown, if an error occurs initializing the resources for external sorting.
	 * @throws MemoryAllocationException Thrown, if not enough memory can be obtained from the memory manager to
	 *                                   perform the sort.
	 */
	public UnilateralSortMerger(MemoryManager memoryManager, IOManager ioManager,
			long totalMemory, int maxNumFileHandles,
			Comparator<Key>[] keyComparators, int[] keyPositions, Class<? extends Key>[] keyClasses,
			MutableObjectIterator<PactRecord> input, AbstractInvokable parentTask, float startSpillingFraction)
	throws IOException, MemoryAllocationException
	{
		this(memoryManager, ioManager, totalMemory, -1, -1, maxNumFileHandles, keyComparators, keyPositions, keyClasses, 
			input, parentTask, startSpillingFraction);
	}
	
	/**
	 * Creates a new sorter that reads the data from a given reader and provides an iterator returning that
	 * data in a sorted manner. The memory is divided among sort buffers, write buffers and read buffers
	 * automatically.
	 * <p>
	 * WARNING: The given comparator is used simultaneously in multiple threads (the sorting thread and the merging
	 * thread). Make sure that the given comparator is stateless and does not make use of member variables.
	 * 
	 * @param memoryManager The memory manager from which to allocate the memory.
	 * @param ioManager The I/O manager, which is used to write temporary files to disk.
	 * @param totalMemory The total amount of memory dedicated to sorting, merging and I/O.
	 * @param maxWriteMem The maximal amount of memory to be dedicated to writing sorted runs. Will be subtracted from the total
	 *                 amount of memory (<code>totalMemory</code>).
	 * @param numSortBuffers The number of distinct buffers to use creation of the initial runs.
	 * @param maxNumFileHandles The maximum number of files to be merged at once.
	 * @param keyComparators The comparator used to define the order among the keys.
	 * @param keyPositions The logical positions of the keys in the records.
	 * @param keyClasses The types of the keys.
	 * @param input The input that is sorted by this sorter.
	 * @param parentTask The parent task, which owns all resources used by this sorter.
	 * @param startSpillingFraction The faction of the buffers that have to be filled before the spilling thread
	 *                              actually begins spilling data to disk.
	 * 
	 * @throws IOException Thrown, if an error occurs initializing the resources for external sorting.
	 * @throws MemoryAllocationException Thrown, if not enough memory can be obtained from the memory manager to
	 *                                   perform the sort.
	 */
	public UnilateralSortMerger(MemoryManager memoryManager, IOManager ioManager,
			long totalMemory, long maxWriteMem, int numSortBuffers, int maxNumFileHandles,
			Comparator<Key>[] keyComparators, int[] keyPositions, Class<? extends Key>[] keyClasses,
			MutableObjectIterator<PactRecord> input, AbstractInvokable parentTask, float startSpillingFraction)
	throws IOException, MemoryAllocationException
	{
		// sanity checks
		if (memoryManager == null | ioManager == null | keyComparators == null | keyPositions == null | keyClasses == null) {
			throw new NullPointerException();
		}
		if (parentTask == null) {
			throw new NullPointerException("Parent Task must not be null.");
		}
		if (maxNumFileHandles < 2) {
			throw new IllegalArgumentException("Merger cannot work with less than two file handles.");
		}
		if (keyComparators.length < 1) {
			throw new IllegalArgumentException("There must be at least one sort column and hence one comparator.");
		}
		if (keyComparators.length != keyPositions.length || keyPositions.length != keyClasses.length) {
			throw new IllegalArgumentException("The number of comparators, key columns and key types must match.");
		}
		
		if (totalMemory < MIN_SORT_MEM + MIN_WRITE_MEM) {
			throw new IllegalArgumentException("Too little memory provided to Sort-Merger to perform task.");
		}
		
		this.maxNumFileHandles = maxNumFileHandles;
		this.memoryManager = memoryManager;
		this.ioManager = ioManager;
		this.keyComparators = keyComparators;
		this.keyPositions = keyPositions;
		this.keyClasses = keyClasses;
		this.parent = parentTask;
		
		this.memoryToReleaseAtShutdown = new ArrayList<List<MemorySegment>>();
		this.channelsToDeleteAtShutdown = new ArrayList<Channel.ID>();
		this.openChannels = new ArrayList<BlockChannelAccess<?,?>>();
		
		// determine the size of the I/O buffers. the size must be chosen such that we can accommodate
		// the desired number of merges, plus the writing, from the total memory
		if (maxWriteMem != 0)
		{
			if (maxWriteMem != -1 && maxWriteMem < MIN_WRITE_MEM) {
				throw new IllegalArgumentException("The specified maximum write memory is to low. " +
					"Required are at least " + MIN_WRITE_MEM + " bytes.");
			}
			
			// determine how the reading side limits the buffer size, because we buffers for the readers
			// during the merging phase 
			final int minBuffers = NUM_WRITE_BUFFERS + maxNumFileHandles;
			final int desiredBuffers = NUM_WRITE_BUFFERS + 2 * maxNumFileHandles;
			
			int bufferSize = (int) (totalMemory / desiredBuffers);
			if (bufferSize < MIN_IO_BUFFER_SIZE) {
				bufferSize = MIN_IO_BUFFER_SIZE;
				if (totalMemory / minBuffers < MIN_IO_BUFFER_SIZE) {
					maxNumFileHandles = (int) (totalMemory / MIN_IO_BUFFER_SIZE) - NUM_WRITE_BUFFERS;
					if (LOG.isWarnEnabled())
						LOG.warn("Reducing maximal merge fan-in to " + maxNumFileHandles + " due to memory limitations.");
				}
			}
			else {
				bufferSize = Math.min(MAX_IO_BUFFER_SIZE, MathUtils.roundDownToPowerOf2(bufferSize));
			}
			
			if (maxWriteMem < 0) {
				maxWriteMem = Math.max(totalMemory / 64, MIN_WRITE_MEM);
			}
			this.ioBufferSize = Math.min(bufferSize, MathUtils.roundDownToPowerOf2((int) (maxWriteMem / NUM_WRITE_BUFFERS)));
			maxWriteMem = NUM_WRITE_BUFFERS * this.ioBufferSize;
		}
		else {
			// no I/O happening
			this.ioBufferSize = -1;
		}
		
		final long sortMem = totalMemory - maxWriteMem;
		final long numSortMemSegments = sortMem / SORT_MEM_SEGMENT_SIZE;
		
		// decide how many sort buffers to use
		if (numSortBuffers < 1) {
			if (sortMem > 96 * 1024 * 1024) {
				numSortBuffers = 3;
			}
			else if (numSortMemSegments >= 2 * MIN_NUM_SORT_MEM_SEGMENTS) {
				numSortBuffers = 2;
			}
			else {
				numSortBuffers = 1;
			}
		}
		final int numSegmentsPerSortBuffer = numSortMemSegments / numSortBuffers > Integer.MAX_VALUE ? 
						Integer.MAX_VALUE : (int) (numSortMemSegments / numSortBuffers);
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("Instantiating unilateral sort-merger with " + maxWriteMem + " bytes of write cache and " + sortMem + 
				" bytes of sorting/merging memory. Dividing sort memory over " + numSortBuffers + 
				" buffers (" + numSegmentsPerSortBuffer + " pages with " + SORT_MEM_SEGMENT_SIZE + 
				" bytes) , merging maximally " + maxNumFileHandles + " streams at once.");
		}
		
		// circular queues pass buffers between the threads
		final CircularQueues circularQueues = new CircularQueues();
		
		this.sortBuffers = new ArrayList<NormalizedKeySorter<?>>(numSortBuffers);
		final PactRecordAccessors accessors = new PactRecordAccessors(keyPositions, keyClasses);
		
		// allocate the sort buffers and fill empty queue with them
		for (int i = 0; i < numSortBuffers; i++)
		{
			final List<MemorySegment> sortSegments = memoryManager.allocateStrict(parentTask, numSegmentsPerSortBuffer, SORT_MEM_SEGMENT_SIZE);
			final NormalizedKeySorter<PactRecord> buffer = new NormalizedKeySorter<PactRecord>(accessors, sortSegments);
			this.sortBuffers.add(buffer);

			// add to empty queue
			CircularElement element = new CircularElement(i, buffer);
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

		// start the thread that reads the input channels
		this.readThread = getReadingThread(exceptionHandler, input, circularQueues, parentTask,
			((long) (startSpillingFraction * sortMem)));

		// start the thread that sorts the buffers
		this.sortThread = getSortingThread(exceptionHandler, circularQueues, parentTask);

		// start the thread that handles spilling to secondary storage
		this.spillThread = getSpillingThread(exceptionHandler, circularQueues, memoryManager, ioManager,
			sortMem, parentTask);
		
		startThreads();
	}
	
	/**
	 * Starts all the threads that are used by this sort-merger.
	 */
	protected void startThreads()
	{
		// start threads
		this.readThread.start();
		this.sortThread.start();
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
		synchronized(this) {
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
				if (this.iterator == null && this.iteratorException == null) {
					this.iteratorException = new IOException("The sort-merger has been closed.");
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
			// we encapsulate each phase in a try-catch block to ensure that the succeeding phases happen anyways
			
			// PHASE 1: RELEASE ALL MEMORY. This should cause exceptions on all channels, because their memory segments are freed
			try {
				while (!this.memoryToReleaseAtShutdown.isEmpty()) {
					try {
						List<MemorySegment> segments =  this.memoryToReleaseAtShutdown.remove(this.memoryToReleaseAtShutdown.size() - 1);
						this.memoryManager.release(segments);
					}
					catch (Throwable t) {}
				}
				this.memoryToReleaseAtShutdown.clear();
			}
			catch (Throwable t) {}
			
			// PHASE 2: RELEASE ALL SORT BUFFERS. This should cause exceptions while sorting, because the segments are freed
			try {
				// release all sort buffers
				for (NormalizedKeySorter<?> sorter : this.sortBuffers) {
					this.memoryManager.release(sorter.dispose());
				}
				this.sortBuffers.clear();
			}
			catch (Throwable t) {}
			
			// PHASE 3: CLOSE ALL CURRENTLY OPEN CHANNELS
			while (!this.openChannels.isEmpty()) {
				try {
					BlockChannelAccess<?, ?> channel = this.openChannels.remove(this.openChannels.size() - 1);
					if (!channel.isClosed())
						channel.close();
					channel.deleteChannel();
				}
				catch (Throwable t) {}
			}
			
			///PHASE 4: DELETE ALL NON-OPEN CHANNELS
			for (Channel.ID channel : this.channelsToDeleteAtShutdown) {
				try {
					final File f = new File(channel.getPath());
					if (f.exists()) {
						f.delete();
					}
				}
				catch (Throwable t) {}
			}
		}
	}
	
	// ------------------------------------------------------------------------
	//              Cleanup of Temp Files and Allocated Memory
	// ------------------------------------------------------------------------

	/**
	 * Adds a given collection of memory segments to the list of segments that are to be released at shutdown.
	 * 
	 * @param s The collection of memory segments.
	 */
	protected void registerSegmentsToBeFreedAtShutdown(List<MemorySegment> s) {
		this.memoryToReleaseAtShutdown.add(s);
	}

	/**
	 * Removes a given collection of memory segments from the list of segments that are to be released at shutdown.
	 * 
	 * @param s The collection of memory segments.
	 */
	protected void unregisterSegmentsToBeFreedAtShutdown(List<MemorySegment> s) {
		this.memoryToReleaseAtShutdown.remove(s);
	}
	
	/**
	 * Adds a channel to the list of channels that are to be removed at shutdown.
	 * 
	 * @param s The channel id.
	 */
	protected void registerChannelToBeRemovedAtShudown(Channel.ID channel) {
		this.channelsToDeleteAtShutdown.add(channel);
	}

	/**
	 * Removes a channel from the list of channels that are to be removed at shutdown.
	 * 
	 * @param s The channel id.
	 */
	protected void unregisterChannelToBeRemovedAtShudown(Channel.ID channel) {
		this.channelsToDeleteAtShutdown.remove(channel);
	}
	
	/**
	 * Adds a channel reader/writer to the list of channels that are to be removed at shutdown.
	 * 
	 * @param s The channel reader/writer.
	 */
	protected void registerOpenChannelToBeRemovedAtShudown(BlockChannelAccess<?, ?> channel) {
		this.openChannels.add(channel);
	}

	/**
	 * Removes a channel reader/writer from the list of channels that are to be removed at shutdown.
	 * 
	 * @param s The channel reader/writer.
	 */
	protected void unregisterOpenChannelToBeRemovedAtShudown(BlockChannelAccess<?, ?> channel) {
		this.openChannels.remove(channel);
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
	 * @return The thread that reads data from a Nephele reader and puts it into a queue.
	 */
	protected ThreadBase getReadingThread(ExceptionHandler<IOException> exceptionHandler,
			MutableObjectIterator<PactRecord> reader, CircularQueues queues, AbstractInvokable parentTask,
			long startSpillingBytes)
	{
		return new ReadingThread(exceptionHandler, reader, queues, parentTask, startSpillingBytes);
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
	protected ThreadBase getSortingThread(ExceptionHandler<IOException> exceptionHandler, CircularQueues queues,
			AbstractInvokable parentTask)
	{
		return new SortingThread(exceptionHandler, queues, parentTask);
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
	protected ThreadBase getSpillingThread(ExceptionHandler<IOException> exceptionHandler, CircularQueues queues,
			MemoryManager memoryManager, IOManager ioManager, long readMemSize,
			AbstractInvokable parentTask)
	{
		return new SpillingThread(exceptionHandler, queues, memoryManager, ioManager, readMemSize, parentTask);
	}

	// ------------------------------------------------------------------------
	//                           Result Iterator
	// ------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.sort.SortMerger#getIterator()
	 */
	@Override
	public MutableObjectIterator<PactRecord> getIterator() throws InterruptedException
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
	protected final void setResultIterator(MutableObjectIterator<PactRecord> iterator)
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
	protected final MergeIterator getMergingIterator(final List<Channel.ID> channelIDs,
		final List<List<MemorySegment>> inputSegments, List<BlockChannelAccess<?, ?>> readerList)
	throws IOException
	{
		// create one iterator per channel id
		if (LOG.isDebugEnabled())
			LOG.debug("Performing merge of " + channelIDs.size() + " sorted streams.");
		
		final List<ChannelReaderInputView> inViews = new ArrayList<ChannelReaderInputView>(channelIDs.size());
		for (int i = 0; i < channelIDs.size(); i++) {
			final Channel.ID id = channelIDs.get(i);
			final List<MemorySegment> segsForChannel = inputSegments.get(i);
			
			// wrap channel reader as iterator
			final BlockChannelReader reader = this.ioManager.createBlockChannelReader(id);
			readerList.add(reader);
			registerOpenChannelToBeRemovedAtShudown(reader);
			unregisterChannelToBeRemovedAtShudown(id);
			
			final ChannelReaderInputView inView = new ChannelReaderInputView(reader, segsForChannel, false);
			inViews.add(inView);
		}
		
		
		final List<MutableObjectIterator<PactRecord>> iterators = new ArrayList<MutableObjectIterator<PactRecord>>(channelIDs.size());
		for (int i = 0; i < inViews.size(); i++) {
			final ChannelReaderInputView inView = inViews.get(i);
			inView.waitForFirstBlock();
			iterators.add(new ChannelReaderIterator(inView));
		}

		return new MergeIterator(iterators, this.keyComparators, this.keyPositions, this.keyClasses);
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
	protected final List<Channel.ID> mergeChannelList(final List<Channel.ID> channelIDs, 
				final List<MemorySegment> writeBuffers,	final long readMemorySize)
	throws IOException, MemoryAllocationException
	{
		final double numMerges = Math.ceil(channelIDs.size() / ((double) this.maxNumFileHandles));
		final int channelsToMergePerStep = (int) Math.ceil(channelIDs.size() / numMerges);
		
		// allocate the memory for the merging step
		final List<List<MemorySegment>> readBuffers = new ArrayList<List<MemorySegment>>(channelsToMergePerStep);
		final List<MemorySegment> allBuffers = getSegmentsForReaders(readBuffers, readMemorySize, channelsToMergePerStep);
		registerSegmentsToBeFreedAtShutdown(allBuffers);
		
		// the list containing the IDs of the merged channels
		final ArrayList<Channel.ID> mergedChannelIDs = new ArrayList<Channel.ID>((int) (numMerges + 1));
		
		final ArrayList<Channel.ID> channelsToMergeThisStep = new ArrayList<Channel.ID>(channelsToMergePerStep);
		int channelNum = 0;
		while (channelNum < channelIDs.size())
		{
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
		
		// free the memory that was allocated for the readers
		this.memoryManager.release(allBuffers);
		unregisterSegmentsToBeFreedAtShutdown(allBuffers);
		
		return mergedChannelIDs;
	}

	/**
	 * Merges the sorted runs described by the given Channel IDs into a single sorted run. The merging process
	 * uses the given read and write buffers.
	 * 
	 * @param channelIDs The IDs of the runs' channels.
	 * @param readBuffers The buffers for the readers that read the sorted runs.
	 * @param writeBuffers The buffers for the writer that writes the merged channel.
	 * @return The ID of the channel that describes the merged run.
	 */
	protected Channel.ID mergeChannels(List<Channel.ID> channelIDs, List<List<MemorySegment>> readBuffers,
			List<MemorySegment> writeBuffers)
	throws IOException
	{
		// the list with the readers, to be closed at shutdown
		List<BlockChannelAccess<?, ?>> channelAccesses = new ArrayList<BlockChannelAccess<?, ?>>(channelIDs.size());

		// the list with the target iterators
		final MergeIterator mergeIterator = getMergingIterator(channelIDs, readBuffers, channelAccesses);

		// create a new channel writer
		final Channel.ID mergedChannelID = this.ioManager.createChannel();
		final BlockChannelWriter writer = this.ioManager.createBlockChannelWriter(mergedChannelID);
		registerOpenChannelToBeRemovedAtShudown(writer);
		final ChannelWriterOutputView output = new ChannelWriterOutputView(writer, writeBuffers, this.ioBufferSize);

		// read the merged stream and write the data back
		PactRecord rec = new PactRecord();
		while (mergeIterator.next(rec)) {
			rec.write(output);
		}
		output.close();
		
		// register merged result to be removed at shutdown
		unregisterOpenChannelToBeRemovedAtShudown(writer);
		registerChannelToBeRemovedAtShudown(mergedChannelID);
		
		// remove the merged channel readers from the clear-at-shutdown list
		for (int i = 0; i < channelAccesses.size(); i++) {
			BlockChannelAccess<?, ?> access = channelAccesses.get(i);
			if (!access.isClosed())
				access.close();
			access.deleteChannel();
			unregisterOpenChannelToBeRemovedAtShudown(access);
		}

		return mergedChannelID;
	}
	
	/**
	 * Fills the given list with collections of buffers for channels. The list will contain as many collections
	 * as the parameter <code>numReaders</code> specifies.
	 * 
	 * @param target The list into which the lists with buffers for the channels are put.
	 * @param totalReadMemory The total amount of memory to be divided among the channels.
	 * @param numChannels The number of channels for which to allocate buffers. Must not be zero.
	 * @return A list with all memory segments that were allocated.
	 * @throws MemoryAllocationException Thrown, if the specified memory is insufficient to merge the channels
	 *                                   or if the memory manager could not provide the requested memory.
	 */
	protected final List<MemorySegment> getSegmentsForReaders(List<List<MemorySegment>> target,
		long totalReadMemory, int numChannels)
	throws MemoryAllocationException
	{
		// determine the memory to use per channel and the number of buffers
		final int numBuffers = (int) (totalReadMemory / (numChannels * this.ioBufferSize));
		// allocate all buffers in one step, for efficiency
		final List<MemorySegment> memorySegments = this.memoryManager.allocateStrict(this.parent, numBuffers * numChannels, this.ioBufferSize);
		
		// get the buffers for all but the last channel
		for (int i = 0, buffer = 0; i < numChannels - 1; i++) {
			List<MemorySegment> segs = new ArrayList<MemorySegment>(numBuffers);
			target.add(segs);
			for (int k = 0; k < numBuffers; k++, buffer++) {
				segs.add(memorySegments.get(buffer));
			}
		}
		
		// the last channel gets the remaining buffers
		List<MemorySegment> segsForLast = new ArrayList<MemorySegment>(numBuffers);
		target.add(segsForLast);
		for (int i = (numChannels - 1) * numBuffers; i < memorySegments.size(); i++) {
			segsForLast.add(memorySegments.get(i));
		}
		return memorySegments;
	}

	// ------------------------------------------------------------------------
	// Inter-Thread Communication
	// ------------------------------------------------------------------------

	/**
	 * The element that is passed as marker for the end of data.
	 */
	protected static final CircularElement SENTINEL = new CircularElement();

	/**
	 * The element that is passed as marker for signal beginning of spilling.
	 */
	protected static final CircularElement SPILLING_MARKER = new CircularElement();
	
	/**
	 * Class representing buffers that circulate between the reading, sorting and spilling thread.
	 */
	protected static final class CircularElement
	{
		final int id;
		final NormalizedKeySorter<PactRecord> buffer;

		public CircularElement() {
			this.buffer = null;
			this.id = -1;
		}

		public CircularElement(int id, NormalizedKeySorter<PactRecord> buffer) {
			this.id = id;
			this.buffer = buffer;
		}
	}

	/**
	 * Collection of queues that are used for the communication between the threads.
	 */
	protected static final class CircularQueues
	{
		final BlockingQueue<CircularElement> empty;

		final BlockingQueue<CircularElement> sort;

		final BlockingQueue<CircularElement> spill;

		public CircularQueues() {
			this.empty = new LinkedBlockingQueue<CircularElement>();
			this.sort = new LinkedBlockingQueue<CircularElement>();
			this.spill = new LinkedBlockingQueue<CircularElement>();
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
	protected static abstract class ThreadBase extends Thread implements Thread.UncaughtExceptionHandler
	{
		/**
		 * The queue of empty buffer that can be used for reading;
		 */
		protected final CircularQueues queues;

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
		protected ThreadBase(ExceptionHandler<IOException> exceptionHandler, String name, CircularQueues queues,
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
	protected static class ReadingThread extends ThreadBase
	{
		/**
		 * The input channels to read from.
		 */
		private final MutableObjectIterator<PactRecord> reader;
		
		/**
		 * The fraction of the buffers that must be full before the spilling starts.
		 */
		private final long startSpillingBytes;

		/**
		 * Creates a new reading thread.
		 * 
		 * @param exceptionHandler The exception handler to call for all exceptions.
		 * @param reader The reader to pull the data from.
		 * @param queues The queues used to pass buffers between the threads.
		 * @param parentTask The task that started this thread. If non-null, it is used to register this thread.
		 */
		public ReadingThread(ExceptionHandler<IOException> exceptionHandler,
				MutableObjectIterator<PactRecord> reader, CircularQueues queues,
				AbstractInvokable parentTask, long startSpillingBytes)
		{
			super(exceptionHandler, "SortMerger Reading Thread", queues, parentTask);

			// members
			this.reader = reader;
			this.startSpillingBytes = startSpillingBytes;
		}

		/**
		 * The entry point for the thread. Gets a buffer for all threads and then loops as long as there is input
		 * available.
		 */
		public void go() throws IOException
		{	
			final MutableObjectIterator<PactRecord> reader = this.reader;
			
			final PactRecord current = new PactRecord();
			PactRecord leftoverRecord = null;
			
			CircularElement element = null;
			long bytesUntilSpilling = this.startSpillingBytes;
			boolean done = false;
			
			// check if we should directly spill
			if (bytesUntilSpilling < 1) {
				bytesUntilSpilling = 0;
				this.queues.sort.add(SPILLING_MARKER);
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
				final NormalizedKeySorter<PactRecord> buffer = element.buffer;
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
							
							// send the sentinel
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
								// send the sentinel
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
						// send the sentinel
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
			this.queues.sort.add(SENTINEL);
			LOG.debug("Reading thread done.");
		}
	}

	/**
	 * The thread that sorts filled buffers.
	 */
	protected static class SortingThread extends ThreadBase
	{		
		private final IndexedSorter sorter;

		/**
		 * Creates a new sorting thread.
		 * 
		 * @param exceptionHandler The exception handler to call for all exceptions.
		 * @param queues The queues used to pass buffers between the threads.
		 * @param parentTask The task that started this thread. If non-null, it is used to register this thread.
		 */
		public SortingThread(ExceptionHandler<IOException> exceptionHandler, CircularQueues queues,
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
				CircularElement element = null;
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

				if (element != SENTINEL && element != SPILLING_MARKER) {
					if (LOG.isDebugEnabled())
						LOG.debug("Sorting buffer " + element.id + ".");
					
					this.sorter.sort(element.buffer);
					
					if (LOG.isDebugEnabled())
						LOG.debug("Sorted buffer " + element.id + ".");
				}
				else if (element == SENTINEL) {
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
	protected class SpillingThread extends ThreadBase
	{
		protected final MemoryManager memoryManager;	// memory manager for memory allocation and release

		protected final IOManager ioManager;			// I/O manager to create channels
		
		protected final long readMemSize;				// memory for reading and pre-fetching buffers

		/**
		 * Creates the spilling thread.
		 * 
		 * @param exceptionHandler The exception handler to call for all exceptions.
		 * @param queues The queues used to pass buffers between the threads.
		 * @param memoryManager The memory manager used to allocate buffers for the readers and writers.
		 * @param ioManager The I/I manager used to instantiate readers and writers from.
		 * @param writeMemSize The amount of memory to be used for spilling, if spilling is required.
		 * @param readMemSize The amount of memory to be used for the readers that merge the sorted runs.
		 * @param parentTask The task that started this thread. If non-null, it is used to register this thread.
		 */
		public SpillingThread(ExceptionHandler<IOException> exceptionHandler, CircularQueues queues,
				MemoryManager memoryManager, IOManager ioManager,
				long readMemSize, AbstractInvokable parentTask)
		{
			super(exceptionHandler, "SortMerger spilling thread", queues, parentTask);

			// members
			this.memoryManager = memoryManager;
			this.ioManager = ioManager;
			this.readMemSize = readMemSize;
		}

		/**
		 * Entry point of the thread.
		 */
		public void go() throws IOException
		{
			// ------------------- In-Memory Cache ------------------------
			
			final List<CircularElement> cache = new ArrayList<CircularElement>();
			CircularElement element = null;
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
				else if (element == SENTINEL) {
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
				
				List<MutableObjectIterator<PactRecord>> iterators = new ArrayList<MutableObjectIterator<PactRecord>>(cache.size());
								
				// iterate buffers and collect a set of iterators
				for (CircularElement cached : cache)
				{
					// note: the yielded iterator only operates on the buffer heap (and disregards the stack)
					iterators.add(cached.buffer.getIterator());
				}
				
				// release the remaining sort-buffers
				if (LOG.isDebugEnabled())
					LOG.debug("Releasing unused sort-buffer memory.");
				releaseSortBuffers();
				
				// set lazy iterator
				setResultIterator(iterators.size() == 1 ? iterators.get(0) :
					new MergeIterator(iterators, keyComparators, keyPositions, keyClasses));
				return;
			}			
			
			// ------------------- Spilling Phase ------------------------
			
			final Channel.Enumerator enumerator = this.ioManager.createChannelEnumerator();
			final LinkedBlockingQueue<MemorySegment> returnQueue = new LinkedBlockingQueue<MemorySegment>();
			final int writeBufferSize = UnilateralSortMerger.this.ioBufferSize;
			
			List<Channel.ID> channelIDs = new ArrayList<Channel.ID>();
			List<MemorySegment> writeBuffers;

			// allocate memory segments for channel writer
			try {
				writeBuffers = this.memoryManager.allocateStrict(UnilateralSortMerger.this.parent, 
												NUM_WRITE_BUFFERS, writeBufferSize);
				registerSegmentsToBeFreedAtShutdown(writeBuffers);
			}
			catch (MemoryAllocationException maex) {
				throw new IOException("Spilling thread was unable to allocate memory for the channel writer.", maex);
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
					}
					else return;
				}
				
				// check if we are still running
				if (!isRunning()) {
					return;
				}
				// check if this is the end-of-work buffer
				if (element == SENTINEL) {
					break;
				}
				
				// open next channel
				Channel.ID channel = enumerator.next();
				channelIDs.add(channel);

				// create writer
				final BlockChannelWriter writer = this.ioManager.createBlockChannelWriter(channel, returnQueue);
				registerOpenChannelToBeRemovedAtShudown(writer);
				final ChannelWriterOutputView output = new ChannelWriterOutputView(writer, writeBuffers, writeBufferSize);

				// write sort-buffer to channel
				if (LOG.isDebugEnabled())
					LOG.debug("Spilling buffer " + element.id + ".");
				element.buffer.writeToOutput(output);
				if (LOG.isDebugEnabled())
					LOG.debug("Spilled buffer " + element.id + ".");

				output.close();
				unregisterOpenChannelToBeRemovedAtShudown(writer);
				registerChannelToBeRemovedAtShudown(channel);

				// pass empty sort-buffer to reading thread
				element.buffer.reset();
				this.queues.empty.add(element);
			}

			// done with the spilling
			if (LOG.isDebugEnabled()) {
				LOG.debug("Spilling done.");
				LOG.debug("Releasing sort-buffer memory.");
			}
			
			releaseSortBuffers();

			// ------------------- Merging Phase ------------------------
			
			try {
				// merge channels until sufficient file handles are available
				while (channelIDs.size() > UnilateralSortMerger.this.maxNumFileHandles) {
					channelIDs = mergeChannelList(channelIDs, writeBuffers, this.readMemSize);
				}
				
				// from here on, we won't write again
				this.memoryManager.release(writeBuffers);
				unregisterSegmentsToBeFreedAtShutdown(writeBuffers);
				writeBuffers.clear();
				
				// check if we have spilled some data at all
				if (channelIDs.isEmpty()) {
					setResultIterator(EmptyMutableObjectIterator.<PactRecord>get());
				}
				else {
					if (LOG.isDebugEnabled())
						LOG.debug("Beginning final merge.");
					
					// allocate the memory for the final merging step
					List<List<MemorySegment>> readBuffers = new ArrayList<List<MemorySegment>>(channelIDs.size());
					
					// allocate the read memory and register it to be released
					List<MemorySegment> allBuffers = getSegmentsForReaders(readBuffers, this.readMemSize, channelIDs.size());
					registerSegmentsToBeFreedAtShutdown(allBuffers);
					
					// get the readers and register them to be released
					setResultIterator(getMergingIterator(channelIDs, readBuffers, new ArrayList<BlockChannelAccess<?, ?>>(channelIDs.size())));
				}
			}
			catch (MemoryAllocationException maex) {
				throw new IOException("Merging of sorted runs failed, because the memory for the I/O channels could not be allocated.", maex);
			}

			// done
			if (LOG.isDebugEnabled())
				LOG.debug("Spilling and merging thread done.");
		}
		
		/**
		 * Releases the memory that is registered for in-memory sorted run generation.
		 */
		protected final void releaseSortBuffers()
		{
			while (!this.queues.empty.isEmpty()) {
				try {
					final NormalizedKeySorter<?> sorter = this.queues.empty.take().buffer;
					final List<MemorySegment> segments = sorter.dispose();
					this.memoryManager.release(segments);
					UnilateralSortMerger.this.sortBuffers.remove(sorter);
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
		
		protected final CircularElement takeNext(BlockingQueue<CircularElement> queue, List<CircularElement> cache)
		throws InterruptedException
		{
			return cache.isEmpty() ? queue.take() : cache.remove(0);
		}
	}

	/**
	 * This class represents an iterator over a stream produced by a reader.
	 */
	protected static final class ChannelReaderIterator implements MutableObjectIterator<PactRecord>
	{
		private final ChannelReaderInputView input; // the reader from which to get the input

		/**
		 * Creates a new reader iterator.
		 * 
		 * @param input The input from which to read the records.
		 */
		protected ChannelReaderIterator(ChannelReaderInputView input) {
			this.input = input;
		}

		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.runtime.util.ReadingIterator#next(java.lang.Object)
		 */
		@Override
		public boolean next(PactRecord target) throws IOException
		{
			try {
				target.read(this.input);
				return true;
			}
			catch (EOFException eofex) {
				return false;
			}
		}
	}
	

	
	public static final class InputDataCollector implements Collector
	{
		private final CircularQueues queues;		// the queues used to pass buffers
		
		private NormalizedKeySorter<PactRecord> currentBuffer;
		
		private CircularElement currentElement;
		
		private long bytesUntilSpilling;			// number of bytes left before we signal to spill
		
		private boolean spillingInThisBuffer;
		
		private volatile boolean running;
		

		public InputDataCollector(CircularQueues queues, long startSpillingBytes)
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
		public void collect(PactRecord record)
		{
			try {
				if (this.spillingInThisBuffer) {
					if (this.currentBuffer.write(record)) {
						if (this.bytesUntilSpilling - this.currentBuffer.getOccupancy() <= 0) {
							this.bytesUntilSpilling = 0;
							// send the sentinel
							this.queues.sort.add(SPILLING_MARKER);
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
						this.queues.sort.add(SPILLING_MARKER);
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
				
				this.queues.sort.add(SENTINEL);
			}
		}
	}
}