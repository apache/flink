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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.services.iomanager.Channel;
import eu.stratosphere.nephele.services.iomanager.StreamChannelAccess;
import eu.stratosphere.nephele.services.iomanager.ChannelReader;
import eu.stratosphere.nephele.services.iomanager.ChannelWriter;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.iomanager.RawComparator;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.runtime.util.EmptyMutableObjectIterator;
import eu.stratosphere.pact.runtime.util.MutableObjectIterator;

/**
 * The {@link UnilateralSortMerger} is part of a merge-sort implementation.
 * 
 * Conceptually, a merge sort works as follows:
 * (1) Divide the unsorted list into n sublists of about 1/n the size.
 * (2) Sort each sublist recursively by re-applying merge sort.
 * (3) Merge the two sublists back into one sorted list.
 * 
 * Internally, the {@link UnilateralSortMerger} logic is factored into three threads (read, sort, spill) which
 * communicate through a set of blocking queues (forming a closed loop).
 * Memory is allocated using the {@link MemoryManager} interface. Thus the component will not exceed the
 * user-provided memory limits.
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
	 * A mask that is ANDed to the buffer size to make it a multiple of the minimal buffer size.
	 */
	protected static final long BUFFER_ALIGNMENT_MASK = ~(0x1fffL);

	/**
	 * The minimal size of an IO buffer. Currently set to 8 KiBytes
	 */
	protected static final int MIN_IO_BUFFER_SIZE = 8 * 1024;

	/**
	 * The maximal size of an IO buffer. Currently set to 512 KiBytes
	 */
	protected static final int MAX_IO_BUFFER_SIZE = 512 * 1024;
	
	/**
	 * The minimal size of a sort buffer. Currently set to 2 MiBytes.
	 */
	protected static final int MIN_SORT_BUFFER_SIZE = 2 * 1024 * 1024;

	/**
	 * The number of buffers to use by the writers.
	 */
	protected static final int NUM_WRITE_BUFFERS = 2;

	// ------------------------------------------------------------------------
	//                               Fields
	// ------------------------------------------------------------------------

	/**
	 * This list contains all segments of allocated memory. They will be freed the latest in the
	 * shutdown method. If some segments have been freed before, they will not be freed again.
	 */
	private final List<List<MemorySegment>> memoryToReleaseAtShutdown;
	
	/**
	 * A list of lists containing channel readers and writers that will be closed at shutdown.
	 */
	private final List<List<StreamChannelAccess<?, ?>>> channelsToDeleteAtShutdown;
	
	/**
	 * The segments for the sort buffers.
	 */
	protected final List<MemorySegment> sortSegments;

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
	protected final AbstractTask parent;
	
	/**
	 * The monitor which guards the iterator field.
	 */
	protected final Object iteratorLock = new Object();
	
	/**
	 * The iterator to be returned by the sort-merger. This variable is zero, while receiving and merging is still in
	 * progress and it will be set once we have &lt; merge factor sorted sub-streams that will then be streamed sorted.
	 */
	protected MutableObjectIterator<PactRecord> iterator;
	
	/**
	 * The exception that is set, if the iterator cannot be created.
	 */
	protected IOException iteratorException;

	/**
	 * The maximum number of file handles
	 */
	protected final int maxNumFileHandles;
	
	/**
	 * Flag indicating that the sorter was closed.
	 */
	protected volatile boolean closed;
	
	// ------------------------------------------------------------------------
	// Threads
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
	// Constructor & Shutdown
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
			MutableObjectIterator<PactRecord> input, AbstractTask parentTask, float startSpillingFraction)
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
	 * @param ioMemory The amount of memory to be dedicated to writing sorted runs. Will be subtracted from the total
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
			long totalMemory, long ioMemory, int numSortBuffers, int maxNumFileHandles,
			Comparator<Key>[] keyComparators, int[] keyPositions, Class<? extends Key>[] keyClasses,
			MutableObjectIterator<PactRecord> input, AbstractTask parentTask, float startSpillingFraction)
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
		if (totalMemory < maxNumFileHandles * MIN_IO_BUFFER_SIZE) {
			throw new IOException("Too little memory for merging operations.");
		}
		if (keyComparators.length < 1) {
			throw new IllegalArgumentException("There must be at least one sort column and hence one comparator.");
		}
		if (keyComparators.length != keyPositions.length || keyPositions.length != keyClasses.length) {
			throw new IllegalArgumentException("The number of comparators, key columns and key types must match.");
		}
		
		this.maxNumFileHandles = maxNumFileHandles;
		this.memoryManager = memoryManager;
		this.ioManager = ioManager;
		this.keyComparators = keyComparators;
		this.keyPositions = keyPositions;
		this.keyClasses = keyClasses;
		this.parent = parentTask;
		
		this.memoryToReleaseAtShutdown = new ArrayList<List<MemorySegment>>();
		this.channelsToDeleteAtShutdown = new ArrayList<List<StreamChannelAccess<?, ?>>>();
		
		// circular queues pass buffers between the threads
		final CircularQueues circularQueues = new CircularQueues();
		
		// determine how to spit the memory between the I/O buffers and the sort buffers
		// initially, only the spilling thread needs some I/O buffers for asynchronous writing,
		// the remainder can be dedicated to sort memory
		
		long sortMem;
		
		if (ioMemory < 0) {
			ioMemory = totalMemory / 64;
			ioMemory = Math.max(
					Math.min(ioMemory, NUM_WRITE_BUFFERS * MAX_IO_BUFFER_SIZE),
						NUM_WRITE_BUFFERS * MIN_IO_BUFFER_SIZE);
			ioMemory &= BUFFER_ALIGNMENT_MASK << 1; // align for two buffer sizes
		}
		
		sortMem = totalMemory - ioMemory;
		if (sortMem < MIN_SORT_BUFFER_SIZE) {
			throw new IOException("Too little memory provided to Sort-Merger to perform task.");
		}
		
		// decide how many sort buffers to use
		if (numSortBuffers < 1) {
			if (sortMem > 96 * 1024 * 1024) {
				numSortBuffers = 3;
			}
			else if (sortMem >= 2 * MIN_SORT_BUFFER_SIZE) {
				numSortBuffers = 2;
			}
			else {
				numSortBuffers = 1;
			}
		}
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("Instantiating unilateral sort-merger with " + ioMemory + " bytes of write cache and " + sortMem + 
				" bytes of sorting/merging memory. Dividing sort memory over " + numSortBuffers + 
				" buffers, merging maximally " + maxNumFileHandles + " streams at once.");
		}
		
		
		// allocate the memory for the sort buffers
		final List<MemorySegment> sortSegments = this.memoryManager.allocate(parentTask, sortMem, numSortBuffers, MIN_SORT_BUFFER_SIZE);
		registerSegmentsToBeFreedAtShutdown(sortSegments);
		this.sortSegments = sortSegments;
		
		// fill empty queue with buffers
		for (int i = 0; i < sortSegments.size(); i++) {
			MemorySegment mseg = sortSegments.get(i);

			// comparator
			RawComparator comparator = new DeserializerComparator(keyPositions, keyClasses, keyComparators);

			// sort-buffer
			BufferSortableGuaranteed buffer = new BufferSortableGuaranteed(mseg, comparator);

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
		this.spillThread = getSpillingThread(exceptionHandler, circularQueues, memoryManager, ioManager, ioMemory,
			sortMem, parentTask);
		
		startThreads();
	}
	
	/**
	 * Starts all the threads that are used by this sort-merger.
	 */
	protected void startThreads()
	{
		// start threads
		readThread.start();
		sortThread.start();
		spillThread.start();
	}

	/**
	 * Shuts down all the threads initiated by this sort/merger. Also releases all previously allocated
	 * memory, if it has not yet been released by the threads.
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
			if (readThread != null) {
				try {
					readThread.shutdown();
				} catch (Throwable t) {
					LOG.error("Error shutting down reader thread: " + t.getMessage(), t);
				}
			}
			if (sortThread != null) {
				try {
					sortThread.shutdown();
				} catch (Throwable t) {
					LOG.error("Error shutting down sorter thread: " + t.getMessage(), t);
				}
			}
			if (spillThread != null) {
				try {
					spillThread.shutdown();
				} catch (Throwable t) {
					LOG.error("Error shutting down spilling thread: " + t.getMessage(), t);
				}
			}

			try {
				if (readThread != null) {
					readThread.join();
				}
				
				if (sortThread != null) {
					sortThread.join();
				}
				
				if (spillThread != null) {
					spillThread.join();
				}
			}
			catch (InterruptedException iex) {
				LOG.debug("Closing of sort/merger was interrupted. " +
						"The reading/sorting/spilling threads may still be working.", iex);
			}
		} finally {
			// close all channel accesses
			for (List<StreamChannelAccess<?, ?>> channels : this.channelsToDeleteAtShutdown)
			{
				for (StreamChannelAccess<?, ?> channel : channels) {
					try {
						if (!channel.isClosed()) {
							channel.close();
						}
						channel.deleteChannel();
					}
					catch (Throwable t) {
						// ignore any error at shutdown.
					}
				}
			}
			
			// release all memory
			for (List<MemorySegment> segments: this.memoryToReleaseAtShutdown) {
				memoryManager.release(segments);
			}
			this.memoryToReleaseAtShutdown.clear();
		}
	}

	/**
	 * Adds a given collection of memory segments to the list of segments that are to be released at shutdown.
	 * 
	 * @param s The collection of memory segments.
	 */
	public void registerSegmentsToBeFreedAtShutdown(List<MemorySegment> s) {
		this.memoryToReleaseAtShutdown.add(s);
	}

	/**
	 * Removes a given collection of memory segments from the list of segments that are to be released at shutdown.
	 * 
	 * @param s The collection of memory segments.
	 */
	public void unregisterSegmentsToBeFreedAtShutdown(List<MemorySegment> s) {
		this.memoryToReleaseAtShutdown.remove(s);
	}
	
	/**
	 * Adds a given collection of readers / writers to the list of channels that are to be removed at shutdown.
	 * 
	 * @param s The collection of readers/writers.
	 */
	public void registerChannelsToBeRemovedAtShudown(List<StreamChannelAccess<?, ?>> channels) {
		this.channelsToDeleteAtShutdown.add(channels);
	}

	/**
	 * Removes a given collection of readers / writers from the list of channels that are to be removed at shutdown.
	 * 
	 * @param s The collection of readers/writers.
	 */
	public void unregisterChannelsToBeRemovedAtShudown(List<StreamChannelAccess<?, ?>> channels) {
		this.channelsToDeleteAtShutdown.remove(channels);
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
			MutableObjectIterator<PactRecord> reader, CircularQueues queues, AbstractTask parentTask,
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
			AbstractTask parentTask)
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
	 * @param writeMemSize The amount of memory to be dedicated to writing buffers.
	 * @param readMemSize The amount of memory to be dedicated to reading / pre-fetching buffers. This memory must
	 *                    only be allocated once the sort buffers have been freed.
	 * @param parentTask The task at which the thread registers itself (for profiling purposes).
	 * @return The thread that does the spilling and pre-merging.
	 */
	protected ThreadBase getSpillingThread(ExceptionHandler<IOException> exceptionHandler, CircularQueues queues,
			MemoryManager memoryManager, IOManager ioManager, long writeMemSize, long readMemSize,
			AbstractTask parentTask)
	{
		return new SpillingThread(exceptionHandler, queues, memoryManager, ioManager, writeMemSize,
			readMemSize, parentTask);
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
	protected final void setResultIterator(MutableObjectIterator<PactRecord> iterator) {
		
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
		final List<List<MemorySegment>> inputSegments, List<StreamChannelAccess<?, ?>> readerList)
	throws IOException
	{
		// create one iterator per channel id
		if (LOG.isDebugEnabled())
			LOG.debug("Performing merge of " + channelIDs.size() + " sorted streams.");
		
		final List<MutableObjectIterator<PactRecord>> iterators = new ArrayList<MutableObjectIterator<PactRecord>>(channelIDs.size());
		
		for (int i = 0; i < channelIDs.size(); i++) {
			final Channel.ID id = channelIDs.get(i);
			final List<MemorySegment> segsForChannel = inputSegments.get(i);
			
			// wrap channel reader as iterator
			final ChannelReader reader = ioManager.createChannelReader(id, segsForChannel, true);
			readerList.add(reader);
			
			final MutableObjectIterator<PactRecord> iterator = new ChannelReaderIterator(reader);
			iterators.add(iterator);
		}

		return new MergeIterator(iterators, keyComparators, keyPositions, keyClasses);
	}

	/**
	 * Merges the given sorted runs to a smaller number of sorted runs. 
	 * 
	 * @param channelIDs The IDs of the sorted runs that need to be merged.
	 * @param writeBuffers The buffers to be used by the writers.
	 * @param  memorySize The amount of memory dedicated to the readers.
	 * @return A list of the IDs of the merged channels.
	 * @throws IOException Thrown, if the readers or writers encountered an I/O problem.
	 * @throws MemoryAllocationException Thrown, if the specified memory is insufficient to merge the channels
	 *                                   or if the memory manager could not provide the requested memory.
	 */
	protected final List<Channel.ID> mergeChannelList(final List<Channel.ID> channelIDs, final List<MemorySegment> writeBuffers,
		final long memorySize)
	throws IOException, MemoryAllocationException
	{
		final double numMerges = Math.ceil(channelIDs.size() / ((double) maxNumFileHandles));
		final int channelsToMergePerStep = (int) Math.ceil(channelIDs.size() / numMerges);
		
		if (memorySize < channelsToMergePerStep * MIN_IO_BUFFER_SIZE) {
			throw new MemoryAllocationException("Available memory of " + memorySize + " is not sufficient to merge " + 
				channelsToMergePerStep + " channels.");
		}
		
		// allocate the memory for the merging step
		final List<List<MemorySegment>> readBuffers = new ArrayList<List<MemorySegment>>(channelsToMergePerStep);
		final List<MemorySegment> allBuffers = getSegmentsForReaders(readBuffers, memorySize, channelsToMergePerStep);
		registerSegmentsToBeFreedAtShutdown(allBuffers);
		
		// the list containing the IDs of the merged channels
		final ArrayList<Channel.ID> mergedChannelIDs = new ArrayList<Channel.ID>((int) (numMerges + 1));
		
		final ArrayList<Channel.ID> channelsToMergeThisStep = new ArrayList<Channel.ID>(channelsToMergePerStep);
		int channelNum = 0;
		while (channelNum < channelIDs.size()) {
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
		List<StreamChannelAccess<?, ?>> channelAccesses = new ArrayList<StreamChannelAccess<?, ?>>(channelIDs.size());
		registerChannelsToBeRemovedAtShudown(channelAccesses);

		// the list with the target iterators
		MergeIterator mergeIterator = getMergingIterator(channelIDs, readBuffers, channelAccesses);

		// create a new channel writer
		final Channel.ID mergedChannelID = this.ioManager.createChannel();
		final ChannelWriter writer = this.ioManager.createChannelWriter(mergedChannelID, writeBuffers);
		channelAccesses.add(writer);

		// read the merged stream and write the data back
		PactRecord rec = new PactRecord();
		while ((rec = mergeIterator.next(rec)) != null) {
			// read sorted pairs into memory buffer
			if (!writer.write(rec)) {
				throw new IOException("Writing of pair during merging failed");
			}
		}
		writer.close();
		
		// all readers have finished, so they have closed themselves and deleted themselves
		unregisterChannelsToBeRemovedAtShudown(channelAccesses);

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
		final long ioMemoryPerChannel = totalReadMemory / numChannels;
		final int numBuffers = ioMemoryPerChannel < 2 * MIN_IO_BUFFER_SIZE ? 1 :
			                    ioMemoryPerChannel < 2 * MAX_IO_BUFFER_SIZE ? 2 :
			                    (int) (ioMemoryPerChannel / MAX_IO_BUFFER_SIZE);
		final long bufferSize = (ioMemoryPerChannel / numBuffers) & BUFFER_ALIGNMENT_MASK;
		
		// allocate all buffers in one step, for efficiency
		final List<MemorySegment> memorySegments = this.memoryManager.allocate(this.parent, 
			bufferSize * numBuffers * numChannels, numBuffers * numChannels, MIN_IO_BUFFER_SIZE);
		
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
		final BufferSortableGuaranteed buffer;

		public CircularElement() {
			this.buffer = null;
			this.id = -1;
		}

		public CircularElement(int id, BufferSortableGuaranteed buffer) {
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
	protected static abstract class ThreadBase extends Thread implements Thread.UncaughtExceptionHandler {
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
		private final AbstractTask parentTask;

		/**
		 * The flag marking this thread as alive.
		 */
		private volatile boolean alive;

		/**
		 * Creates a new thread.
		 * 
		 * @param exceptionHandler
		 *        The exception handler to call for all exceptions.
		 * @param name
		 *        The name of the thread.
		 * @param queues
		 *        The queues used to pass buffers between the threads.
		 */
		protected ThreadBase(ExceptionHandler<IOException> exceptionHandler, String name, CircularQueues queues,
				AbstractTask parentTask)
		{
			// thread setup
			super(name);
			this.setDaemon(true);

			// exception handling
			this.exceptionHandler = exceptionHandler;
			this.setUncaughtExceptionHandler(this);

			// queues
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
		 * @throws Exception
		 *         Exceptions that prohibit correct completion of the work may be thrown by the thread.
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
			
			if (exceptionHandler != null) {
				try {
					exceptionHandler.handleException(ioex);
				}
				catch (Throwable t) {}
			}
		}

		/**
		 * {@inheritDoc}
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
	private static class ReadingThread extends ThreadBase
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
		 * @param exceptionHandler
		 *        The exception handler to call for all exceptions.
		 * @param reader
		 *        The reader to pull the data from.
		 * @param queues
		 *        The queues used to pass buffers between the threads.
		 */
		public ReadingThread(ExceptionHandler<IOException> exceptionHandler,
				MutableObjectIterator<PactRecord> reader, CircularQueues queues,
				AbstractTask parentTask, long startSpillingBytes)
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
			
			PactRecord current = new PactRecord();
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
				final BufferSortableGuaranteed buffer = element.buffer;
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
					while (isRunning() && (current = reader.next(current)) != null)
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
				while (isRunning() && (current = reader.next(current)) != null) {
					if (!buffer.write(current)) {
						leftoverRecord = current;
						break;
					}
				}
				
				// check whether the buffer is exhausted or the reader is
				if (current != null || leftoverRecord != null) {
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
	private static class SortingThread extends ThreadBase {
		/**
		 * The sorter.
		 */
		private final IndexedSorter sorter;

		/**
		 * Creates a new sorting thread.
		 * 
		 * @param exceptionHandler
		 *        The exception handler to call for all exceptions.
		 * @param queues
		 *        The queues used to pass buffers between the threads.
		 */
		public SortingThread(ExceptionHandler<IOException> exceptionHandler, CircularQueues queues,
				AbstractTask parentTask) {
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
				CircularElement element = null;
				try {
					element = queues.sort.take();
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
					
					sorter.sort(element.buffer);
					
					if (LOG.isDebugEnabled())
						LOG.debug("Sorted buffer " + element.id + ".");
				}
				else if (element == SENTINEL) {
					if (LOG.isDebugEnabled())
						LOG.debug("Sorting thread done.");
					alive = false;
				}
				queues.spill.add(element);
			}
		}
	}

	/**
	 *
	 */
	private class SpillingThread extends ThreadBase
	{
		private final MemoryManager memoryManager;		// memory manager for memory allocation and release

		private final IOManager ioManager;				// I/O manager to create channels

		private final long writeMemSize;				// memory for output buffers
		
		private final long readMemSize;					// memory for reading and pre-fetching buffers


		public SpillingThread(ExceptionHandler<IOException> exceptionHandler, CircularQueues queues,
				MemoryManager memoryManager, IOManager ioManager,
				long writeMemSize, long readMemSize, AbstractTask parentTask)
		{
			super(exceptionHandler, "SortMerger spilling thread", queues, parentTask);

			// members
			this.memoryManager = memoryManager;
			this.ioManager = ioManager;
			this.writeMemSize = writeMemSize;
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
			while (isRunning())
			{
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
				/* # case 1: operates on in-memory segments only # */
				if (LOG.isDebugEnabled())
					LOG.debug("Initiating merge-iterator (in-memory segments).");
				
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
			final List<MemorySegment> writeBuffers;
			
			List<Channel.ID> channelIDs = new ArrayList<Channel.ID>();

			// allocate memory segments for channel writer
			try {
				writeBuffers = this.memoryManager.allocate(UnilateralSortMerger.this.parent, writeMemSize, 
					NUM_WRITE_BUFFERS, MIN_IO_BUFFER_SIZE);
				registerSegmentsToBeFreedAtShutdown(writeBuffers);
			}
			catch (MemoryAllocationException maex) {
				throw new IOException("Spilling thread was unable to allocate memory for the channel writer.", maex);
			}
			
			// loop as long as the thread is marked alive and we do not see the final element
			while (isRunning())
			{
				try {
					element = takeNext(queues.spill, cache);
				}
				catch (InterruptedException iex) {
					if (isRunning()) {
						LOG.error("Sorting thread was interrupted (without being shut down) while grabbing a buffer. " +
								"Retrying to grab buffer...");
						continue;
					}
					else {
						return;
					}
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
				ChannelWriter writer = this.ioManager.createChannelWriter(channel, writeBuffers);

				// write sort-buffer to channel
				if (LOG.isDebugEnabled())
					LOG.debug("Spilling buffer " + element.id + ".");
				element.buffer.writeToChannel(writer);
				if (LOG.isDebugEnabled())
					LOG.debug("Spilled buffer " + element.id + ".");

				// make sure everything is on disk
				// we do not need to re-collect the buffers, because we still have references to them
				writer.close();

				// pass empty sort-buffer to reading thread
				element.buffer.reset();
				this.queues.empty.add(element);
			}

			// done with the spilling
			if (LOG.isDebugEnabled()) {
				LOG.debug("Spilling done.");
				LOG.debug("Releasing sort-buffer memory.");
			}
			
			// release sort-buffers
			releaseSortBuffers();
			if (UnilateralSortMerger.this.sortSegments != null) {
				unregisterSegmentsToBeFreedAtShutdown(UnilateralSortMerger.this.sortSegments);
				UnilateralSortMerger.this.sortSegments.clear();
			}

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
					List<StreamChannelAccess<?, ?>> readers = new ArrayList<StreamChannelAccess<?, ?>>(channelIDs.size());
					registerChannelsToBeRemovedAtShudown(readers);
					setResultIterator(getMergingIterator(channelIDs, readBuffers, readers));
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
		private void releaseSortBuffers()
		{
			while (!queues.empty.isEmpty()) {
				try {
					MemorySegment segment = queues.empty.take().buffer.unbind();
					UnilateralSortMerger.this.sortSegments.remove(segment);
					memoryManager.release(segment);
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
		
		private CircularElement takeNext(BlockingQueue<CircularElement> queue, List<CircularElement> cache)
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
		private final ChannelReader reader; // the reader from which to get the input

		/**
		 * Creates a new reader iterator.
		 * 
		 * @param reader The reader from which to read the keys and values.
		 */
		protected ChannelReaderIterator(ChannelReader reader) {
			this.reader = reader;
		}

		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.runtime.util.ReadingIterator#next(java.lang.Object)
		 */
		@Override
		public PactRecord next(PactRecord target) throws IOException {
			return this.reader.read(target) ? target : null;
		}

	}
}
