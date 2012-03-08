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
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.services.iomanager.BlockChannelAccess;
import eu.stratosphere.nephele.services.iomanager.BlockChannelWriter;
import eu.stratosphere.nephele.services.iomanager.Channel;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.io.ChannelWriterOutputView;
import eu.stratosphere.pact.runtime.task.ReduceTask;
import eu.stratosphere.pact.runtime.util.EmptyMutableObjectIterator;
import eu.stratosphere.pact.runtime.util.KeyGroupedIterator;


/**
 * The {@link CombiningUnilateralSortMerger} is part of a merge-sort implementation.
 * The {@link ReduceTask} requires a grouping of the incoming key-value pairs by key. Typically grouping is achieved by
 * determining a total order for the given set of pairs (sorting). Thereafter an iteration over the ordered set is
 * performed and each time the key changes the consecutive objects are united into a new group. Reducers have a combining feature
 * can reduce the data before it is written to disk. In order to implement a combining Reducer, the 
 * {@link eu.stratosphere.pact.common.stub.ReduceStub#combine(Key, Iterator, Collector)} method must be implemented and the ReduceStub 
 * must be annotated with the {@link eu.stratosphere.pact.common.contract.ReduceContract.Combinable} annotation.
 * Conceptually, a merge sort with combining works as follows:
 * (1) Divide the unsorted list into n sublists of about 1/n the size. (2) Sort each sublist recursively by re-applying
 * merge sort. (3) Combine all tuples with the same key within a sublist (4) Merge the two sublists back into one sorted
 * list.
 * Internally, the {@link CombiningUnilateralSortMerger} logic is factored into four threads (read, sort, combine,
 * spill) which communicate through a set of blocking queues (forming a closed loop).
 * Memory is allocated using the {@link MemoryManager} interface. Thus the component will most likely not exceed the
 * user-provided memory limits.
 * 
 * @author Fabian Hueske
 * @author Stephan Ewen
 */
public class CombiningUnilateralSortMerger extends UnilateralSortMerger
{
	// ------------------------------------------------------------------------
	// Constants & Fields
	// ------------------------------------------------------------------------

	/**
	 * Logging.
	 */
	private static final Log LOG = LogFactory.getLog(CombiningUnilateralSortMerger.class);

	/**
	 * The stub called for the combiner.
	 */
	private final ReduceStub combineStub;

	/**
	 * A flag indicating whether the last merge also combines the values.
	 */
	private final boolean combineLastMerge;
	

	// ------------------------------------------------------------------------
	// Constructor
	// ------------------------------------------------------------------------

	/**
	 * Creates a new sorter that reads the data from a given reader and provides an iterator returning that
	 * data in a sorted manner. The memory is divided among sort buffers, write buffers and read buffers
	 * automatically.
	 * 
	 * @param combineStub The stub used to combine values with the same key.
	 * @param memoryManager The memory manager from which to allocate the memory.
	 * @param ioManager The I/O manager, which is used to write temporary files to disk.
	 * @param totalMemory The total amount of memory dedicated to sorting and merging.
	 * @param maxNumFileHandles The maximum number of files to be merged at once.
	 * @param keyComparators The comparator used to define the order among the keys.
	 * @param keyPositions The logical positions of the keys in the records.
	 * @param keyClasses The types of the keys.
	 * @param input The input that is sorted by this sorter.
	 * @param parentTask The parent task, which owns all resources used by this sorter.
	 * @param startSpillingFraction The faction of the buffers that have to be filled before the spilling thread
	 *                              actually begins spilling data to disk.
	 * @param combineLastMerge A flag indicating whether the last merge step applies the combiner as well.
	 * 
	 * @throws IOException Thrown, if an error occurs initializing the resources for external sorting.
	 * @throws MemoryAllocationException Thrown, if not enough memory can be obtained from the memory manager to
	 *                                   perform the sort.
	 */
	public CombiningUnilateralSortMerger(ReduceStub combineStub, MemoryManager memoryManager, IOManager ioManager,
			long totalMemory, int maxNumFileHandles,
			Comparator<Key>[] keyComparators, int[] keyPositions, Class<? extends Key>[] keyClasses,
			MutableObjectIterator<PactRecord> input, AbstractTask parentTask,
			float startSpillingFraction, boolean combineLastMerge)
	throws IOException, MemoryAllocationException
	{
		this (combineStub, memoryManager, ioManager, totalMemory, -1, -1, maxNumFileHandles,
			keyComparators, keyPositions, keyClasses, input, parentTask,
			startSpillingFraction, combineLastMerge);
	}
	
	/**
	 * Creates a new sorter that reads the data from a given reader and provides an iterator returning that
	 * data in a sorted manner. The memory is divided among sort buffers, write buffers and read buffers
	 * automatically.
	 * 
	 * @param combineStub The stub used to combine values with the same key.
	 * @param memoryManager The memory manager from which to allocate the memory.
	 * @param ioManager The I/O manager, which is used to write temporary files to disk.
	 * @param totalMemory The total amount of memory dedicated to sorting, merging and I/O.
	 * @param maxIoMemory The maximal amount of memory to be dedicated to writing sorted runs. Will be subtracted from the total
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
	 * @param combineLastMerge A flag indicating whether the last merge step applies the combiner as well.
	 * 
	 * @throws IOException Thrown, if an error occurs initializing the resources for external sorting.
	 * @throws MemoryAllocationException Thrown, if not enough memory can be obtained from the memory manager to
	 *                                   perform the sort.
	 */
	public CombiningUnilateralSortMerger(ReduceStub combineStub,
			MemoryManager memoryManager, IOManager ioManager,
			long totalMemory, long maxIoMemory, int numSortBuffers, int maxNumFileHandles,
			Comparator<Key>[] keyComparators, int[] keyPositions, Class<? extends Key>[] keyClasses,
			MutableObjectIterator<PactRecord> input, AbstractTask parentTask,
			float startSpillingFraction, boolean combineLastMerge)
	throws IOException, MemoryAllocationException
	{
		super(memoryManager, ioManager, totalMemory, maxIoMemory, numSortBuffers, maxNumFileHandles,
			keyComparators, keyPositions, keyClasses, input, parentTask, startSpillingFraction);

		this.combineStub = combineStub;
		this.combineLastMerge = combineLastMerge;
	}

	// ------------------------------------------------------------------------
	// Factory Methods
	// ------------------------------------------------------------------------

	/**
	 * @param exceptionHandler
	 * @param queues
	 * @param memoryManager
	 * @param ioManager
	 * @param ioMemorySize
	 * @param parentTask
	 * @return A thread that spills data to disk.
	 */
	@Override
	protected ThreadBase getSpillingThread(ExceptionHandler<IOException> exceptionHandler, CircularQueues queues,
			MemoryManager memoryManager, IOManager ioManager, long readMemSize,
			AbstractInvokable parentTask)
	{
		return new CombiningSpillingThread(exceptionHandler, queues, memoryManager, ioManager,
			readMemSize, parentTask);
	}

	// ------------------------------------------------------------------------
	// Combining & Merging Methods
	// ------------------------------------------------------------------------

	/**
	 * Merges the sorted runs described by the given Channel IDs into a single sorted run. The merging process
	 * uses the given read and write buffers. During the merging process, the combiner is used to reduce the
	 * number of values with identical key.
	 * 
	 * @param channelIDs The IDs of the runs' channels.
	 * @param readBuffers The buffers for the readers that read the sorted runs.
	 * @param writeBuffers The buffers for the writer that writes the merged channel.
	 * @return The ID of the channel that describes the merged run.
	 */
	@Override
	protected Channel.ID mergeChannels(List<Channel.ID> channelIDs, List<List<MemorySegment>> readBuffers,
			List<MemorySegment> writeBuffers)
	throws IOException
	{
		// the list with the readers, to be closed at shutdown
		final List<BlockChannelAccess<?, ?>> channelAccesses = new ArrayList<BlockChannelAccess<?, ?>>(channelIDs.size());

		// the list with the target iterators
		final MergeIterator mergeIterator = getMergingIterator(channelIDs, readBuffers, channelAccesses);
		final KeyGroupedIterator groupedIter = new KeyGroupedIterator(mergeIterator, this.keyPositions, this.keyClasses);
		
		// create a new channel writer
		final Channel.ID mergedChannelID = this.ioManager.createChannel();
		final BlockChannelWriter writer = this.ioManager.createBlockChannelWriter(mergedChannelID);
		registerOpenChannelToBeRemovedAtShudown(writer);
		final ChannelWriterOutputView output = new ChannelWriterOutputView(writer, writeBuffers, this.ioBufferSize);
		
		final WriterCollector collector = new WriterCollector(output);
		final ReduceStub combineStub = this.combineStub;
		
		try {
			while (groupedIter.nextKey()) {
				combineStub.combine(groupedIter.getValues(), collector);
			}
		}
		catch (Exception e) {
			throw new IOException("An error occurred in the combiner user code.");
		}
		
		output.close(); //IS VERY IMPORTANT!!!!
		writer.close();
		// register merged result to be removed at shutdown
		unregisterOpenChannelToBeRemovedAtShudown(writer);
		registerChannelToBeRemovedAtShudown(mergedChannelID);

		// remove the merged channel readers from the clear-at-shutdown list
		for (int i = 0; i < channelAccesses.size(); i++) {
			unregisterOpenChannelToBeRemovedAtShudown(channelAccesses.get(i));
		}

		return mergedChannelID;
	}

	// ------------------------------------------------------------------------
	// Threads
	// ------------------------------------------------------------------------

	protected class CombiningSpillingThread extends SpillingThread
	{
		public CombiningSpillingThread(ExceptionHandler<IOException> exceptionHandler, CircularQueues queues,
				MemoryManager memoryManager, IOManager ioManager, long readMemSize,
				AbstractInvokable parentTask)
		{
			super(exceptionHandler, queues, memoryManager, ioManager, readMemSize, parentTask);
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
					else {
						return;
					}
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
				
				List<MutableObjectIterator<PactRecord>> iterators = new ArrayList<MutableObjectIterator<PactRecord>>();
								
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
				MutableObjectIterator<PactRecord> resIter = iterators.size() == 1 ? iterators.get(0) : 
					new MergeIterator(iterators, keyComparators, keyPositions, keyClasses);
				
				if (CombiningUnilateralSortMerger.this.combineLastMerge) {
					KeyGroupedIterator iter = new KeyGroupedIterator(resIter, keyPositions, keyClasses);
					setResultIterator(new CombiningIterator(combineStub, iter));
				} else {
					setResultIterator(resIter);
				}
				return;
			}			
			
			// ------------------- Spilling Phase ------------------------
			
			final Channel.Enumerator enumerator = this.ioManager.createChannelEnumerator();
			final LinkedBlockingQueue<MemorySegment> returnQueue = new LinkedBlockingQueue<MemorySegment>();
			final int writeBufferSize = CombiningUnilateralSortMerger.this.ioBufferSize;
			
			List<Channel.ID> channelIDs = new ArrayList<Channel.ID>();
			List<MemorySegment> writeBuffers;

			// allocate memory segments for channel writer
			try {
				writeBuffers = this.memoryManager.allocateStrict(CombiningUnilateralSortMerger.this.parent, NUM_WRITE_BUFFERS, writeBufferSize);
				registerSegmentsToBeFreedAtShutdown(writeBuffers);
			}
			catch (MemoryAllocationException maex) {
				throw new IOException("Spilling thread was unable to allocate memory for the channel writer.", maex);
			}

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
				final Channel.ID channel = enumerator.next();
				channelIDs.add(channel);

				// create writer
				if (LOG.isDebugEnabled())
					LOG.debug("Creating temp file " + channel.toString() + '.');
				
				// create writer
				final BlockChannelWriter writer = this.ioManager.createBlockChannelWriter(channel, returnQueue);
				registerOpenChannelToBeRemovedAtShudown(writer);
				final ChannelWriterOutputView output = new ChannelWriterOutputView(writer, writeBuffers, writeBufferSize);

				if (LOG.isDebugEnabled())
					LOG.debug("Combining buffer " + element.id + '.');

				// set up the combining helpers
				final NormalizedKeySorter<PactRecord> buffer = element.buffer;
				final CombineValueIterator iter = new CombineValueIterator(buffer);
				final WriterCollector collector = new WriterCollector(output);

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
				if (LOG.isDebugEnabled())
					LOG.debug("Combined and spilled buffer " + element.id + ".");

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

			try {
				// merge channels until sufficient file handles are available
				while (channelIDs.size() > CombiningUnilateralSortMerger.this.maxNumFileHandles) {
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
					// allocate the memory for the final merging step
					List<List<MemorySegment>> readBuffers = new ArrayList<List<MemorySegment>>(channelIDs.size());
					
					// allocate the read memory and register it to be released
					List<MemorySegment> allBuffers = getSegmentsForReaders(readBuffers, this.readMemSize, channelIDs.size());
					registerSegmentsToBeFreedAtShutdown(allBuffers);
					
					final MergeIterator mergeIterator = getMergingIterator(channelIDs, readBuffers, new ArrayList<BlockChannelAccess<?, ?>>(channelIDs.size()));
					
					// set the target for the user iterator
					// if the final merge combines, create a combining iterator around the merge iterator,
					// otherwise not
					if (CombiningUnilateralSortMerger.this.combineLastMerge) {
						KeyGroupedIterator iter = new KeyGroupedIterator(mergeIterator, keyPositions, keyClasses);
						setResultIterator(new CombiningIterator(combineStub, iter));
					} else {
						setResultIterator(mergeIterator);
					}
				}
			}
			catch (MemoryAllocationException maex) {
				throw new IOException("Merging of sorted runs failed, because the memory for the I/O channels could not be allocated.", maex);
			}

			if (LOG.isDebugEnabled())
				LOG.debug("Spilling thread done.");
		}

	} // end spilling/merging thread

	// ------------------------------------------------------------------------

	/**
	 * This class implements an iterator over values from a {@link eu.stratosphere.pact.runtime.sort.BufferSortable}.
	 * The iterator returns the values of a given
	 * interval.
	 */
	private static final class CombineValueIterator implements Iterator<PactRecord>
	{
		private final NormalizedKeySorter<PactRecord> buffer; // the buffer from which values are returned
		
		private final PactRecord record;

		private int last; // the position of the last value to be returned

		private int position; // the position of the next value to be returned

		/**
		 * Creates an iterator over the values in a <tt>BufferSortable</tt>.
		 * 
		 * @param buffer
		 *        The buffer to get the values from.
		 */
		public CombineValueIterator(NormalizedKeySorter<PactRecord> buffer)
		{
			this.buffer = buffer;
			this.record = new PactRecord();
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
		}

		/*
		 * (non-Javadoc)
		 * @see java.util.Iterator#hasNext()
		 */
		@Override
		public boolean hasNext() {
			return this.position <= this.last;
		}

		/*
		 * (non-Javadoc)
		 * @see java.util.Iterator#next()
		 */
		@Override
		public PactRecord next()
		{
			if (this.position <= this.last) {
				try {
					this.buffer.getRecord(this.record, this.position);					
					this.position++;
					return this.record;
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

		/*
		 * (non-Javadoc)
		 * @see java.util.Iterator#remove()
		 */
		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

	};

	// ------------------------------------------------------------------------

	/**
	 * A simple collector that collects Key and Value and writes them into a given <code>Writer</code>.
	 */
	private static final class WriterCollector implements Collector
	{	
		private final ChannelWriterOutputView output; // the writer to write to

		/**
		 * Creates a new writer collector that writes to the given writer.
		 * 
		 * @param output The writer output view to write to.
		 */
		private WriterCollector(ChannelWriterOutputView output) {
			this.output = output;
		}

		/*
		 * (non-Javadoc)
		 * @see
		 * eu.stratosphere.pact.common.stub.Collector#collect(eu.stratosphere.pact.common.type.Key,
		 * eu.stratosphere.pact.common.type.Value)
		 */
		@Override
		public void collect(PactRecord record) {
			
			// DW: Start of temporary code
			this.collectedPactRecordsInBytes += record.getBinaryLength();
			// DW: End of temporary code
			
			try {
				record.write(this.output);
			}
			catch (IOException ioex) {
				throw new RuntimeException("An error occurred forwarding the record to the writer.", ioex);
			}
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stub.Collector#close()
		 */
		@Override
		public void close() {}

		// DW: Start of temporary code
		private long collectedPactRecordsInBytes = 0L;
		
		@Override
		public long getCollectedPactRecordsInBytes() {
			
			final long retVal = this.collectedPactRecordsInBytes;
			this.collectedPactRecordsInBytes = 0L;
			
			return retVal;
		}
		// DW: End of temporary code
	}

	// ------------------------------------------------------------------------

	/**
	 * A simple collector that collects Key and Value and puts them into an <tt>ArrayList</tt>.
	 */
	private static final class ListCollector implements Collector
	{
		private ArrayList<PactRecord> list; // the list to collect pairs in

		/**
		 * Creates a new collector that collects output in the given list.
		 * 
		 * @param list The list to collect output in.
		 */
		private ListCollector(ArrayList<PactRecord> list) {
			this.list = list;
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stub.Collector#collect(eu.stratosphere.pact.common.type.Key, eu.stratosphere.pact.common.type.Value)
		 */
		@Override
		public void collect(PactRecord record) {
			
			// DW: Start of temporary code
			this.collectedPactRecordsInBytes += record.getBinaryLength();
			// DW: End of temporary code
			
			this.list.add(record.createCopy());

		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stub.Collector#close()
		 */
		@Override
		public void close() {
			// does nothing
		}
		
		// DW: Start of temporary code
		private long collectedPactRecordsInBytes = 0L;
		
		@Override
		public long getCollectedPactRecordsInBytes() {
			
			final long retVal = this.collectedPactRecordsInBytes;
			this.collectedPactRecordsInBytes = 0L;
			
			return retVal;
		}
		// DW: End of temporary code
	}

	// ------------------------------------------------------------------------

	private static final class CombiningIterator implements MutableObjectIterator<PactRecord>
	{
		private final ReduceStub combineStub;

		private final KeyGroupedIterator iterator;

		private final ArrayList<PactRecord> results;

		private final ListCollector collector;

		private CombiningIterator(ReduceStub combineStub, KeyGroupedIterator iterator) {
			this.combineStub = combineStub;
			this.iterator = iterator;

			this.results = new ArrayList<PactRecord>();
			this.collector = new ListCollector(this.results);
		}

		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.runtime.util.ReadingIterator#next(java.lang.Object)
		 */
		@Override
		public boolean next(PactRecord target) throws IOException
		{
			try {
				while (this.results.isEmpty() && this.iterator.nextKey()) {
					this.combineStub.combine(this.iterator.getValues(), this.collector);
				}
			}
			catch (Exception ex) {
				throw new RuntimeException("An exception occurred in the combiner user code: " + ex.getMessage(), ex);
			}
			
			if (!this.results.isEmpty()) {
				this.results.remove(0).copyTo(target);
				return true;
			}
			else {
				return false;
			}
		}
	}
}
