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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.services.iomanager.BlockChannelAccess;
import eu.stratosphere.nephele.services.iomanager.BlockChannelWriter;
import eu.stratosphere.nephele.services.iomanager.Channel;
import eu.stratosphere.nephele.services.iomanager.ChannelWriterOutputView;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.common.generic.GenericReducer;
import eu.stratosphere.pact.common.generic.types.TypeComparator;
import eu.stratosphere.pact.common.generic.types.TypeSerializer;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.task.ReduceDriver;
import eu.stratosphere.pact.runtime.util.EmptyMutableObjectIterator;
import eu.stratosphere.pact.runtime.util.KeyGroupedIterator;


/**
 * The {@link CombiningUnilateralSortMerger} is part of a merge-sort implementation.
 * The {@link ReduceDriver} requires a grouping of the incoming key-value pairs by key. Typically grouping is achieved by
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
 * @author Stephan Ewen
 * @author Fabian Hueske
 */
public class CombiningUnilateralSortMerger<E> extends UnilateralSortMerger<E>
{
	// ------------------------------------------------------------------------
	// Constants & Fields
	// ------------------------------------------------------------------------

	/**
	 * Logging.
	 */
	private static final Log LOG = LogFactory.getLog(CombiningUnilateralSortMerger.class);

	private final GenericReducer<E, ?> combineStub;	// the user code stub that does the combining
	
	private final boolean combineLastMerge;			// Flag indicating whether the last merge also combines the values.
	

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
	 * @param serializer The type serializer.
	 * @param comparator The type comparator establishing the order relation.
	 * @param totalMemory The total amount of memory dedicated to sorting, merging and I/O.
	 * @param maxNumFileHandles The maximum number of files to be merged at once.
	 * @param startSpillingFraction The faction of the buffers that have to be filled before the spilling thread
	 *                              actually begins spilling data to disk.
	 * @param combineLastMerge A flag indicating whether the last merge step applies the combiner as well.
	 * 
	 * @throws IOException Thrown, if an error occurs initializing the resources for external sorting.
	 * @throws MemoryAllocationException Thrown, if not enough memory can be obtained from the memory manager to
	 *                                   perform the sort.
	 */
	public CombiningUnilateralSortMerger(GenericReducer<E, ?> combineStub, MemoryManager memoryManager, IOManager ioManager,
			MutableObjectIterator<E> input, AbstractInvokable parentTask, 
			TypeSerializer<E> serializer, TypeComparator<E> comparator,
			long totalMemory, int maxNumFileHandles, float startSpillingFraction, boolean combineLastMerge)
	throws IOException, MemoryAllocationException
	{
		this(combineStub, memoryManager, ioManager, input, parentTask, serializer, comparator,
			totalMemory, -1, maxNumFileHandles, startSpillingFraction, combineLastMerge);
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
	 * @param serializer The type serializer.
	 * @param comparator The type comparator establishing the order relation.
	 * @param totalMemory The total amount of memory dedicated to sorting, merging and I/O.
	 * @param numSortBuffers The number of distinct buffers to use creation of the initial runs.
	 * @param maxNumFileHandles The maximum number of files to be merged at once.
	 * @param startSpillingFraction The faction of the buffers that have to be filled before the spilling thread
	 *                              actually begins spilling data to disk.
	 * @param combineLastMerge A flag indicating whether the last merge step applies the combiner as well.
	 * 
	 * @throws IOException Thrown, if an error occurs initializing the resources for external sorting.
	 * @throws MemoryAllocationException Thrown, if not enough memory can be obtained from the memory manager to
	 *                                   perform the sort.
	 */
	public CombiningUnilateralSortMerger(GenericReducer<E, ?> combineStub, MemoryManager memoryManager, IOManager ioManager,
			MutableObjectIterator<E> input, AbstractInvokable parentTask, 
			TypeSerializer<E> serializer, TypeComparator<E> comparator,
			long totalMemory, int numSortBuffers, int maxNumFileHandles, 
			float startSpillingFraction, boolean combineLastMerge)
	throws IOException, MemoryAllocationException
	{
		super(memoryManager, ioManager, input, parentTask, serializer, comparator,
			totalMemory, numSortBuffers, maxNumFileHandles, startSpillingFraction, false);
		
		this.combineStub = combineStub;
		this.combineLastMerge = combineLastMerge;
	}

	// ------------------------------------------------------------------------
	// Factory Methods
	// ------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.sort.UnilateralSortMerger#getSpillingThread(eu.stratosphere.pact.runtime.sort.ExceptionHandler, eu.stratosphere.pact.runtime.sort.UnilateralSortMerger.CircularQueues, eu.stratosphere.nephele.template.AbstractInvokable, eu.stratosphere.nephele.services.memorymanager.MemoryManager, eu.stratosphere.nephele.services.iomanager.IOManager, eu.stratosphere.pact.runtime.plugable.TypeSerializer, eu.stratosphere.pact.runtime.plugable.TypeComparator, java.util.List, java.util.List, int)
	 */
	@Override
	protected ThreadBase<E> getSpillingThread(ExceptionHandler<IOException> exceptionHandler, CircularQueues<E> queues,
		AbstractInvokable parentTask, MemoryManager memoryManager, IOManager ioManager, 
		TypeSerializer<E> serializer, TypeComparator<E> comparator,
		List<MemorySegment> sortReadMemory, List<MemorySegment> writeMemory, int maxFileHandles)
	{
		return new CombiningSpillingThread(exceptionHandler, queues, parentTask,
			memoryManager, ioManager, serializer, comparator, sortReadMemory, writeMemory, maxFileHandles);
	}

	// ------------------------------------------------------------------------
	// Threads
	// ------------------------------------------------------------------------

	protected class CombiningSpillingThread extends SpillingThread
	{		
		private final TypeComparator<E> comparator2;
		
		public CombiningSpillingThread(ExceptionHandler<IOException> exceptionHandler, CircularQueues<E> queues,
				AbstractInvokable parentTask, MemoryManager memManager, IOManager ioManager, 
				TypeSerializer<E> serializer, TypeComparator<E> comparator, 
				List<MemorySegment> sortReadMemory, List<MemorySegment> writeMemory, int maxNumFileHandles)
		{
			super(exceptionHandler, queues, parentTask, memManager, ioManager, serializer, comparator, 
				sortReadMemory, writeMemory, maxNumFileHandles);
			
			this.comparator2 = comparator.duplicate();
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
				if (element == spillingMarker()) {
					break;
				}
				else if (element == endMarker()) {
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
				MutableObjectIterator<E> resIter = iterators.isEmpty() ? EmptyMutableObjectIterator.<E>get() :
						iterators.size() == 1 ? iterators.get(0) : 
						new MergeIterator<E>(iterators,	this.serializer, this.comparator);
				
				if (CombiningUnilateralSortMerger.this.combineLastMerge) {
					KeyGroupedIterator<E> iter = new KeyGroupedIterator<E>(resIter, this.serializer, this.comparator2);
					setResultIterator(new CombiningIterator<E>(CombiningUnilateralSortMerger.this.combineStub, iter, this.serializer));
				} else {
					setResultIterator(resIter);
				}
				
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
				if (element == endMarker()) {
					break;
				}
				
				// open next channel
				Channel.ID channel = enumerator.next();
				registerChannelToBeRemovedAtShudown(channel);
				
				if (LOG.isDebugEnabled())
					LOG.debug("Creating temp file " + channel.toString() + '.');

				// create writer
				final BlockChannelWriter writer = this.ioManager.createBlockChannelWriter(
																channel, this.numWriteBuffersToCluster);
				registerOpenChannelToBeRemovedAtShudown(writer);
				final ChannelWriterOutputView output = new ChannelWriterOutputView(writer, this.writeMemory,
																			this.memManager.getPageSize());

				// write sort-buffer to channel
				if (LOG.isDebugEnabled())
					LOG.debug("Combining buffer " + element.id + '.');

				// set up the combining helpers
				final NormalizedKeySorter<E> buffer = element.buffer;
				final CombineValueIterator<E> iter = new CombineValueIterator<E>(buffer, this.serializer.createInstance());
				final WriterCollector<E> collector = new WriterCollector<E>(output, this.serializer);
				
				final GenericReducer<E, ?> combineStub = CombiningUnilateralSortMerger.this.combineStub;

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
				final MergeIterator<E> mergeIterator = getMergingIterator(
						channelIDs, readBuffers, new ArrayList<BlockChannelAccess<?, ?>>(channelIDs.size()));
				
				// set the target for the user iterator
				// if the final merge combines, create a combining iterator around the merge iterator,
				// otherwise not
				if (CombiningUnilateralSortMerger.this.combineLastMerge) {
					KeyGroupedIterator<E> iter = new KeyGroupedIterator<E>(mergeIterator, this.serializer, this.comparator2);
					setResultIterator(new CombiningIterator<E>(CombiningUnilateralSortMerger.this.combineStub, iter, this.serializer));
				} else {
					setResultIterator(mergeIterator);
				}
			}

			// done
			if (LOG.isDebugEnabled())
				LOG.debug("Spilling and merging thread done.");
		}
		
		// ------------------ Combining & Merging Methods -----------------

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
		protected ChannelWithBlockCount mergeChannels(List<ChannelWithBlockCount> channelIDs, List<List<MemorySegment>> readBuffers,
				List<MemorySegment> writeBuffers)
		throws IOException
		{
			// the list with the readers, to be closed at shutdown
			final List<BlockChannelAccess<?, ?>> channelAccesses = new ArrayList<BlockChannelAccess<?, ?>>(channelIDs.size());

			// the list with the target iterators
			final MergeIterator<E> mergeIterator = getMergingIterator(channelIDs, readBuffers, channelAccesses);
			final KeyGroupedIterator<E> groupedIter = new KeyGroupedIterator<E>(mergeIterator, this.serializer, this.comparator2);

			// create a new channel writer
			final Channel.ID mergedChannelID = this.ioManager.createChannel();
			registerChannelToBeRemovedAtShudown(mergedChannelID);
			final BlockChannelWriter writer = this.ioManager.createBlockChannelWriter(
															mergedChannelID, this.numWriteBuffersToCluster);
			registerOpenChannelToBeRemovedAtShudown(writer);
			final ChannelWriterOutputView output = new ChannelWriterOutputView(writer, writeBuffers, 
																			this.memManager.getPageSize());
			
			final WriterCollector<E> collector = new WriterCollector<E>(output, this.serializer);
			final GenericReducer<E, ?> combineStub = CombiningUnilateralSortMerger.this.combineStub;

			// combine and write to disk
			try {
				while (groupedIter.nextKey()) {
					combineStub.combine(groupedIter.getValues(), collector);
				}
			}
			catch (Exception e) {
				throw new IOException("An error occurred in the combiner user code.");
			}
			output.close(); //IS VERY IMPORTANT!!!!
			
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

	} // end spilling/merging thread

	// ------------------------------------------------------------------------

	/**
	 * This class implements an iterator over values from a sort buffer. The iterator returns the values of a given
	 * interval.
	 */
	private static final class CombineValueIterator<E> implements Iterator<E>
	{
		private final NormalizedKeySorter<E> buffer; // the buffer from which values are returned
		
		private final E record;

		private int last; // the position of the last value to be returned

		private int position; // the position of the next value to be returned

		/**
		 * Creates an iterator over the values in a <tt>BufferSortable</tt>.
		 * 
		 * @param buffer
		 *        The buffer to get the values from.
		 */
		public CombineValueIterator(NormalizedKeySorter<E> buffer, E instance)
		{
			this.buffer = buffer;
			this.record = instance;
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
		public E next()
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

	}

	// ------------------------------------------------------------------------

	/**
	 * A simple collector that collects Key and Value and writes them into a given <code>Writer</code>.
	 */
	private static final class WriterCollector<E> implements Collector<E>
	{	
		private final ChannelWriterOutputView output; // the writer to write to
		
		private final TypeSerializer<E> serializer;

		/**
		 * Creates a new writer collector that writes to the given writer.
		 * 
		 * @param output The writer output view to write to.
		 */
		private WriterCollector(ChannelWriterOutputView output, TypeSerializer<E> serializer) {
			this.output = output;
			this.serializer = serializer;
		}

		/*
		 * (non-Javadoc)
		 * @see
		 * eu.stratosphere.pact.common.stub.Collector#collect(eu.stratosphere.pact.common.type.Key,
		 * eu.stratosphere.pact.common.type.Value)
		 */
		@Override
		public void collect(E record) {
			try {
				this.serializer.serialize(record, this.output);
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

	}

	// ------------------------------------------------------------------------

	/**
	 * A simple collector that collects Key and Value and puts them into an <tt>ArrayList</tt>.
	 */
	private static final class ListCollector<E> implements Collector<E>
	{
		private final ArrayDeque<E> list; // the list to collect pairs in
		
		private final TypeSerializer<E> serializer; // the serializer that creates copies

		/**
		 * Creates a new collector that collects output in the given list.
		 * 
		 * @param list The list to collect output in.
		 */
		private ListCollector(ArrayDeque<E> list, TypeSerializer<E> serializer) {
			this.list = list;
			this.serializer = serializer;
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stub.Collector#collect(eu.stratosphere.pact.common.type.Key, eu.stratosphere.pact.common.type.Value)
		 */
		@Override
		public void collect(E record) {
			this.list.add(this.serializer.createCopy(record));

		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stub.Collector#close()
		 */
		@Override
		public void close() {
			// does nothing
		}
	}

	// ------------------------------------------------------------------------

	private static final class CombiningIterator<E> implements MutableObjectIterator<E>
	{
		private final GenericReducer<E, ?> combineStub;

		private final KeyGroupedIterator<E> iterator;

		private final ArrayDeque<E> results;

		private final ListCollector<E> collector;
		
		private final TypeSerializer<E> serializer;

		private CombiningIterator(GenericReducer<E, ?> combineStub, KeyGroupedIterator<E> iterator, TypeSerializer<E> serializer)
		{
			this.combineStub = combineStub;
			this.iterator = iterator;
			this.serializer = serializer;
			
			this.results = new ArrayDeque<E>();
			this.collector = new ListCollector<E>(this.results, serializer);
		}

		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.runtime.util.ReadingIterator#next(java.lang.Object)
		 */
		@Override
		public boolean next(E target) throws IOException
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
				this.serializer.copyTo(this.results.poll(), target);
				return true;
			}
			else {
				return false;
			}
		}
	}
}
