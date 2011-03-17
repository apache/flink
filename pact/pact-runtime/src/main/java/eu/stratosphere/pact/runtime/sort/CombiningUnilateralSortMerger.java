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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.io.Reader;
import eu.stratosphere.nephele.services.iomanager.Channel;
import eu.stratosphere.nephele.services.iomanager.ChannelAccess;
import eu.stratosphere.nephele.services.iomanager.ChannelWriter;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.iomanager.SerializationFactory;
import eu.stratosphere.nephele.services.iomanager.Writer;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.runtime.task.ReduceTask;
import eu.stratosphere.pact.runtime.task.util.EmptyIterator;
import eu.stratosphere.pact.runtime.task.util.KeyGroupedIterator;


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
 * 
 * @param <K> The key class
 * @param <V> The value class
 */
public class CombiningUnilateralSortMerger<K extends Key, V extends Value> extends UnilateralSortMerger<K, V> {
	
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
	private final ReduceStub<K, V, ?, ?> combineStub;

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
	 * @param keySerialization The serializer/deserializer for the keys.
	 * @param valueSerialization The serializer/deserializer for the values.
	 * @param keyComparator The comparator used to define the order among the keys.
	 * @param reader The reader from which the input is drawn that will be sorted.
	 * @param parentTask The parent task, which owns all resources used by this sorter.
	 * @param combineLastMerge A flag indicating whether the last merge step applies the combiner as well.
	 * 
	 * @throws IOException Thrown, if an error occurs initializing the resources for external sorting.
	 * @throws MemoryAllocationException Thrown, if not enough memory can be obtained from the memory manager to
	 *                                   perform the sort.
	 */
	public CombiningUnilateralSortMerger(ReduceStub<K, V, ?, ?> combineStub,
			MemoryManager memoryManager, IOManager ioManager,
			long totalMemory, int maxNumFileHandles,
			SerializationFactory<K> keySerialization, SerializationFactory<V> valueSerialization,
			Comparator<K> keyComparator, Reader<KeyValuePair<K, V>> reader,
			AbstractTask parentTask, boolean combineLastMerge)
	throws IOException, MemoryAllocationException
	{
		this (combineStub, memoryManager, ioManager, totalMemory, -1, -1, maxNumFileHandles,
			keySerialization, valueSerialization, keyComparator, reader, parentTask, combineLastMerge);
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
	 * @param ioMemory The amount of memory to be dedicated to writing sorted runs. Will be subtracted from the total
	 *                 amount of memory (<code>totalMemory</code>).
	 * @param numSortBuffers The number of distinct buffers to use creation of the initial runs.
	 * @param maxNumFileHandles The maximum number of files to be merged at once.
	 * @param keySerialization The serializer/deserializer for the keys.
	 * @param valueSerialization The serializer/deserializer for the values.
	 * @param keyComparator The comparator used to define the order among the keys.
	 * @param reader The reader from which the input is drawn that will be sorted.
	 * @param parentTask The parent task, which owns all resources used by this sorter.
	 * @param combineLastMerge A flag indicating whether the last merge step applies the combiner as well.
	 * 
	 * @throws IOException Thrown, if an error occurs initializing the resources for external sorting.
	 * @throws MemoryAllocationException Thrown, if not enough memory can be obtained from the memory manager to
	 *                                   perform the sort.
	 */
	public CombiningUnilateralSortMerger(ReduceStub<K, V, ?, ?> combineStub,
			MemoryManager memoryManager, IOManager ioManager,
			long totalMemory, long ioMemory, int numSortBuffers, int maxNumFileHandles,
			SerializationFactory<K> keySerialization, SerializationFactory<V> valueSerialization,
			Comparator<K> keyComparator, Reader<KeyValuePair<K, V>> reader,
			AbstractTask parentTask, boolean combineLastMerge)
	throws IOException, MemoryAllocationException
	{
		super(memoryManager, ioManager, totalMemory, ioMemory, numSortBuffers, maxNumFileHandles,
			keySerialization, valueSerialization, keyComparator, reader, parentTask);

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
			MemoryManager memoryManager, IOManager ioManager, long writeMemSize, long readMemSize,
			AbstractTask parentTask)
	{
		return new SpillingThread(exceptionHandler, queues, memoryManager, ioManager, writeMemSize, readMemSize,
			parentTask);
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
		final List<ChannelAccess<?>> channelAccesses = new ArrayList<ChannelAccess<?>>(channelIDs.size());
		registerChannelsToBeRemovedAtShudown(channelAccesses);

		// the list with the target iterators
		final Iterator<KeyValuePair<K, V>> mergeIterator = getMergingIterator(channelIDs, readBuffers, channelAccesses);
		final KeyGroupedIterator<K, V> groupedIter = new KeyGroupedIterator<K, V>(mergeIterator);
		
		// create a new channel writer and a collector that uses the writer to dump its data to disk
		final Channel.ID mergedChannelID = this.ioManager.createChannel();
		final ChannelWriter writer = this.ioManager.createChannelWriter(mergedChannelID, writeBuffers);
		channelAccesses.add(writer);
		
		final WriterCollector<K, V> collector = new WriterCollector<K, V>(writer);
		final ReduceStub<K, V, ?, ?> combineStub = this.combineStub;
		
		while (groupedIter.nextKey()) {
			combineStub.combine(groupedIter.getKey(), groupedIter.getValues(), collector);
		}
		writer.close();

		// all readers have finished, so they have closed themselves and deleted themselves
		unregisterChannelsToBeRemovedAtShudown(channelAccesses);

		return mergedChannelID;
	}

	// ------------------------------------------------------------------------
	// Threads
	// ------------------------------------------------------------------------

	/**
	 * This thread
	 */
	private class SpillingThread extends ThreadBase
	{
		private final MemoryManager memoryManager;		// memory manager for memory allocation and release

		private final IOManager ioManager;				// I/O manager to create channels

		private final long writeMemSize;				// memory for output buffers
		
		private final long readMemSize;					// memory for reading and pre-fetching buffers
		

		public SpillingThread(ExceptionHandler<IOException> exceptionHandler, CircularQueues queues,
				MemoryManager memoryManager, IOManager ioManager, long writeMemSize, long readMemSize,
				AbstractTask parentTask)
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
			final Channel.Enumerator enumerator = this.ioManager.createChannelEnumerator();
			final List<MemorySegment> writeBuffers;
			
			List<Channel.ID> channelIDs = new ArrayList<Channel.ID>();

			// allocate memory segments for channel writer
			try {
				writeBuffers = this.memoryManager.allocate(CombiningUnilateralSortMerger.this.parent, writeMemSize, 
					NUM_WRITE_BUFFERS, MIN_IO_BUFFER_SIZE);
				registerSegmentsToBeFreedAtShutdown(writeBuffers);
			}
			catch (MemoryAllocationException maex) {
				throw new IOException("Spilling thread was unable to allocate memory for the channel writer.", maex);
			}
			
			// ------------------- Spilling Phase ------------------------

			// loop as long as the thread is marked alive and we do not see the final element
			while (isRunning()) {
				CircularElement element = null;
				try {
					element = queues.spill.take();
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
				LOG.debug("Creating temp file " + channel.toString() + '.');
				final ChannelWriter writer = ioManager.createChannelWriter(channel, writeBuffers);

				LOG.debug("Combining buffer " + element.id + '.');

				// set up the combining helpers
				final BufferSortableGuaranteed<K, V> buffer = element.buffer;
				final CombineValueIterator<V> iter = new CombineValueIterator<V>(buffer);
				final Collector<K, V> collector = new WriterCollector<K, V>(writer);

				int i = 0;
				int stop = buffer.size() - 1;

				while (i < stop) {
					int seqStart = i;
					while (i < stop && 0 == buffer.compare(i, i + 1)) {
						i++;
					}

					if (i == seqStart) {
						// no duplicate key, no need to combine. simply copy
						buffer.writeToChannel(writer, seqStart, 1);
					} else {
						// get the key and an iterator over the values
						K key = buffer.getKey(seqStart);
						iter.set(seqStart, i);

						// call the combiner to combine
						combineStub.combine(key, iter, collector);
					}
					i++;
				}

				// write the last pair, if it has not yet been included in the last iteration
				if (i == stop) {
					buffer.writeToChannel(writer, stop, 1);
				}

				// done combining and writing out
				LOG.debug("Combined and spilled buffer " + element.id + ".");

				writer.close();

				// pass empty sort-buffer to reading thread
				element.buffer.reset();
				queues.empty.add(element);
			}

			// if sentinel then set lazy iterator
			LOG.debug("Spilling done.");


			// release sort-buffers
			LOG.debug("Releasing sort-buffer memory.");
			while (!queues.empty.isEmpty()) {
				try {
					this.memoryManager.release(queues.empty.take().buffer.unbind());
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
			if (CombiningUnilateralSortMerger.this.sortSegments != null) {
				unregisterSegmentsToBeFreedAtShutdown(CombiningUnilateralSortMerger.this.sortSegments);
				CombiningUnilateralSortMerger.this.sortSegments.clear();
			}
			

			try {
				// merge channels until sufficient file handles are available
				while (channelIDs.size() > CombiningUnilateralSortMerger.this.maxNumFileHandles) {
					channelIDs = mergeChannelList(channelIDs, writeBuffers, this.readMemSize);
				}
				
				// from here on, we won't write again
				this.memoryManager.release(writeBuffers);
				unregisterSegmentsToBeFreedAtShutdown(writeBuffers);

				// check if we have spilled some data at all
				if (channelIDs.isEmpty()) {
					setResultIterator(EmptyIterator.<KeyValuePair<K, V>>get());
				}
				else {
					// allocate the memory for the final merging step
					final List<List<MemorySegment>> readBuffers = new ArrayList<List<MemorySegment>>(channelIDs.size());
					final List<MemorySegment> allBuffers = getSegmentsForReaders(readBuffers, this.readMemSize, channelIDs.size());
					registerSegmentsToBeFreedAtShutdown(allBuffers);
					
					// get the readers and register them to be released
					final List<ChannelAccess<?>> readers = new ArrayList<ChannelAccess<?>>(channelIDs.size());
					registerChannelsToBeRemovedAtShudown(readers);
					
					final Iterator<KeyValuePair<K, V>> mergeIterator = getMergingIterator(channelIDs, readBuffers, readers);
					
					// set the target for the user iterator
					// if the final merge combines, create a combining iterator around the merge iterator,
					// otherwise not
					if (CombiningUnilateralSortMerger.this.combineLastMerge) {
						KeyGroupedIterator<K, V> iter = new KeyGroupedIterator<K, V>(mergeIterator);
						setResultIterator(new CombiningIterator<K, V>(combineStub, iter));
					} else {
						setResultIterator(mergeIterator);
					}
				}
			}
			catch (MemoryAllocationException maex) {
				throw new IOException("Merging of sorted runs failed, because the memory for the I/O channels could not be allocated.", maex);
			}

			LOG.debug("Spilling thread done.");
		}

	} // end spilling/merging thread

	// ------------------------------------------------------------------------

	/**
	 * This class implements an iterator over values from a {@link eu.stratosphere.pact.runtime.sort.BufferSortable}.
	 * The iterator returns the values of a given
	 * interval.
	 */
	private static final class CombineValueIterator<V extends Value> implements Iterator<V>
	{
		private final BufferSortableGuaranteed<?, V> buffer; // the buffer from which values are returned

		private int last; // the position of the last value to be returned

		private int position; // the position of the next value to be returned

		/**
		 * Creates an iterator over the values in a <tt>BufferSortable</tt>.
		 * 
		 * @param buffer
		 *        The buffer to get the values from.
		 */
		public CombineValueIterator(BufferSortableGuaranteed<?, V> buffer) {
			this.buffer = buffer;
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
			return position <= last;
		}

		/*
		 * (non-Javadoc)
		 * @see java.util.Iterator#next()
		 */
		@Override
		public V next() {
			if (position > last) {
				throw new NoSuchElementException();
			}

			try {
				V value = buffer.getValue(position);
				position++;

				return value;
			} catch (IOException ioex) {
				LOG.error("Error retrieving a value from a buffer.", ioex);
				throw new RuntimeException("Could not load the next value: " + ioex.getMessage(), ioex);
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
	private static final class WriterCollector<K extends Key, V extends Value> implements Collector<K, V> {
		
		private final Writer writer; // the writer to write to

		private KeyValuePair<K, V> pair; // the reusable key/value pair

		/**
		 * Creates a new writer collector that writes to the given writer.
		 * 
		 * @param writer
		 *        The writer to write to.
		 */
		private WriterCollector(Writer writer) {
			this.writer = writer;
			this.pair = new KeyValuePair<K, V>();
		}

		/*
		 * (non-Javadoc)
		 * @see
		 * eu.stratosphere.pact.common.stub.Collector#collect(eu.stratosphere.pact.common.type.Key,
		 * eu.stratosphere.pact.common.type.Value)
		 */
		@Override
		public void collect(K key, V value) {
			pair.setKey(key);
			pair.setValue(value);
			
			try {
				writer.write(pair);
			}
			catch (IOException ioex) {
				throw new RuntimeException("An error occurred forwarding the key/value pair to the writer.", ioex);
			}
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stub.Collector#close()
		 */
		@Override
		public void close() {
		}

	}

	// ------------------------------------------------------------------------

	/**
	 * A simple collector that collects Key and Value and puts them into an <tt>ArrayList</tt>.
	 */
	private static final class ListCollector<K extends Key, V extends Value> implements Collector<K, V> {
		private ArrayList<KeyValuePair<K, V>> list; // the list to collect pairs in

		/**
		 * Creates a new collector that collects output in the given list.
		 * 
		 * @param list
		 *        The list to collect output in.
		 */
		private ListCollector(ArrayList<KeyValuePair<K, V>> list) {
			this.list = list;
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stub.Collector#collect(eu.stratosphere.pact.common.type.Key, eu.stratosphere.pact.common.type.Value)
		 */
		@Override
		public void collect(K key, V value) {
			list.add(new KeyValuePair<K, V>(key, value));

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

	private static final class CombiningIterator<K extends Key, V extends Value> implements
			Iterator<KeyValuePair<K, V>> {
		private final ReduceStub<K, V, ?, ?> combineStub;

		private final KeyGroupedIterator<K, V> iterator;

		private final ArrayList<KeyValuePair<K, V>> results;

		private final ListCollector<K, V> collector;

		private CombiningIterator(ReduceStub<K, V, ?, ?> combineStub, KeyGroupedIterator<K, V> iterator) {
			this.combineStub = combineStub;
			this.iterator = iterator;

			this.results = new ArrayList<KeyValuePair<K, V>>();
			this.collector = new ListCollector<K, V>(this.results);
		}

		/*
		 * (non-Javadoc)
		 * @see java.util.Iterator#hasNext()
		 */
		@Override
		public boolean hasNext() {
			while (results.isEmpty() && iterator.nextKey()) {
				combineStub.combine(iterator.getKey(), iterator.getValues(), collector);
			}

			return results.size() > 0;
		}

		/*
		 * (non-Javadoc)
		 * @see java.util.Iterator#next()
		 */
		@Override
		public KeyValuePair<K, V> next() {
			if (!results.isEmpty() || hasNext()) {
				return results.remove(0);
			}

			throw new NoSuchElementException();
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
}
