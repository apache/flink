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
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.io.Reader;
import eu.stratosphere.nephele.services.ServiceException;
import eu.stratosphere.nephele.services.iomanager.Channel;
import eu.stratosphere.nephele.services.iomanager.ChannelReader;
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
 * @param <K>
 *        The key class
 * @param <V>
 *        The value class
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

	public CombiningUnilateralSortMerger(ReduceStub<K, V, ?, ?> combineStub, MemoryManager memoryManager,
			IOManager ioManager, int numSortBuffers, int sizeSortBuffer, int ioMemorySize, int maxNumFileHandles,
			SerializationFactory<K> keySerialization, SerializationFactory<V> valueSerialization,
			Comparator<K> keyComparator, Reader<KeyValuePair<K, V>> reader, float offsetArrayPerc,
			AbstractTask parentTask, boolean combineLastMerge)
	throws IOException, MemoryAllocationException
	{
		super(memoryManager, ioManager, numSortBuffers, sizeSortBuffer, ioMemorySize, maxNumFileHandles,
			keySerialization, valueSerialization, keyComparator, reader, offsetArrayPerc, parentTask);

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
			MemoryManager memoryManager, IOManager ioManager, int ioMemorySize, AbstractTask parentTask,
			int buffersToKeepBeforeSpilling)
	{
		return new SpillingThread(exceptionHandler, queues, memoryManager, ioManager, ioMemorySize,
			parentTask, buffersToKeepBeforeSpilling);
	}

	// ------------------------------------------------------------------------
	// Combining & Merging Methods
	// ------------------------------------------------------------------------

	/**
	 * @param channelIDs
	 * @param ioMemorySize
	 * @return The ID of the channel that holds the merged data of all input channels.
	 */
	protected Channel.ID mergeChannels(List<Channel.ID> channelIDs, int ioMemorySize) {
		List<Iterator<KeyValuePair<K, V>>> iterators = new ArrayList<Iterator<KeyValuePair<K, V>>>(channelIDs.size());
		final int ioMemoryPerChannel = ioMemorySize / (channelIDs.size() + 2);

		for (Channel.ID id : channelIDs) {

			Collection<MemorySegment> inputSegments;
			final ChannelReader reader;
			try {
				inputSegments = memoryManager.allocate(1, ioMemoryPerChannel);
				freeSegmentsAtShutdown(inputSegments);

				reader = ioManager.createChannelReader(id, inputSegments);
			} catch (MemoryAllocationException mae) {
				throw new RuntimeException("Could not allocate IO buffers for merge reader", mae);
			} catch (ServiceException se) {
				throw new RuntimeException("Could not open channel reader for merging", se);
			}

			// wrap channel reader as iterator
			final Iterator<KeyValuePair<K, V>> iterator = new KVReaderIterator<K, V>(reader, keySerialization,
				valueSerialization, memoryManager, true);
			iterators.add(iterator);
		}

		MergeIterator<K, V> mi = new MergeIterator<K, V>(iterators, keyComparator);
		KeyGroupedIterator<K, V> groupedIter = new KeyGroupedIterator<K, V>(mi);

		// create a new channel writer
		final Channel.Enumerator enumerator = ioManager.createChannelEnumerator();
		final Channel.ID mergedChannelID = enumerator.next();

		Collection<MemorySegment> outputSegments;
		ChannelWriter writer;
		try {
			outputSegments = memoryManager.allocate(2, ioMemoryPerChannel);
			freeSegmentsAtShutdown(outputSegments);

			writer = ioManager.createChannelWriter(mergedChannelID, outputSegments);
		} catch (MemoryAllocationException mae) {
			throw new RuntimeException("Could not allocate IO Buffer for merge writer", mae);
		} catch (ServiceException se) {
			throw new RuntimeException("Could not open channel writer for merging", se);
		}

		WriterCollector<K, V> collector = new WriterCollector<K, V>(writer);

		while (groupedIter.nextKey()) {
			this.combineStub.combine(groupedIter.getKey(), groupedIter.getValues(), collector);
		}

		// close channel writer
		try {
			outputSegments = writer.close();
		} catch (ServiceException se) {
			throw new RuntimeException("Could not close channel writer", se);
		}

		memoryManager.release(outputSegments);

		return mergedChannelID;
	}

	// ------------------------------------------------------------------------
	// Threads
	// ------------------------------------------------------------------------

	/**
	 * This thread
	 */
	private class SpillingThread extends ThreadBase {
		private final MemoryManager memoryManager;

		private final IOManager ioManager;

		private final int ioMemorySize;
		
		private final int buffersToKeepBeforeSpilling;

		public SpillingThread(ExceptionHandler<IOException> exceptionHandler, CircularQueues queues,
				MemoryManager memoryManager, IOManager ioManager, int ioMemorySize, AbstractTask parentTask,
				int buffersToKeepBeforeSpilling)
		{
			super(exceptionHandler, "SortMerger spilling thread", queues, parentTask);

			// members
			this.memoryManager = memoryManager;
			this.ioManager = ioManager;
			this.ioMemorySize = ioMemorySize;
			this.buffersToKeepBeforeSpilling = buffersToKeepBeforeSpilling;
		}

		/**
		 * Entry point of the thread.
		 */
		public void go() throws Exception {
			final Channel.Enumerator enumerator = ioManager.createChannelEnumerator();
			List<Channel.ID> channelIDs = new ArrayList<Channel.ID>();

			// allocate memory segments for channel writer
			Collection<MemorySegment> outputSegments = memoryManager.allocate(2, ioMemorySize / 2);

			CircularElement element = null;

			// loop as long as the thread is marked alive and we do not see the final
			// element
			while (isRunning() && (element = queues.spill.take()) != SENTINEL) {
				// open next channel
				Channel.ID channel = enumerator.next();
				channelIDs.add(channel);

				// create writer
				LOG.debug("Creating temp file " + channel.toString() + '.');
				ChannelWriter writer = ioManager.createChannelWriter(channel, outputSegments);

				LOG.debug("Combining buffer " + element.id + '.');

				// set up the combining helpers
				final BufferSortable<K, V> buffer = element.buffer;
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

				outputSegments = writer.close();

				// pass empty sort-buffer to reading thread
				element.buffer.reset();
				queues.empty.put(element);
			}

			// if sentinel then set lazy iterator
			LOG.debug("Spilling done.");

			// free output buffers
			LOG.debug("Releasing output-buffer memory.");
			memoryManager.release(outputSegments);

			// release sort-buffers
			LOG.debug("Releasing sort-buffer memory.");
			while (!queues.empty.isEmpty()) {
				memoryManager.release(queues.empty.take().buffer.unbind());
			}

			// merge channels until sufficient file handles are available
			while (channelIDs.size() > maxNumFileHandles) {
				channelIDs = mergeChannelList(channelIDs, ioMemorySize);
			}

			// set the target for the user iterator
			// if the final merge combines, create a combining iterator around the merge iterator,
			// otherwise not
			if (CombiningUnilateralSortMerger.this.combineLastMerge) {
				KeyGroupedIterator<K, V> iter = new KeyGroupedIterator<K, V>(getMergingIterator(channelIDs,
					ioMemorySize));
				lazyIterator.setTarget(new CombiningIterator<K, V>(combineStub, iter));
			} else {
				lazyIterator.setTarget(getMergingIterator(channelIDs, ioMemorySize));
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
	private static final class CombineValueIterator<V extends Value> implements Iterator<V> {
		private final BufferSortable<?, V> buffer; // the buffer from which values are returned

		private int last; // the position of the last value to be returned

		private int position; // the position of the next value to be returned

		/**
		 * Creates an iterator over the values in a <tt>BufferSortable</tt>.
		 * 
		 * @param buffer
		 *        The buffer to get the values from.
		 */
		public CombineValueIterator(BufferSortable<?, V> buffer) {
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
			writer.write(pair);
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
		 * @see
		 * eu.stratosphere.pact.common.stub.Collector#collect(eu.stratosphere.pact.common.type.Key,
		 * eu.stratosphere.pact.common.type.Value)
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
