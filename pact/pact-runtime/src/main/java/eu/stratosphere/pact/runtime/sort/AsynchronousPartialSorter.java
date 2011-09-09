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
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

import eu.stratosphere.nephele.io.Reader;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.iomanager.SerializationFactory;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;


/**
 * The {@link AsynchronousPartialSorter} is a simple sort implementation that sorts
 * bulks inside its buffers, and returns them directly, without merging them. Therefore,
 * it establishes an order within certain windows, but not across them.
 * 
 * @author Fabian Hueske
 * @author Stephan Ewen
 * 
 * @param <K> The key class
 * @param <V> The value class
 */
public class AsynchronousPartialSorter<K extends Key, V extends Value> extends UnilateralSortMerger<K, V>
{
	
	private BufferQueueIterator bufferIterator;
	
	// ------------------------------------------------------------------------
	// Constructor
	// ------------------------------------------------------------------------
	
	/**
	 * 
	 * 
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
	 * 
	 * @throws IOException Thrown, if an error occurs initializing the resources for external sorting.
	 * @throws MemoryAllocationException Thrown, if not enough memory can be obtained from the memory manager to
	 *                                   perform the sort.
	 */
	public AsynchronousPartialSorter(MemoryManager memoryManager, IOManager ioManager,
			long totalMemory,
			SerializationFactory<K> keySerialization, SerializationFactory<V> valueSerialization,
			Comparator<K> keyComparator, Reader<KeyValuePair<K, V>> reader,
			AbstractTask parentTask)
	throws IOException, MemoryAllocationException
	{
		super(memoryManager, ioManager, totalMemory, 
			0,
			totalMemory < 2 * MIN_SORT_BUFFER_SIZE ? 1 : Math.max((int) Math.ceil(totalMemory / (64.0 * 1024 * 1024)), 2),
			2,
			keySerialization, valueSerialization, keyComparator, reader, parentTask, 0.0f);
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
		this.bufferIterator = new BufferQueueIterator(queues);
		setResultIterator(this.bufferIterator);
		
		return new SpillingThread(exceptionHandler, queues, memoryManager, ioManager,
			this.keySerialization, this.valueSerialization,
			writeMemSize, readMemSize,
			parentTask);
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.sort.UnilateralSortMerger#close()
	 */
	public void close()
	{
		// make a best effort to close the buffer iterator
		try {
			if (this.bufferIterator != null) {
				this.bufferIterator.close();
				this.bufferIterator = null;
			}
		}
		finally {
			super.close();
		}
	}

	// ------------------------------------------------------------------------
	// Threads
	// ------------------------------------------------------------------------

	/**
	 * This thread
	 */
	private class SpillingThread extends ThreadBase
	{
		public SpillingThread(ExceptionHandler<IOException> exceptionHandler, CircularQueues queues,
				MemoryManager memoryManager, IOManager ioManager,
				SerializationFactory<K> keySerializer, SerializationFactory<V> valSerializer,
				long writeMemSize, long readMemSize,
				AbstractTask parentTask)
		{
			super(exceptionHandler, "Partial Sorter Iterator Thread.", queues, parentTask);
		}

		/**
		 * Entry point of the thread. Does Nothing, since the thread does not spill.
		 */
		public void go() throws IOException
		{}
		
	} // end spilling thread

	// ------------------------------------------------------------------------

	/**
	 * This class implements an iterator over values from a {@link eu.stratosphere.pact.runtime.sort.BufferSortable}.
	 * The iterator returns the values of a given
	 * interval.
	 */
	private final class BufferQueueIterator implements Iterator<KeyValuePair<K, V>>
	{
		private final CircularQueues queues;
		
		private CircularElement currentElement;
		
		private Iterator<KeyValuePair<K, V>> currentIterator;
		
		private volatile boolean closed = false;

		/**
		 * Creates an iterator over the values in a <tt>BufferSortable</tt>.
		 * 
		 * @param buffer
		 *        The buffer to get the values from.
		 */
		protected BufferQueueIterator(CircularQueues queues) {
			this.queues = queues;
		}

		/*
		 * (non-Javadoc)
		 * @see java.util.Iterator#hasNext()
		 */
		@Override
		public boolean hasNext() {
			if (this.currentIterator != null &&  this.currentIterator.hasNext()) {
				return true;
			}
			else if (this.closed) {
				throw new IllegalStateException();
			}
			else {
				while (true) {
					if (this.currentElement == SENTINEL) {
						return false;
					}
					else if (this.currentElement != null) {
						// return the current element to the empty queue
						this.currentElement.buffer.reset();
						this.queues.empty.add(this.currentElement);
					}
					
					// get a new element
					try {
						this.currentElement = queues.spill.take();
						if (this.currentElement == SENTINEL) {
							return false;
						}
						if (this.currentElement == SPILLING_MARKER) {
							this.currentElement = null;
							continue;
						}
					}
					catch (InterruptedException e) {
						throw new RuntimeException("Iterator was interrupted getting the next sortedBuffer.");
					}
					
					this.currentIterator = this.currentElement.buffer.getIterator();
					if (this.currentIterator.hasNext()) {
						return true;
					}
					this.currentIterator = null;
				}
			}
		}

		/*
		 * (non-Javadoc)
		 * @see java.util.Iterator#next()
		 */
		@Override
		public KeyValuePair<K, V> next()
		{
			if (hasNext()) {
				return this.currentIterator.next();
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
		
		public void close() {
			synchronized (this) {
				if (this.closed) {
					return;
				}
				this.closed = true;
			}
			
			if (this.currentElement != null) {
				this.queues.empty.add(this.currentElement);
				this.currentElement = null;
			}
			if (this.currentIterator != null) {
				this.currentIterator = null;
			}
		}

	};
}
