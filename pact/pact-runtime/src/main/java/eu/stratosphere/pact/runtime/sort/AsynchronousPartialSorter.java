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

import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.MutableObjectIterator;


/**
 * The {@link AsynchronousPartialSorter} is a simple sort implementation that sorts
 * bulks inside its buffers, and returns them directly, without merging them. Therefore,
 * it establishes an order within certain windows, but not across them.
 * 
 * @author Fabian Hueske
 * @author Stephan Ewen
 */
public class AsynchronousPartialSorter extends UnilateralSortMerger
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
	 * @param keyComparators The comparator used to define the order among the keys.
	 * @param keyPositions The logical positions of the keys in the records.
	 * @param keyClasses The types of the keys.
	 * @param input The input that is sorted by this sorter.
	 * @param parentTask The parent task, which owns all resources used by this sorter.
	 * 
	 * @throws IOException Thrown, if an error occurs initializing the resources for external sorting.
	 * @throws MemoryAllocationException Thrown, if not enough memory can be obtained from the memory manager to
	 *                                   perform the sort.
	 */
	public AsynchronousPartialSorter(
			MemoryManager memoryManager, IOManager ioManager, long totalMemory,
			Comparator<Key>[] keyComparators, int[] keyPositions, Class<? extends Key>[] keyClasses,
			MutableObjectIterator<PactRecord> input, AbstractTask parentTask)
	throws IOException, MemoryAllocationException
	{
		super(memoryManager, ioManager, totalMemory, 
			0,
			totalMemory < 2 * MIN_SORT_MEM ? 1 : Math.max((int) Math.ceil(totalMemory / (64.0 * 1024 * 1024)), 2),
			2, keyComparators, keyPositions, keyClasses, input, parentTask, 0.0f);
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
			AbstractInvokable parentTask)
	{
		this.bufferIterator = new BufferQueueIterator(queues);
		setResultIterator(this.bufferIterator);
		
		return new SpillingThread(exceptionHandler, queues, memoryManager, ioManager,
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
				long writeMemSize, long readMemSize,
				AbstractInvokable parentTask)
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
	private final class BufferQueueIterator implements MutableObjectIterator<PactRecord>
	{
		private final CircularQueues queues;
		
		private CircularElement currentElement;
		
		private MutableObjectIterator<PactRecord> currentIterator;
		
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
		public boolean next(PactRecord target) throws IOException
		{
			if (this.currentIterator != null && this.currentIterator.next(target)) {
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
					if (this.currentIterator.next(target)) {
						return true;
					}
					this.currentIterator = null;
				}
			}
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
