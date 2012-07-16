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
import java.util.List;
import java.util.concurrent.TimeUnit;

import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.common.generic.types.TypeComparator;
import eu.stratosphere.pact.common.generic.types.TypeSerializer;
import eu.stratosphere.pact.common.util.MutableObjectIterator;


/**
 * The {@link AsynchronousPartialSorter} is a simple sort implementation that sorts
 * bulks inside its buffers, and returns them directly, without merging them. Therefore,
 * it establishes an order within certain windows, but not across them.
 * 
 * @author Fabian Hueske
 * @author Stephan Ewen
 */
public class AsynchronousPartialSorter<E> extends UnilateralSortMerger<E>
{
	private static final int MAX_MEM_PER_PARTIAL_SORT = 64 * 1024 * 0124;
	
	private BufferQueueIterator bufferIterator;
	
	// ------------------------------------------------------------------------
	// Constructor
	// ------------------------------------------------------------------------
	
	/**
	 * 
	 * 
	 * @param memoryManager The memory manager from which to allocate the memory.
	 * @param input The input that is sorted by this sorter.
	 * @param parentTask The parent task, which owns all resources used by this sorter.
	 * @param serializer The type serializer.
	 * @param comparator The type comparator establishing the order relation.
	 * @param totalMemory The total amount of memory dedicated to sorting.
	 * 
	 * @throws IOException Thrown, if an error occurs initializing the resources for external sorting.
	 * @throws MemoryAllocationException Thrown, if not enough memory can be obtained from the memory manager to
	 *                                   perform the sort.
	 */
	public AsynchronousPartialSorter(MemoryManager memoryManager,
			MutableObjectIterator<E> input, AbstractInvokable parentTask, 
			TypeSerializer<E> serializer, TypeComparator<E> comparator,
			long totalMemory)
	throws IOException, MemoryAllocationException
	{
		super(memoryManager, null, input, parentTask, serializer, comparator, totalMemory,
			totalMemory < 2 * MIN_NUM_SORT_MEM_SEGMENTS * memoryManager.getPageSize() ? 1 : 
				Math.max((int) Math.ceil(((double) totalMemory) / MAX_MEM_PER_PARTIAL_SORT), 2),
			2, 0.0f, true);
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
	
	/* 
	 * This method does not actually create a spilling thread, but grabs the circular queues and creates the
	 * iterator that reads from the sort buffers in turn.
	 * 
	 * @see eu.stratosphere.pact.runtime.sort.UnilateralSortMerger#getSpillingThread(eu.stratosphere.pact.runtime.sort.ExceptionHandler, eu.stratosphere.pact.runtime.sort.UnilateralSortMerger.CircularQueues, eu.stratosphere.nephele.template.AbstractInvokable, eu.stratosphere.nephele.services.memorymanager.MemoryManager, eu.stratosphere.nephele.services.iomanager.IOManager, eu.stratosphere.pact.runtime.plugable.TypeSerializers, eu.stratosphere.pact.runtime.plugable.TypeComparator, java.util.List, java.util.List, int)
	 */
	@Override
	protected ThreadBase<E> getSpillingThread(ExceptionHandler<IOException> exceptionHandler, CircularQueues<E> queues,
			AbstractInvokable parentTask, MemoryManager memoryManager, IOManager ioManager, 
			TypeSerializer<E> serializer, TypeComparator<E> comparator,
			List<MemorySegment> sortReadMemory, List<MemorySegment> writeMemory, int maxFileHandles)
	{
		this.bufferIterator = new BufferQueueIterator(queues);
		setResultIterator(this.bufferIterator);
		
		return null;
	}

	// ------------------------------------------------------------------------

	/**
	 * This class implements an iterator over values from a {@link eu.stratosphere.pact.runtime.sort.BufferSortable}.
	 * The iterator returns the values of a given
	 * interval.
	 */
	private final class BufferQueueIterator implements MutableObjectIterator<E>
	{
		private final CircularQueues<E> queues;
		
		private CircularElement<E> currentElement;
		
		private MutableObjectIterator<E> currentIterator;
		
		private volatile boolean closed = false;


		protected BufferQueueIterator(CircularQueues<E> queues) {
			this.queues = queues;
		}

		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.common.util.MutableObjectIterator#next(java.lang.Object)
		 */
		@Override
		public boolean next(E target) throws IOException
		{
			if (this.currentIterator != null && this.currentIterator.next(target)) {
				return true;
			}
			else if (this.closed) {
				throw new IllegalStateException("The sorter has been closed.");
			}
			else {
				if (AsynchronousPartialSorter.this.iteratorException != null) {
					throw new IOException("The sorter has ancountered an error.", AsynchronousPartialSorter.this.iteratorException);
				}
				
				while (true) {
					if (this.currentElement == endMarker()) {
						return false;
					}
					else if (this.currentElement != null) {
						// return the current element to the empty queue
						this.currentElement.buffer.reset();
						this.queues.empty.add(this.currentElement);
					}
					
					// get a new element
					try {
						this.currentElement = null;
						while (!this.closed && this.currentElement == null) {
							this.currentElement = this.queues.spill.poll(1000, TimeUnit.MILLISECONDS);
						}
						if (AsynchronousPartialSorter.this.iteratorException != null) {
							throw new IOException("The sorter has ancountered an error.", AsynchronousPartialSorter.this.iteratorException);
						}
						
						if (this.currentElement == endMarker()) {
							// signals the end, no more buffers will come
							// release the memory first before returning
							releaseSortBuffers();
							return false;
						}
						if (this.currentElement == spillingMarker()) {
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
		
		public void close()
		{
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
		
		private final void releaseSortBuffers()
		{
			while (!this.queues.empty.isEmpty()) {
				final CircularElement<E> elem = this.queues.empty.poll();
				if (elem != null) {
					final NormalizedKeySorter<E> sorter = elem.buffer;
					final List<MemorySegment> segments = sorter.dispose();
					AsynchronousPartialSorter.this.memoryManager.release(segments);
				}
			}
		}

	};
}