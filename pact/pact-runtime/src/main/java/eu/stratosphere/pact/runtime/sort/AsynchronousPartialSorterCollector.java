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

import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.common.generic.types.TypeComparator;
import eu.stratosphere.pact.common.generic.types.TypeSerializer;
import eu.stratosphere.pact.common.util.MutableObjectIterator;

/**
 * The {@link AsynchronousPartialSorterCollector} is a simple sort implementation that sorts
 * bulks inside its buffers, and returns them directly, without merging them. Therefore,
 * it establishes an order within certain windows, but not across them.
 * <p>
 * In contract to the {@link AsynchronousPartialSorter}, this class has no dedicated reading thread that
 * pulls records from an iterator, but offers a collector into which data to be sorted is pushed.
 * 
 * @author Stephan Ewen
 */
public class AsynchronousPartialSorterCollector<E> extends AsynchronousPartialSorter<E>
{
	private InputDataCollector<E> collector;
	
	// ------------------------------------------------------------------------
	// Constructor
	// ------------------------------------------------------------------------
	
	/**
	 * 
	 * 
	 * @param memoryManager The memory manager from which to allocate the memory.
	 * @param parentTask The parent task, which owns all resources used by this sorter.
	 * @param serializer The type serializer.
	 * @param comparator The type comparator establishing the order relation.
	 * @param totalMemory The total amount of memory dedicated to sorting.
	 * 
	 * @throws IOException Thrown, if an error occurs initializing the resources for external sorting.
	 * @throws MemoryAllocationException Thrown, if not enough memory can be obtained from the memory manager to
	 *                                   perform the sort.
	 */
	public AsynchronousPartialSorterCollector(MemoryManager memoryManager,
			AbstractInvokable parentTask, 
			TypeSerializer<E> serializer, TypeComparator<E> comparator,
			long totalMemory)
	throws IOException, MemoryAllocationException
	{
		super(memoryManager, null, parentTask, serializer, comparator, totalMemory);
	}
	
	// ------------------------------------------------------------------------
	
	/**
	 * Gets the collector that writes into the sort buffers.
	 * 
	 * @return The collector that writes into the sort buffers.
	 */
	public InputDataCollector<E> getInputCollector() {
		return this.collector;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.sort.UnilateralSortMerger#getReadingThread(eu.stratosphere.pact.runtime.sort.ExceptionHandler, eu.stratosphere.pact.common.util.MutableObjectIterator, eu.stratosphere.pact.runtime.sort.UnilateralSortMerger.CircularQueues, eu.stratosphere.nephele.template.AbstractInvokable, eu.stratosphere.pact.runtime.plugable.TypeSerializers, long)
	 */
	@Override
	protected ThreadBase<E> getReadingThread(ExceptionHandler<IOException> exceptionHandler,
		MutableObjectIterator<E> reader, CircularQueues<E> queues, AbstractInvokable parentTask,
		TypeSerializer<E> serializer, long startSpillingBytes)
	{
		this.collector = new InputDataCollector<E>(queues, startSpillingBytes);
		return null;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.sort.UnilateralSortMerger#close()
	 */
	public void close()
	{
		try {
			if (this.collector != null)
				this.collector.close();
		}
		finally {
			super.close();
		}
	}
}
