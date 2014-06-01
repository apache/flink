/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.sort;

import java.io.IOException;

import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.common.typeutils.TypeSerializerFactory;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.util.MutableObjectIterator;

/**
 * The {@link AsynchronousPartialSorterCollector} is a simple sort implementation that sorts
 * bulks inside its buffers, and returns them directly, without merging them. Therefore,
 * it establishes an order within certain windows, but not across them.
 * <p>
 * In contract to the {@link AsynchronousPartialSorter}, this class has no dedicated reading thread that
 * pulls records from an iterator, but offers a collector into which data to be sorted is pushed.
 * 
 */
public class AsynchronousPartialSorterCollector<E> extends AsynchronousPartialSorter<E> {
	
	private InputDataCollector<E> collector;
	
	// ------------------------------------------------------------------------
	// Constructor
	// ------------------------------------------------------------------------
	
	/**
	 * @param memoryManager The memory manager from which to allocate the memory.
	 * @param parentTask The parent task, which owns all resources used by this sorter.
	 * @param serializerFactory The type serializer.
	 * @param comparator The type comparator establishing the order relation.
	 * @param memoryFraction The fraction of memory dedicated to sorting.
	 * 
	 * @throws IOException Thrown, if an error occurs initializing the resources for external sorting.
	 * @throws MemoryAllocationException Thrown, if not enough memory can be obtained from the memory manager to
	 *                                   perform the sort.
	 */
	public AsynchronousPartialSorterCollector(MemoryManager memoryManager,
			AbstractInvokable parentTask, 
			TypeSerializerFactory<E> serializerFactory, TypeComparator<E> comparator,
			double memoryFraction)
	throws IOException, MemoryAllocationException
	{
		super(memoryManager, null, parentTask, serializerFactory, comparator,
				memoryFraction);
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

	@Override
	protected ThreadBase<E> getReadingThread(ExceptionHandler<IOException> exceptionHandler,
		MutableObjectIterator<E> reader, CircularQueues<E> queues, AbstractInvokable parentTask,
		TypeSerializer<E> serializer, long startSpillingBytes)
	{
		this.collector = new InputDataCollector<E>(queues, startSpillingBytes);
		return null;
	}
	

	public void close() {
		try {
			if (this.collector != null) {
				this.collector.close();
			}
		}
		finally {
			super.close();
		}
	}
}
