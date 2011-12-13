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
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
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
public class AsynchronousPartialSorterCollector extends AsynchronousPartialSorter
{
	private InputDataCollector collector;
	
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
	public AsynchronousPartialSorterCollector(
			MemoryManager memoryManager, IOManager ioManager, long totalMemory,
			Comparator<Key>[] keyComparators, int[] keyPositions, Class<? extends Key>[] keyClasses,
			AbstractInvokable parentTask)
	throws IOException, MemoryAllocationException
	{
		super(memoryManager, ioManager, totalMemory, keyComparators, keyPositions, keyClasses, null, parentTask);
	}
	
	// ------------------------------------------------------------------------
	
	/**
	 * Gets the collector that writes into the sort buffers.
	 * 
	 * @return The collector that writes into the sort buffers.
	 */
	public InputDataCollector getInputCollector() {
		return this.collector;
	}

	// ------------------------------------------------------------------------
	// Factory Methods
	// ------------------------------------------------------------------------

	@Override
	protected ThreadBase getReadingThread(ExceptionHandler<IOException> exceptionHandler, 
			MutableObjectIterator<PactRecord> reader, CircularQueues queues, AbstractInvokable parentTask, long startSpillingBytes)
	{
		this.collector = new InputDataCollector(queues, startSpillingBytes);
		
		return new DummyThread(exceptionHandler, queues, parentTask);
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
