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

package eu.stratosphere.nephele.services.memorymanager.spi;


import eu.stratosphere.nephele.services.memorymanager.DataInputView;
import eu.stratosphere.nephele.services.memorymanager.DataOutputView;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager.MemorySegmentDescriptor;



public final class DefaultMemorySegment extends MemorySegment
{
	/**
	 * The descriptor to the portion of the memory that was allocated.
	 */
	private MemorySegmentDescriptor descriptor;

	// --------------------------------------------------------------------------------------------
	
	DefaultMemorySegment(MemorySegmentDescriptor descriptor, DataInputView inputView, DataOutputView outputView)
	{
		super(descriptor.memory, descriptor.start, descriptor.size, inputView, outputView);
		this.descriptor = descriptor;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the memory segment descriptor, that describes where the segment was allocated from.
	 * 
	 * @return The memory segment descriptor.
	 */
	MemorySegmentDescriptor getSegmentDescriptor()
	{
		return this.descriptor;
	}
	
	/**
	 * Clears all memory references in the views over this memory segment. This way, code trying to access this
	 * memory segment through the views will fail.
	 */
	void clearMemoryReferences()
	{
		this.descriptor = null;
		this.memory = null;
		this.wrapper = null;
		
		((DefaultDataInputView) this.inputView).memory = null;
		((DefaultDataOutputView) this.outputView).memory = null;
	}
}
