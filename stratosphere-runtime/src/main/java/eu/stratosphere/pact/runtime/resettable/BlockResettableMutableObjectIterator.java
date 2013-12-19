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

package eu.stratosphere.pact.runtime.resettable;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.runtime.util.ResettableMutableObjectIterator;
import eu.stratosphere.util.MutableObjectIterator;

/**
 * Implementation of an iterator that fetches a block of data into main memory and offers resettable
 * access to the data in that block.
 * 
 * @author Stephan Ewen
 * @author Fabian Hueske
 */
public class BlockResettableMutableObjectIterator<T> extends AbstractBlockResettableIterator<T>
implements ResettableMutableObjectIterator<T>
{
	public static final Log LOG = LogFactory.getLog(BlockResettableMutableObjectIterator.class);
	
	// ------------------------------------------------------------------------
	
	private final MutableObjectIterator<T> input;
	
	private boolean readPhase;
	
	private boolean leftOverReturned;
	
	private boolean fullWriteBuffer;
	
	private boolean noMoreBlocks;
	
	private final T leftOverRecord;
	
	// ------------------------------------------------------------------------
	
	public BlockResettableMutableObjectIterator(MemoryManager memoryManager,
			MutableObjectIterator<T> input,	TypeSerializer<T> serializer,
			long availableMemory, AbstractInvokable ownerTask)
	throws MemoryAllocationException
	{
		super(serializer, memoryManager, availableMemory, ownerTask);
		
		this.input = input;
		this.leftOverRecord = serializer.createInstance();
		this.leftOverReturned = true;
	}
	
	// --------------------------------------------------------------------------------------------


	@Override
	public boolean next(T target) throws IOException
	{
		// check for the left over element
		if (this.readPhase) {
			return getNextRecord(target);
		} else {
			// writing phase. check for leftover first
			if (this.leftOverReturned) {
				// get next record
				if (this.input.next(target)) {
					if (writeNextRecord(target)) {
						return true;
					} else {
						// did not fit into memory, keep as leftover
						this.serializer.copyTo(target, this.leftOverRecord);
						this.leftOverReturned = false;
						this.fullWriteBuffer = true;
						return false;
					}
				} else {
					this.noMoreBlocks = true;
					return false;
				}
			} else if (this.fullWriteBuffer) {
				return false;
			} else {
				this.leftOverReturned = true;
				this.serializer.copyTo(this.leftOverRecord, target);
				return true;
			}
		}
	}
	

	public void reset()
	{
		// a reset always goes to the read phase
		this.readPhase = true;
		super.reset();
	}
	

	@Override
	public boolean nextBlock() throws IOException
	{
		// check the state
		if (this.closed) {
			throw new IllegalStateException("Iterator has been closed.");
		}
		
		// check whether more blocks are available
		if (this.noMoreBlocks) {
			return false;
		}

		// reset the views in the superclass
		super.nextBlock();
		
		// if there is no leftover record, get a record such that we guarantee to advance
		if (this.leftOverReturned || !this.fullWriteBuffer) {
			if (this.input.next(this.leftOverRecord)) {
				this.leftOverReturned = false;
			} else {
				this.noMoreBlocks = true;
				this.fullWriteBuffer = true;
				this.readPhase = false;
				return false;
			}
		}

		// write the leftover record
		if (!writeNextRecord(this.leftOverRecord)) {
			throw new IOException("BlockResettableIterator could not serialize record into fresh memory block: " +
					"Record is too large.");
		}
		this.readPhase = false;
		this.fullWriteBuffer = false;
		
		return true;
	}
	
	/**
	 * Checks, whether the input that is blocked by this iterator, has further elements
	 * available. This method may be used to forecast (for example at the point where a
	 * block is full) whether there will be more data (possibly in another block).
	 * 
	 * @return True, if there will be more data, false otherwise.
	 */
	public boolean hasFurtherInput()
	{
		return !this.noMoreBlocks; 
	}
	

	public void close()
	{
		// suggest that we are in the read phase. because nothing is in the current block,
		// read requests will fail
		this.readPhase = true;
		super.close();
	}
}
