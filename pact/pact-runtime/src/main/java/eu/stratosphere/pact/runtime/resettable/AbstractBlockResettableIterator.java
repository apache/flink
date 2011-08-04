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

package eu.stratosphere.pact.runtime.resettable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.services.iomanager.Buffer;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.runtime.util.MemoryBlockIterator;

/**
 * Base class for iterators that fetch a block of data into main memory and offer resettable
 * access to the data in that block.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 * @author Fabian Hueske
 */
abstract class AbstractBlockResettableIterator implements MemoryBlockIterator
{
	public static final Log LOG = LogFactory.getLog(AbstractBlockResettableIterator.class);
	
	public static final int MIN_BUFFER_SIZE = 8 * 1024;
	
	// ------------------------------------------------------------------------
	
	protected final MemoryManager memoryManager;
	
	protected final List<MemorySegment> emptySegments;

	protected final List<Buffer.Input> fullBuffers;
	
	protected final List<Buffer.Input> consumedBuffers;
	
	protected Buffer.Input bufferCurrentlyRead;
	
	protected Buffer.Output bufferCurrentlyFilled;
	
	protected boolean noMoreBlocks;
	
	protected volatile boolean closed;		// volatile since it may be asynchronously set to abort after current block
	
	// ------------------------------------------------------------------------
	
	protected AbstractBlockResettableIterator(MemoryManager memoryManager, long availableMemory, int nrOfBuffers, 
			AbstractInvokable ownerTask)
	throws MemoryAllocationException
	{
		if (nrOfBuffers < 1) {
			throw new IllegalArgumentException("BlockResettableIterator needs at least one element.");
		}
		if (availableMemory < MIN_BUFFER_SIZE) {
			throw new IllegalArgumentException("Block Resettable iterator requires at leat " + MIN_BUFFER_SIZE + " bytes of memory.");
		}
		
		this.memoryManager = memoryManager;
		
		// allocate the memory buffers
		this.emptySegments = this.memoryManager.allocate(ownerTask, availableMemory, nrOfBuffers, MIN_BUFFER_SIZE);
		this.fullBuffers = new ArrayList<Buffer.Input>();
		this.consumedBuffers = new ArrayList<Buffer.Input>();
		
		if (LOG.isDebugEnabled())
			LOG.debug("Iterator initalized using " + availableMemory + " bytes of IO buffer.");
	}
	
	// --------------------------------------------------------------------------------------------

	/**
	 * 
	 */
	public void reset()
	{
		if (this.closed) {
			throw new IllegalStateException("Iterator was closed.");
		}
		
		// if some full buffers remain, remember them
		List<Buffer.Input> fullBuffsLeft = null;
		if (!this.fullBuffers.isEmpty()) {
			fullBuffsLeft = new ArrayList<Buffer.Input>(this.fullBuffers.size());
			fullBuffsLeft.addAll(this.fullBuffers);
			this.fullBuffers.clear();
		}

		// we need to rewind all consumed buffers and add them again to the full buffers
		for (int i = 0; i < this.consumedBuffers.size(); i++) {
			Buffer.Input in = this.consumedBuffers.get(i);
			in.rewind();
			this.fullBuffers.add(in);
		}
		this.consumedBuffers.clear();
		
		// add the currently read buffer
		if (this.bufferCurrentlyRead != null) {
			this.bufferCurrentlyRead.rewind();
			this.fullBuffers.add(this.bufferCurrentlyRead);
			this.bufferCurrentlyRead = null;
		}
		
		// re-add the left buffers
		if (fullBuffsLeft != null) {
			this.fullBuffers.addAll(fullBuffsLeft);
		}
		
		// if we are currently filling a buffer, add it
		if (this.bufferCurrentlyFilled != null) {
			final int pos = this.bufferCurrentlyFilled.getPosition();
			final Buffer.Input in = new Buffer.Input(this.bufferCurrentlyFilled.dispose());
			in.reset(pos);
			this.fullBuffers.add(in);
			this.bufferCurrentlyFilled = null;
		}
		
		// take the first input buffer
		this.bufferCurrentlyRead = this.fullBuffers.remove(0);
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
	
	/**
	 * Opens the block resettable iterator. This method will cause the iterator to start asynchronously 
	 * reading the input and prepare the first block.
	 */
	public void open() throws IOException
	{
		if (LOG.isDebugEnabled())
			LOG.debug("Iterator opened.");
		
		// move the first = next block
		nextBlock();
	}
	
	/**
	 * This method closes the iterator and releases all resources. This method works both as a regular
	 * shutdown and as a canceling method. The method may be called multiple times and will not produce
	 * an error.
	 */
	public void close()
	{
		synchronized (this) {
			if (this.closed) {
				return;
			}
			this.closed = true;
		}
		
		// remove all blocks
		List<MemorySegment> toReturn = new ArrayList<MemorySegment>(this.emptySegments.size() + 
				this.fullBuffers.size() + this.consumedBuffers.size() + 2);

		// collect empty segments
		toReturn.addAll(this.emptySegments);
		this.emptySegments.clear();
		
		// collect all other segments
		collectAllBuffers(toReturn);
		
		// release the memory segment
		this.memoryManager.release(toReturn);
		
		if (LOG.isDebugEnabled())
			LOG.debug("Iterator closed.");
	}
	
	
	/**
	 * Takes all buffers in the list of full buffers and consumed buffers,
	 * as well as the currently written and currently read buffers, and disposes
	 * them, putting their backing memory segment in the given list.
	 * 
	 * @param target The list to collect the buffers in. 
	 */
	protected void collectAllBuffers(List<MemorySegment> target)
	{
		// collect full buffers
		while (!this.fullBuffers.isEmpty()) {
			Buffer.Input in = this.fullBuffers.remove(this.fullBuffers.size() - 1);
			target.add(in.dispose());
		}
		
		// collect consumed segments
		while (!this.consumedBuffers.isEmpty()) {
			Buffer.Input in = this.consumedBuffers.remove(this.consumedBuffers.size() - 1);
			target.add(in.dispose());
		}
		
		// return the currently read buffer
		if (this.bufferCurrentlyRead != null) {
			target.add(this.bufferCurrentlyRead.dispose());
			this.bufferCurrentlyRead = null;
		}
		
		// return the currently filled buffer
		if (this.bufferCurrentlyFilled != null) {
			target.add(this.bufferCurrentlyFilled.dispose());
			this.bufferCurrentlyFilled = null;
		}
	}
}
