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
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.services.iomanager.Buffer;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.runtime.util.ResettableIterator;

/**
 * Implementation of an iterator that fetches a block of data into main memory and offers resettable
 * access to the data in that block.
 * 
 * @author Stephan Ewen
 * @author Fabian Hueske
 */
public class BlockResettableIterator extends AbstractBlockResettableIterator implements ResettableIterator<PactRecord>
{
	public static final Log LOG = LogFactory.getLog(BlockResettableIterator.class);
	
	public static final int MIN_BUFFER_SIZE = 8 * 1024;
	
	// ------------------------------------------------------------------------
	
	protected Iterator<PactRecord> input;
	
	private PactRecord nextElement;
	
	private final PactRecord stagingElement;
	
	private PactRecord leftOverElement;
	
	// ------------------------------------------------------------------------
	
	public BlockResettableIterator(MemoryManager memoryManager, Iterator<PactRecord> input,
			long availableMemory, int nrOfBuffers, AbstractInvokable ownerTask)
	throws MemoryAllocationException
	{
		this(memoryManager, availableMemory, nrOfBuffers, ownerTask);
		this.input = input;
	}
	
	public BlockResettableIterator(MemoryManager memoryManager,
			long availableMemory, int nrOfBuffers, AbstractInvokable ownerTask)
	throws MemoryAllocationException
	{
		super(memoryManager, availableMemory, nrOfBuffers, ownerTask);
		
		this.stagingElement = new PactRecord();
	}
	
	// ------------------------------------------------------------------------
	
	public void reopen(Iterator<PactRecord> input) throws IOException
	{
		this.input = input;
		
		collectAllBuffers(this.emptySegments);
		this.noMoreBlocks = false;
		this.closed = false;
		
		nextBlock();
	}

	/* (non-Javadoc)
	 * @see java.util.Iterator#hasNext()
	 */
	@Override
	public boolean hasNext()
	{
		if (this.nextElement == null)
		{
			// we need to make a case distinction whether we are currently reading through full blocks
			// or filling blocks anew
			if (this.bufferCurrentlyRead != null)
			{
				// we are reading from a full block
				if (this.bufferCurrentlyRead.read(this.stagingElement)) {
					// the current buffer had another element
					this.nextElement = this.stagingElement;
					return true;
				}
				else {
					// the current buffer is exhausted
					this.consumedBuffers.add(this.bufferCurrentlyRead);
					if (this.fullBuffers.isEmpty()) {
						// no more elements in this block.
						this.bufferCurrentlyRead = null;
						return false;
					}
					else {
						// go to next input block
						this.bufferCurrentlyRead = this.fullBuffers.remove(0);
						if (this.bufferCurrentlyRead.read(this.stagingElement)) {
							// the current buffer had another element
							this.nextElement = this.stagingElement;
							return true;
						}
						else {
							throw new RuntimeException("BlockResettableIterator: " +
									"BUG - Could not de-serialize element newly obtaint input block buffer.");
						}
					}
				}
			}
			else if (this.bufferCurrentlyFilled != null) {
				// we are reading from the input reader and filling the block along
				if (this.input.hasNext()) {
					PactRecord next = this.input.next();

					if (this.bufferCurrentlyFilled.write(next)) {
						// object fit into current buffer
						this.nextElement = next;
						return true;
					}
					else {
						// object did not fit into current buffer
						// add the current buffer to the full buffers
						final int fillPosition = this.bufferCurrentlyFilled.getPosition();
						final MemorySegment seg = this.bufferCurrentlyFilled.dispose();
						this.bufferCurrentlyFilled = null;
						
						final Buffer.Input in = new Buffer.Input(seg);
						in.reset(fillPosition);
						this.consumedBuffers.add(in);
						
						// get the next buffer
						if (this.emptySegments.isEmpty()) {
							// no more empty segments. the current element is left over
							this.leftOverElement = next;
							return false;
						}
						else {
							// next segment available, use it.
							this.bufferCurrentlyFilled = new Buffer.Output(this.emptySegments.remove(this.emptySegments.size() - 1));
							if (this.bufferCurrentlyFilled.write(next)) {
								// object fit into next buffer
								this.nextElement = next;
								return true;
							}
							else {
								throw new RuntimeException("BlockResettableIterator: " +
									"Could not serialize element into fresh block buffer - element is too large.");
							}
						}
					}
				}
				else {
					// no more input from the reader
					this.noMoreBlocks = true;
					return false;
				}
			}
			else {
				// we have a repeated call to hasNext() an either the buffers are completely filled, or completely read
				// or the iterator was closed
				if (this.closed) {
					throw new IllegalStateException("Iterator was closed.");
				}
				return false;
			}
		}
		else {
			return true;
		}
	}
	
	/* (non-Javadoc)
	 * @see java.util.Iterator#next()
	 */
	@Override
	public PactRecord next() {
		if (this.nextElement == null) {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
		}
		
		PactRecord out = this.nextElement;
		this.nextElement = null;
		return out;
	}
	
	/* (non-Javadoc)
	 * @see java.util.Iterator#remove()
	 */
	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.util.MemoryBlockIterator#nextBlock()
	 */
	@Override
	public boolean nextBlock() {
		// check the state
		if (this.closed) {
			throw new IllegalStateException("Iterator has been closed.");
		}
		
		// check whether more blocks are available
		if (this.noMoreBlocks) {
			return false;
		}
		
		// the basic logic is to dispose all input buffers and reuse the memory segments by putting
		// them into the empty segment list
		collectAllBuffers(this.emptySegments);
			
		// set one buffer to be filled and write the next element
		this.bufferCurrentlyFilled = new Buffer.Output(this.emptySegments.remove(this.emptySegments.size() - 1));
		
		PactRecord next = this.leftOverElement;
		this.leftOverElement = null;
		if (next == null) {
			if (this.input.hasNext()) {
				next = this.input.next();
			}
			else {
				this.noMoreBlocks = true;
				return false;
			}
		}
		
		if (!this.bufferCurrentlyFilled.write(next)) {
				throw new RuntimeException("BlockResettableIterator: " +
					"Could not serialize element into fresh block buffer - element is too large.");
		}
		this.nextElement = next;
		return true;
	}
}
