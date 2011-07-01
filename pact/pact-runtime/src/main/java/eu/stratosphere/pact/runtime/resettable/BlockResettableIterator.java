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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.io.RecordDeserializer;
import eu.stratosphere.nephele.services.iomanager.Buffer;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.pact.runtime.task.util.MemoryBlockIterator;

/**
 * Implementation of an iterator that fetches a block of data into main memory and offers resettable
 * access to the data in that block.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 * @author Fabian Hueske
 * 
 * @param <T> The type of the records that are iterated over.
 */
public class BlockResettableIterator<T extends Record> implements MemoryBlockIterator<T>
{
	public static final Log LOG = LogFactory.getLog(BlockResettableIterator.class);
	
	public static final int MIN_BUFFER_SIZE = 8 * 1024;
	
	// ------------------------------------------------------------------------
	
	protected final Iterator<T> input;
	
	protected final MemoryManager memoryManager;
	
	protected final List<MemorySegment> emptySegments;

	protected final List<Buffer.Input> fullBuffers;
	
	protected final List<Buffer.Input> consumedBuffers;
	
	private final RecordDeserializer<T> deserializer;
	
	private Buffer.Input bufferCurrentlyRead;
	
	private Buffer.Output bufferCurrentlyFilled;
	
	private T nextElement = null;
	
	private T leftOverElement = null;
	
	private boolean noMoreBlocks = false;
	
	private volatile boolean closed = false;
	
	// ------------------------------------------------------------------------
	
	public BlockResettableIterator(MemoryManager memoryManager, Iterator<T> input,
			long availableMemory, int nrOfBuffers,
			RecordDeserializer<T> deserializer, AbstractInvokable ownerTask)
	throws MemoryAllocationException
	{
		if (nrOfBuffers < 1) {
			throw new IllegalArgumentException("BlockResettableIterator needs at least one element.");
		}
		if (availableMemory < MIN_BUFFER_SIZE) {
			throw new IllegalArgumentException("Block Resettable iterator requires at leat " + MIN_BUFFER_SIZE + " bytes of memory.");
		}
		
		this.input = input;
		this.memoryManager = memoryManager;
		this.deserializer = deserializer;
		
		// allocate the memory buffers
		this.emptySegments = this.memoryManager.allocate(ownerTask, availableMemory, nrOfBuffers, MIN_BUFFER_SIZE);
		this.fullBuffers = new ArrayList<Buffer.Input>();
		this.consumedBuffers = new ArrayList<Buffer.Input>();
		
		if (LOG.isDebugEnabled())
			LOG.debug("Iterator initalized using " + availableMemory + " bytes of IO buffer.");
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
				T next = this.deserializer.getInstance();
				
				// we are reading from a full block
				if (this.bufferCurrentlyRead.read(next)) {
					// the current buffer had another element
					this.nextElement = next;
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
						if (this.bufferCurrentlyRead.read(next)) {
							this.nextElement = next;
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
					T next = this.input.next();

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
	public T next() {
		if (this.nextElement == null) {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
		}
		
		T out = this.nextElement;
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
	 * @see eu.stratosphere.pact.runtime.task.util.ResettableIterator#reset()
	 */
	@Override
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
			
		// set one buffer to be filled and write the leftover element
		this.bufferCurrentlyFilled = new Buffer.Output(this.emptySegments.remove(this.emptySegments.size() - 1));
		if (this.leftOverElement != null) {
			if (!this.bufferCurrentlyFilled.write(this.leftOverElement)) {
				throw new RuntimeException("BlockResettableIterator: " +
					"Could not serialize element into fresh block buffer - element is too large.");
			}
			this.nextElement = this.leftOverElement;
			this.leftOverElement = null;
		}
		
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
		return this.leftOverElement != null || this.input.hasNext(); 
	}
	
	/**
	 * Opens the block resettable iterator. This method will cause the iterator to start asynchronously 
	 * reading the input and prepare the first block.
	 */
	public void open()
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
	private void collectAllBuffers(List<MemorySegment> target)
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
