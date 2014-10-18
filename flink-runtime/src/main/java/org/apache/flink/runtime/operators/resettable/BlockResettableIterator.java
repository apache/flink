/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.runtime.operators.resettable;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memorymanager.MemoryAllocationException;
import org.apache.flink.runtime.memorymanager.MemoryManager;
import org.apache.flink.runtime.util.ResettableIterator;

/**
 * Implementation of an iterator that fetches a block of data into main memory and offers resettable
 * access to the data in that block.
 * 
 */
public class BlockResettableIterator<T> extends AbstractBlockResettableIterator<T> implements ResettableIterator<T> {
	
	public static final Logger LOG = LoggerFactory.getLogger(BlockResettableIterator.class);
	
	// ------------------------------------------------------------------------
	
	protected Iterator<T> input;
	
	private T nextElement;

	private final T reuseElement;
	
	private T leftOverElement;
	
	private boolean readPhase;
	
	private boolean noMoreBlocks;
	
	// ------------------------------------------------------------------------
	
	public BlockResettableIterator(MemoryManager memoryManager, Iterator<T> input,
			TypeSerializer<T> serializer, int numPages, AbstractInvokable ownerTask)
	throws MemoryAllocationException
	{
		this(memoryManager, serializer, numPages, ownerTask);
		this.input = input;
	}
	
	public BlockResettableIterator(MemoryManager memoryManager,
			TypeSerializer<T> serializer, int numPages, AbstractInvokable ownerTask)
	throws MemoryAllocationException
	{
		super(serializer, memoryManager, numPages, ownerTask);
		
		this.reuseElement = serializer.createInstance();
	}
	
	// ------------------------------------------------------------------------
	
	public void reopen(Iterator<T> input) throws IOException {
		this.input = input;
		
		this.noMoreBlocks = false;
		this.closed = false;
		
		nextBlock();
	}
	
	

	@Override
	public boolean hasNext() {
		try {
			if (this.nextElement == null) {
				if (this.readPhase) {
					// read phase, get next element from buffer
					T tmp = getNextRecord(this.reuseElement);
					if (tmp != null) {
						this.nextElement = tmp;
						return true;
					} else {
						return false;
					}
				} else {
					if (this.input.hasNext()) {
						final T next = this.input.next();
						if (writeNextRecord(next)) {
							this.nextElement = next;
							return true;
						} else {
							this.leftOverElement = next;
							return false;
						}
					} else {
						this.noMoreBlocks = true;
						return false;
					}
				}
			} else {
				return true;
			}
		} catch (IOException ioex) {
			throw new RuntimeException("Error (de)serializing record in block resettable iterator.", ioex);
		}
	}
	

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
	

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
	

	public void reset() {
		// a reset always goes to the read phase
		this.readPhase = true;
		super.reset();
	}
	

	@Override
	public boolean nextBlock() throws IOException {
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
		
		T next = this.leftOverElement;
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
		
		// write the leftover record
		if (!writeNextRecord(next)) {
			throw new IOException("BlockResettableIterator could not serialize record into fresh memory block: " +
					"Record is too large.");
		}
		
		this.nextElement = next;
		this.readPhase = false;
		
		return true;
	}
	
	/**
	 * Checks, whether the input that is blocked by this iterator, has further elements
	 * available. This method may be used to forecast (for example at the point where a
	 * block is full) whether there will be more data (possibly in another block).
	 * 
	 * @return True, if there will be more data, false otherwise.
	 */
	public boolean hasFurtherInput() {
		return !this.noMoreBlocks; 
	}
	

	public void close() {
		// suggest that we are in the read phase. because nothing is in the current block,
		// read requests will fail
		this.readPhase = true;
		super.close();
	}
}
