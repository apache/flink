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

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.ListMemorySegmentSource;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.runtime.io.RandomAccessInputView;
import eu.stratosphere.pact.runtime.io.SimpleCollectingOutputView;
import eu.stratosphere.pact.runtime.util.MemoryBlockIterator;

/**
 * Base class for iterators that fetch a block of data into main memory and offer resettable
 * access to the data in that block.
 */
abstract class AbstractBlockResettableIterator<T> implements MemoryBlockIterator {
	
	protected static final Log LOG = LogFactory.getLog(AbstractBlockResettableIterator.class);
	
	// ------------------------------------------------------------------------
	
	protected final RandomAccessInputView readView;
	
	protected final SimpleCollectingOutputView collectingView;
	
	protected final TypeSerializer<T> serializer;
	
	protected int numRecordsInBuffer;
	
	protected int numRecordsReturned;
	
	protected final ArrayList<MemorySegment> emptySegments;
	
	protected final ArrayList<MemorySegment> fullSegments;
	
	private final MemoryManager memoryManager;
	
	protected volatile boolean closed;		// volatile since it may be asynchronously set to abort after current block
	
	// ------------------------------------------------------------------------
	
	protected AbstractBlockResettableIterator(TypeSerializer<T> serializer, MemoryManager memoryManager,
			int numPages, AbstractInvokable ownerTask)
	throws MemoryAllocationException
	{
		if (numPages < 1)
			throw new IllegalArgumentException("Block Resettable iterator requires at leat one page of memory");
		
		this.memoryManager = memoryManager;
		this.serializer = serializer;
		
		this.emptySegments = new ArrayList<MemorySegment>(numPages);
		this.fullSegments = new ArrayList<MemorySegment>(numPages);
		memoryManager.allocatePages(ownerTask, emptySegments, numPages);
		
		this.collectingView = new SimpleCollectingOutputView(this.fullSegments, 
						new ListMemorySegmentSource(this.emptySegments), memoryManager.getPageSize());
		this.readView = new RandomAccessInputView(this.fullSegments, memoryManager.getPageSize());
		
		if (LOG.isDebugEnabled())
			LOG.debug("Iterator initalized using " + numPages + " memory buffers.");
	}
	
	// --------------------------------------------------------------------------------------------

	public void open() {
		if (LOG.isDebugEnabled())
			LOG.debug("Block Resettable Iterator opened.");
	}
	

	public void reset() {
		if (this.closed) {
			throw new IllegalStateException("Iterator was closed.");
		}
		
		this.readView.setReadPosition(0);
		this.numRecordsReturned = 0;
	}
	

	@Override
	public boolean nextBlock() throws IOException {
		this.numRecordsInBuffer = 0;
		
		// add the full segments to the empty ones
		for (int i = this.fullSegments.size() - 1; i >= 0; i--) {
			this.emptySegments.add(this.fullSegments.remove(i));
		}
		
		// reset the views
		this.collectingView.reset();
		this.readView.setReadPosition(0);
		return true;
	}
	
	/**
	 * This method closes the iterator and releases all resources. This method works both as a regular
	 * shutdown and as a canceling method. The method may be called multiple times and will not produce
	 * an error.
	 */
	public void close() {
		synchronized (this) {
			if (this.closed) {
				return;
			}
			this.closed = true;
		}
		
		this.numRecordsInBuffer = 0;
		this.numRecordsReturned = 0;
		
		// add the full segments to the empty ones
		for (int i = this.fullSegments.size() - 1; i >= 0; i--) {
			this.emptySegments.add(this.fullSegments.remove(i));
		}
		
		// release the memory segment
		this.memoryManager.release(this.emptySegments);
		this.emptySegments.clear();
		
		if (LOG.isDebugEnabled())
			LOG.debug("Block Resettable Iterator closed.");
	}
	
	// --------------------------------------------------------------------------------------------
	
	protected boolean writeNextRecord(T record) throws IOException {
		try {
			this.serializer.serialize(record, this.collectingView);
			this.numRecordsInBuffer++;
			return true;
		} catch (EOFException eofex) {
			return false;
		}
	}
	
	protected T getNextRecord(T reuse) throws IOException {
		if (this.numRecordsReturned < this.numRecordsInBuffer) {
			this.numRecordsReturned++;
			return this.serializer.deserialize(reuse, this.readView);
		} else {
			return null;
		}
	}
}
