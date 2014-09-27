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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.SpillingBuffer;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memorymanager.ListMemorySegmentSource;
import org.apache.flink.runtime.memorymanager.MemoryAllocationException;
import org.apache.flink.runtime.memorymanager.MemoryManager;
import org.apache.flink.runtime.util.ResettableIterator;

/**
 * Implementation of a resettable iterator. While iterating the first time over the data, the iterator writes the
 * records to a spillable buffer. Any subsequent iteration re-reads the data from that buffer.
 * 
 * @param <T> The type of record that the iterator handles.
 */
public class SpillingResettableIterator<T> implements ResettableIterator<T> {
	
	private static final Logger LOG = LoggerFactory.getLogger(SpillingResettableIterator.class);
	
	// ------------------------------------------------------------------------

	private T next;
	
	private T instance;
	
	protected DataInputView inView;
	
	protected final TypeSerializer<T> serializer;
	
	private int elementCount;
	
	private int currentElementNum;
	
	protected final SpillingBuffer buffer;
	
	protected final Iterator<T> input;
	
	protected final MemoryManager memoryManager;
	
	private final List<MemorySegment> memorySegments;
	
	private final boolean releaseMemoryOnClose;
	
	// ------------------------------------------------------------------------


	public SpillingResettableIterator(Iterator<T> input, TypeSerializer<T> serializer, 
			MemoryManager memoryManager, IOManager ioManager,
			int numPages, AbstractInvokable parentTask)
	throws MemoryAllocationException
	{
		this(input, serializer, memoryManager, ioManager, memoryManager.allocatePages(parentTask, numPages), true);
	}
	
	public SpillingResettableIterator(Iterator<T> input, TypeSerializer<T> serializer,
			MemoryManager memoryManager, IOManager ioManager, List<MemorySegment> memory)
	{
		this(input, serializer, memoryManager, ioManager, memory, false);
	}
	
	private SpillingResettableIterator(Iterator<T> input, TypeSerializer<T> serializer,
			MemoryManager memoryManager, IOManager ioManager,
			List<MemorySegment> memory, boolean releaseMemOnClose)
	{
		this.memoryManager = memoryManager;
		this.input = input;
		this.instance = serializer.createInstance();
		this.serializer = serializer;
		this.memorySegments = memory;
		this.releaseMemoryOnClose = releaseMemOnClose;
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("Creating spilling resettable iterator with " + memory.size() + " pages of memory.");
		}
		
		this.buffer = new SpillingBuffer(ioManager, new ListMemorySegmentSource(memory), memoryManager.getPageSize());
	}

	
	public void open() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Spilling Resettable Iterator opened.");
		}
	}

	public void reset() throws IOException {
		this.inView = this.buffer.flip();
		this.currentElementNum = 0;
	}

	@Override
	public boolean hasNext() {
		if (this.next == null) {
			if (this.inView != null) {
				if (this.currentElementNum < this.elementCount) {
					try {
						this.instance = this.serializer.deserialize(this.instance, this.inView);
					} catch (IOException e) {
						throw new RuntimeException("SpillingIterator: Error reading element from buffer.", e);
					}
					this.next = this.instance;
					this.currentElementNum++;
					return true;
				} else {
					return false;
				}
			} else {
				// writing pass (first)
				if (this.input.hasNext()) {
					this.next = this.input.next();
					try {
						this.serializer.serialize(this.next, this.buffer);
					} catch (IOException e) {
						throw new RuntimeException("SpillingIterator: Error writing element to buffer.", e);
					}
					this.elementCount++;
					return true;
				} else {
					return false;
				}
			}
		} else {
			return true;
		}
	}

	@Override
	public T next() {
		if (this.next != null || hasNext()) {
			final T out = this.next;
			this.next = null;
			return out;
		} else {
			throw new NoSuchElementException();
		}
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	public List<MemorySegment> close() throws IOException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Spilling Resettable Iterator closing. Stored " + this.elementCount + " records.");
		}

		this.inView = null;
		
		final List<MemorySegment> memory = this.buffer.close();
		memory.addAll(this.memorySegments);
		this.memorySegments.clear();
		
		if (this.releaseMemoryOnClose) {
			this.memoryManager.release(memory);
			return Collections.emptyList();
		} else {
			return memory;
		}
	}
}
