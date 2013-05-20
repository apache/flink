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

package eu.stratosphere.pact.runtime.task;

import java.io.IOException;
import java.util.ArrayList;

import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.DataInputView;
import eu.stratosphere.nephele.services.memorymanager.ListMemorySegmentSource;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.generic.types.TypeSerializer;
import eu.stratosphere.pact.runtime.io.InputViewIterator;
import eu.stratosphere.pact.runtime.io.SpillingBuffer;
import eu.stratosphere.pact.runtime.task.util.CloseableInputProvider;

/**
 * 
 */
public class TempBarrier<T> implements CloseableInputProvider<T> {
	
	private final SpillingBuffer buffer;
	
	private final TypeSerializer<T> serializer;
	
	private final TempWritingThread tempWriter;
	
	private final MemoryManager memManager;
	
	private final Object lock = new Object();
	
	private volatile Throwable exception;
	
	private final ArrayList<MemorySegment> memory;
	
	private volatile boolean writingDone;
	
	private volatile boolean closed;

	// --------------------------------------------------------------------------------------------
	
	public TempBarrier(AbstractInvokable owner, MutableObjectIterator<T> input, TypeSerializer<T> serializer,
			MemoryManager memManager, IOManager ioManager, int numPages) throws MemoryAllocationException
	{
		this.serializer = serializer;
		this.memManager = memManager;
		
		this.memory = new ArrayList<MemorySegment>(numPages);
		memManager.allocatePages(owner, this.memory, numPages);
		
		this.buffer = new SpillingBuffer(ioManager, new ListMemorySegmentSource(this.memory), memManager.getPageSize());
		this.tempWriter = new TempWritingThread(input, serializer, this.buffer);
	}
	
	// --------------------------------------------------------------------------------------------

	public void startReading() {
		this.tempWriter.start();
	}

	/**
	 * 
	 * This method resets the input!
	 * 
	 * @see eu.stratosphere.pact.runtime.task.util.CloseableInputProvider#getIterator()
	 */
	@Override
	public MutableObjectIterator<T> getIterator() throws InterruptedException, IOException {
		synchronized (this.lock) {
			while (this.exception == null && !this.writingDone) {
				this.lock.wait(5000);
			}
		}
		
		if (this.exception != null) {
			throw new RuntimeException("An error occurred creating the temp table.", this.exception);
		} else if (this.writingDone) {
			final DataInputView in = this.buffer.flip();
			return new InputViewIterator<T>(in, this.serializer);
		} else {
			return null;
		}
	}

	/* (non-Javadoc)
	 * @see java.io.Closeable#close()
	 */
	@Override
	public void close() throws IOException {
		synchronized (this.lock) {
			if (this.closed) {
				return;
			}
			if (this.exception == null) {
				this.exception = new Exception("The dam has been closed.");
			}
			this.lock.notifyAll();
		}
		
		try {
			this.tempWriter.shutdown();
			this.tempWriter.join();
		} catch (InterruptedException iex) {}
		
		this.memManager.release(this.buffer.close());
		this.memManager.release(this.memory);
	}
	
	private void setException(Throwable t) {
		synchronized (this.lock) {
			this.exception = t;
			this.lock.notifyAll();
		}
		try {
			close();
		} catch (Throwable ex) {}
	}
	
	private void writingDone() throws IOException {
		synchronized (this.lock) {
			this.writingDone = true;
			this.lock.notifyAll();
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	private final class TempWritingThread extends Thread
	{
		private final MutableObjectIterator<T> input;
		private final TypeSerializer<T> serializer;
		private final SpillingBuffer buffer;
		private volatile boolean running = true;
		
		private TempWritingThread(MutableObjectIterator<T> input, TypeSerializer<T> serializer, SpillingBuffer buffer) {
			super("Temp writer");
			setDaemon(true);
			
			this.input = input;
			this.serializer = serializer;
			this.buffer = buffer;
		}
		
		public void run() {
			final MutableObjectIterator<T> input = this.input;
			final TypeSerializer<T> serializer = this.serializer;
			final SpillingBuffer buffer = this.buffer;
			
			try {
				final T record = serializer.createInstance();
				
				while (this.running && input.next(record)) {
					serializer.serialize(record, buffer);
				}
				
				TempBarrier.this.writingDone();
			}
			catch (Throwable t) {
				TempBarrier.this.setException(t);
			}
		}
		
		public void shutdown() {
			this.running = false;
			this.interrupt();
		}
	}
}
