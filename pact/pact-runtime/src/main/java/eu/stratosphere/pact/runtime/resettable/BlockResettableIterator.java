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
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.io.Reader;
import eu.stratosphere.nephele.io.RecordDeserializer;
import eu.stratosphere.nephele.services.iomanager.Buffer;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.pact.runtime.task.util.MemoryBlockIterator;

/**
 * Implementation of an iterator that fetches a block of data into main memory and offers a resettable
 * iterator on this block.
 * 
 * @author mheimel
 * @author Fabian Hueske
 * @author Stephan Ewen
 * 
 * @param <T> The type of the records that are iterated over.
 */
public class BlockResettableIterator<T extends Record> implements MemoryBlockIterator<T> {

	private static final Log LOG = LogFactory.getLog(BlockResettableIterator.class);
	
	public static final int MIN_BUFFER_SIZE = 8 * 1024;
	
	// ------------------------------------------------------------------------

	protected final MemoryManager memoryManager;

	protected final List<MemorySegment> buffers;
	
	protected final BlockingQueue<MemorySegment> emptySegments;

	protected final BlockingQueue<Buffer.Input> filledBuffers;
	
	protected final RecordDeserializer<T> deserializer;

	private final BlockFetcher<T> blockFetcher;

	private Thread blockFetcherThread;
	
	private Buffer.Input in = null;
	
	private T deserializationInstance = null;
	
	private volatile Throwable error = null;
	
	private volatile boolean closed = false;

	// ------------------------------------------------------------------------
	
	public BlockResettableIterator(MemoryManager memoryManager, Reader<T> reader, long availableMemory, int nrOfBuffers,
			RecordDeserializer<T> deserializer, AbstractInvokable ownerTask)
	throws MemoryAllocationException
	{
		this.deserializer = deserializer;
		this.memoryManager = memoryManager;
		
		// allocate the queues
		this.emptySegments = new LinkedBlockingQueue<MemorySegment>();
		this.filledBuffers = new LinkedBlockingQueue<Buffer.Input>();
		
		// allocate the memory buffers
		this.buffers = this.memoryManager.allocate(ownerTask, availableMemory, nrOfBuffers, MIN_BUFFER_SIZE);
		
		// now append all memory segments to the workerQueue
		this.emptySegments.addAll(buffers);
		
		// create the writer thread
		this.blockFetcher = new BlockFetcher<T>(emptySegments, filledBuffers, reader);
		
		LOG.debug("Iterator initalized using " + availableMemory + " bytes of IO buffer.");
	}

	/* (non-Javadoc)
	 * @see java.util.Iterator#hasNext()
	 */
	@Override
	public boolean hasNext()
	{
		// an exception may occur here, if the iterator has been closed or is in the process
		// of being closed
		try {
			if (deserializationInstance == null) {
				deserializationInstance = deserializer.getInstance();
				if (!in.read(deserializationInstance)) {
					deserializationInstance = null;
					return false;
				}
			}
			return true;
		}
		catch (RuntimeException ex) {
			if (this.closed) {
				throw new IllegalStateException("The iterator has been closed.");
			}
			else if (this.error != null) {
				throw new RuntimeException("The iterator encountered an error: " + error.getMessage(), error);
			}
			else {
				// unknown error, so re-throw
				throw ex;
			}
		}
	}

	/* (non-Javadoc)
	 * @see java.util.Iterator#next()
	 */
	@Override
	public T next() {
		if (this.deserializationInstance == null) {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
		}
		
		T out = deserializationInstance;
		deserializationInstance = null;
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
	public void reset() {
		// check the state
		if (this.in == null) {
			if (this.closed) {
				throw new IllegalStateException("Iterator has been closed.");
			}
			else {
				throw new IllegalStateException("Iterator has not been opened.");
			}
		}
		
		// we need a try block here, because no synchronization is performed between
		// resetting and possible asynchronous close calls
		try {
			// re-open the input reader
			this.in.rewind();
			this.deserializationInstance = null;
		}
		catch (RuntimeException ex) {
			if (this.closed) {
				throw new IllegalStateException("The iterator has been closed.");
			}
			else if (this.error != null) {
				throw new RuntimeException("The iterator encountered an error: " + error.getMessage(), error);
			}
			else {
				// unknown error, so re-throw
				throw ex;
			}
		}
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
		// check, whether an exception was set
		if (this.error != null) {
			throw new RuntimeException("The iterator encountered an error: " + error.getMessage(), error);
		}
		
		// we need a try/catch block here, because no synchronization is performed between
		// this method and possible asynchronous close calls
		try {
			// add the last block to the worker queue of the writer thread
			if (this.in != null)
				this.emptySegments.add(this.in.dispose());
			// now fetch the latest filled Buffer
			try {
				this.in = this.filledBuffers.take();
			}
			catch (InterruptedException e) {
				throw new RuntimeException("BlockResettableIterator: Unable to fetch the next filled buffer", e);
			}
			
			if (in.getRemainingBytes() == 0) {
				// empty buffer signals end
				return false;
			}
			return true;
		}
		catch (RuntimeException ex) {
			if (this.closed) {
				throw new IllegalStateException("The iterator has been closed.");
			}
			else if (this.error != null) {
				throw new RuntimeException("The iterator encountered an error: " + error.getMessage(), error);
			}
			else {
				// unknown error, so re-throw
				throw ex;
			}
		}
	}

	/**
	 * Opens the block resettable iterator. This method will cause the iterator to start asynchronously 
	 * reading the input and prepare the first block.
	 */
	public void open() {
		LOG.debug("Iterator opened.");
		
		final UncaughtExceptionHandler uceh = new UncaughtExceptionHandler() {
			@Override
			public void uncaughtException(Thread t, Throwable e) {
				// process error only, if the iterator has not been closed.
				if (!BlockResettableIterator.this.closed) {
					if (e instanceof RuntimeException && e.getCause() != null) {
						BlockResettableIterator.this.error = e.getCause();
					}
					else {
						BlockResettableIterator.this.error = e;
					}
				}
			}
		};
		
		// start the writer Thread
		this.blockFetcherThread = new Thread(blockFetcher);
		this.blockFetcherThread.setDaemon(true);
		this.blockFetcherThread.setName("Block Resettable Iterator Fetching Thread.");
		this.blockFetcherThread.setUncaughtExceptionHandler(uceh);
		this.blockFetcherThread.start();
		
		// fetch the first block
		nextBlock();
	}

	/**
	 * This method closes the iterator and releases all resources. This method works both as a regular
	 * shutdown and as a canceling method.
	 */
	public void close() {
		if (this.closed) {
			return;
		}
		this.closed = true;
		
		// shutdown the block fetcher first
		if (this.blockFetcherThread != null) {
			this.blockFetcher.shutdown();
			this.blockFetcherThread.interrupt();
			this.blockFetcherThread = null;
		}
		
		// remove all blocks
		this.in = null;
		this.emptySegments.clear();
		this.filledBuffers.clear();
		
		// release the memory segment
		this.memoryManager.release(this.buffers);
		this.buffers.clear();
		
		LOG.debug("Iterator closed.");
	}

	
	/**
	 * 
	 */
	private final class BlockFetcher<R extends Record> implements Runnable
	{
		private final BlockingQueue<MemorySegment> requestQueue;

		private final BlockingQueue<Buffer.Input> finishedTasks;

		private final Reader<R> reader;
		
		private volatile boolean alive;

		public BlockFetcher(BlockingQueue<MemorySegment> inputQueue, BlockingQueue<Buffer.Input> outputQueue,
				Reader<R> recordReader)
		{
			this.requestQueue = inputQueue;
			this.finishedTasks = outputQueue;
			this.reader = recordReader;
			this.alive = true;
		}
		
		public void shutdown() {
			this.alive = false;
		}

		@Override
		public void run()
		{
			final Reader<R> reader = this.reader;
			R next = null;
			boolean allRead = false;
			
			while (this.alive && !allRead) {
				// wait for the next request
				MemorySegment request = null;
				try {
					request = this.requestQueue.take();
				}
				catch (InterruptedException iex) {
					if (this.alive) {
						throw new RuntimeException(iex);
					}
					else {
						return;
					}
				}
				// create an output buffer
				Buffer.Output out = new Buffer.Output(request);

				// write the last spilled element
				if (next != null) {
					out.write(next);
				}

				// now fetch elements from the reader until the memory segment is filled
				while (this.alive && reader.hasNext()) {
					try {
						next = reader.next();
					}
					catch (IOException ioex) {
						throw new RuntimeException(ioex);
					}
					catch (InterruptedException iex) {
						throw new RuntimeException(iex);
					}
					
					if (!out.write(next)) {
						break;
					}
				}

				int pos = out.getPosition();
				MemorySegment seg = out.dispose();
				
				// allocate a new input buffer for the segment and push it to the input queue
				Buffer.Input in = new Buffer.Input(seg);
				in.reset(pos);
				
				this.finishedTasks.add(in);
				
				if (!reader.hasNext()) {
					allRead = true;
				}
			} // end while alive
			
			if (!this.alive) {
				return;
			}
			
			// send an empty buffer to signal completion
			MemorySegment request = null;
			try {
				request = requestQueue.take();
			}
			catch (InterruptedException iex) {
				if (this.alive) {
					throw new RuntimeException(iex);
				}
				else {
					return;
				}
			}
			this.finishedTasks.add(new Buffer.Input(request)); // null signals completion
		}
	}

}
