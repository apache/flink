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

package eu.stratosphere.nephele.services.iomanager;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.List;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.services.memorymanager.MemorySegment;

/**
 * The facade for the provided I/O manager services.
 * 
 * @author Alexander Alexandrov
 * @author Stephan Ewen
 */
public final class IOManager implements UncaughtExceptionHandler
{
	/**
	 * Logging.
	 */
	private static final Log LOG = LogFactory.getLog(IOManager.class);

	/**
	 * The default temp paths for anonymous Channels.
	 */
	private final String[] paths;

	/**
	 * A random number generator for the anonymous ChannelIDs.
	 */
	private final Random random;

	/**
	 * The writer thread used for asynchronous block oriented channel writing.
	 */
	private final WriterThread[] writers;

	/**
	 * The reader threads used for asynchronous block oriented channel reading.
	 */
	private final ReaderThread[] readers;
	
	/**
	 * The number of the next path to use.
	 */
	private volatile int nextPath;

	/**
	 * A boolean flag indicating whether the close() has already been invoked.
	 */
	private volatile boolean isClosed = false;

	
	// -------------------------------------------------------------------------
	//               Constructors / Destructors
	// -------------------------------------------------------------------------

	/**
	 * Constructs a new IOManager, writing channels to the system directory.
	 */
	public IOManager() {
		this(System.getProperty("java.io.tmpdir"));
	}
	
	/**
	 * Constructs a new IOManager.
	 * 
	 * @param path The base directory path for files underlying channels.
	 */
	public IOManager(String tempDir) {
		this(new String[] {tempDir});
	}

	/**
	 * Constructs a new IOManager.
	 * 
	 * @param path
	 *        the basic directory path for files underlying anonymous
	 *        channels.
	 */
	public IOManager(String[] paths)
	{
		this.paths = paths;
		this.random = new Random();
		this.nextPath = 0;
		
		// start a write worker thread for each directory
		this.writers = new WriterThread[paths.length];
		for (int i = 0; i < this.writers.length; i++) {
			final WriterThread t = new WriterThread();
			this.writers[i] = t;
			t.setName("IOManager writer thread #" + (i + 1));
			t.setDaemon(true);
			t.setUncaughtExceptionHandler(this);
			t.start();
		}

		// start a reader worker thread for each directory
		this.readers = new ReaderThread[paths.length];
		for (int i = 0; i < this.readers.length; i++) {
			final ReaderThread t = new ReaderThread();
			this.readers[i] = t;
			t.setName("IOManager reader thread #" + (i + 1));
			t.setDaemon(true);
			t.setUncaughtExceptionHandler(this);
			t.start();
		}
	}

	/**
	 * Close method. Shuts down the reader and writer threads immediately, not waiting for their
	 * pending requests to be served. This method waits until the threads have actually ceased their
	 * operation.
	 */
	public synchronized final void shutdown()
	{
		if (!this.isClosed) {
			this.isClosed = true;

			// close writing and reading threads with best effort and log problems
			
			// --------------------------------- writer shutdown ----------------------------------			
			for (int i = 0; i < this.readers.length; i++) {
				try {
					this.writers[i].shutdown();
				}
				catch (Throwable t) {
					LOG.error("Error while shutting down IO Manager writer thread.", t);
				}
			}

			// --------------------------------- reader shutdown ----------------------------------
			for (int i = 0; i < this.readers.length; i++) {
				try {
					this.readers[i].shutdown();
				}
				catch (Throwable t) {
					LOG.error("Error while shutting down IO Manager reader thread.", t);
				}
			}
			
			// ------------------------ wait until shutdown is complete ---------------------------
			try {
				for (int i = 0; i < this.readers.length; i++) {
					this.writers[i].join();
				}
				for (int i = 0; i < this.readers.length; i++) {
					this.readers[i].join();
				}
			}
			catch (InterruptedException iex) {}
		}
	}
	
	/**
	 * Utility method to check whether the IO manager has been properly shut down. The IO manager is considered
	 * to be properly shut down when it is closed and its threads have ceased operation.
	 * 
	 * @return True, if the IO manager has properly shut down, false otherwise.
	 */
	public final boolean isProperlyShutDown()
	{
		boolean readersShutDown = true;
		for (int i = 0; i < this.readers.length; i++) {
			readersShutDown &= this.readers[i].getState() == Thread.State.TERMINATED;
		}
		
		boolean writersShutDown = true;
		for (int i = 0; i < this.writers.length; i++) {
			readersShutDown &= this.writers[i].getState() == Thread.State.TERMINATED;
		}
		
		return this.isClosed && writersShutDown && readersShutDown;
	}

	/* (non-Javadoc)
	 * @see java.lang.Thread.UncaughtExceptionHandler#uncaughtException(java.lang.Thread, java.lang.Throwable)
	 */
	@Override
	public void uncaughtException(Thread t, Throwable e)
	{
		LOG.fatal("IO Thread '" + t.getName() + "' terminated due to an exception. Closing I/O Manager.", e);
		shutdown();	
	}

	// ------------------------------------------------------------------------
	//                          Channel Instantiations
	// ------------------------------------------------------------------------
	
	/**
	 * Creates a new {@link Channel.ID} in one of the temp directories. Multiple
	 * invocations of this method spread the channels evenly across the different directories.
	 * 
	 * @return A channel to a temporary directory.
	 */
	public Channel.ID createChannel()
	{
		final int num = getNextPathNum();
		return new Channel.ID(this.paths[num], num, this.random);
	}

	/**
	 * Creates a new {@link Channel.Enumerator}, spreading the channels in a round-robin fashion
	 * across the temporary file directories.
	 * 
	 * @return An enumerator for channels.
	 */
	public Channel.Enumerator createChannelEnumerator()
	{
		return new Channel.Enumerator(this.paths, this.random);
	}

	
	// ------------------------------------------------------------------------
	//                        Reader / Writer instantiations
	// ------------------------------------------------------------------------
	
	/**
	 * Creates a block channel writer that writes to the given channel. The writer writes asynchronously (write-behind),
	 * accepting write request, carrying them out at some time and returning the written segment to the given queue
	 * afterwards.
	 * 
	 * @param channelID The descriptor for the channel to write to.
	 * @param returnQueue The queue to put the written buffers into.
	 * @return A block channel writer that writes to the given channel.
	 * @throws IOException Thrown, if the channel for the writer could not be opened.
	 */
	public BlockChannelWriter createBlockChannelWriter(Channel.ID channelID,
								LinkedBlockingQueue<MemorySegment> returnQueue)
	throws IOException
	{
		if (this.isClosed) {
			throw new IllegalStateException("I/O-Manger is closed.");
		}
		
		return new BlockChannelWriter(channelID, this.writers[channelID.getThreadNum()].requestQueue, returnQueue, 1);
	}
	
	/**
	 * Creates a block channel writer that writes to the given channel. The writer writes asynchronously (write-behind),
	 * accepting write request, carrying them out at some time and returning the written segment to the given queue
	 * afterwards.
	 * <p>
	 * The writer will collect a specified number of write requests and carry them out
	 * in one, effectively writing one block in the size of multiple memory pages.
	 * Note that this means that no memory segment will reach the return queue before
	 * the given number of requests are collected, so the number of buffers used with
	 * the writer should be greater than the number of requests to combine. Ideally,
	 * the number of memory segments used is a multiple of the number of requests to
	 * combine.
	 * 
	 * @param channelID The descriptor for the channel to write to.
	 * @param returnQueue The queue to put the written buffers into.
	 * @param numRequestsToCombine The number of write requests to combine to one I/O request.
	 * @return A block channel writer that writes to the given channel.
	 * @throws IOException Thrown, if the channel for the writer could not be opened.
	 */
	public BlockChannelWriter createBlockChannelWriter(Channel.ID channelID,
								LinkedBlockingQueue<MemorySegment> returnQueue, int numRequestsToCombine)
	throws IOException
	{
		if (this.isClosed) {
			throw new IllegalStateException("I/O-Manger is closed.");
		}
		
		return new BlockChannelWriter(channelID, this.writers[channelID.getThreadNum()].requestQueue, returnQueue, numRequestsToCombine);
	}
	
	/**
	 * Creates a block channel writer that writes to the given channel. The writer writes asynchronously (write-behind),
	 * accepting write request, carrying them out at some time and returning the written segment its return queue afterwards.
	 * 
	 * @param channelID The descriptor for the channel to write to.
	 * @return A block channel writer that writes to the given channel.
	 * @throws IOException Thrown, if the channel for the writer could not be opened.
	 */
	public BlockChannelWriter createBlockChannelWriter(Channel.ID channelID)
	throws IOException
	{
		if (this.isClosed) {
			throw new IllegalStateException("I/O-Manger is closed.");
		}
		
		return new BlockChannelWriter(channelID, this.writers[channelID.getThreadNum()].requestQueue, new LinkedBlockingQueue<MemorySegment>(), 1);
	}
	
	/**
	 * Creates a block channel writer that writes to the given channel. The writer writes asynchronously (write-behind),
	 * accepting write request, carrying them out at some time and returning the written segment its return queue afterwards.
	 * <p>
	 * The writer will collect a specified number of write requests and carry them out
	 * in one, effectively writing one block in the size of multiple memory pages.
	 * Note that this means that no memory segment will reach the return queue before
	 * the given number of requests are collected, so the number of buffers used with
	 * the writer should be greater than the number of requests to combine. Ideally,
	 * the number of memory segments used is a multiple of the number of requests to
	 * combine.
	 * 
	 * @param channelID The descriptor for the channel to write to.
	 * @param numRequestsToCombine The number of write requests to combine to one I/O request.
	 * @return A block channel writer that writes to the given channel.
	 * @throws IOException Thrown, if the channel for the writer could not be opened.
	 */
	public BlockChannelWriter createBlockChannelWriter(Channel.ID channelID, int numRequestsToCombine)
	throws IOException
	{
		if (this.isClosed) {
			throw new IllegalStateException("I/O-Manger is closed.");
		}
		
		return new BlockChannelWriter(channelID, this.writers[channelID.getThreadNum()].requestQueue, new LinkedBlockingQueue<MemorySegment>(), numRequestsToCombine);
	}
	
	/**
	 * Creates a block channel reader that reads blocks from the given channel. The reader reads asynchronously,
	 * such that a read request is accepted, carried out at some (close) point in time, and the full segment
	 * is pushed to the given queue.
	 * 
	 * @param channelID The descriptor for the channel to write to.
	 * @param returnQueue The queue to put the full buffers into.
	 * @return A block channel reader that reads from the given channel.
	 * @throws IOException Thrown, if the channel for the reader could not be opened.
	 */
	public BlockChannelReader createBlockChannelReader(Channel.ID channelID,
										LinkedBlockingQueue<MemorySegment> returnQueue)
	throws IOException
	{
		if (this.isClosed) {
			throw new IllegalStateException("I/O-Manger is closed.");
		}
		
		return new BlockChannelReader(channelID, this.readers[channelID.getThreadNum()].requestQueue, returnQueue, 1);
	}
	
	/**
	 * Creates a block channel reader that reads blocks from the given channel. The reader reads asynchronously,
	 * such that a read request is accepted, carried out at some (close) point in time, and the full segment
	 * is pushed to the given queue.
	 * <p>
	 * The reader will collect a specified number of read requests and carry them out
	 * in one, effectively reading one block in the size of multiple memory pages.
	 * Note that this means that no memory segment will reach the return queue before
	 * the given number of requests are collected, so the number of buffers used with
	 * the reader should be greater than the number of requests to combine. Ideally,
	 * the number of memory segments used is a multiple of the number of requests to
	 * combine.
	 * 
	 * @param channelID The descriptor for the channel to write to.
	 * @param returnQueue The queue to put the full buffers into.
	 * @param numRequestsToCombine The number of read requests to combine to one I/O request.
	 * @return A block channel reader that reads from the given channel.
	 * @throws IOException Thrown, if the channel for the reader could not be opened.
	 */
	public BlockChannelReader createBlockChannelReader(Channel.ID channelID,
					LinkedBlockingQueue<MemorySegment> returnQueue, int numRequestsToCombine)
	throws IOException
	{
		if (this.isClosed) {
			throw new IllegalStateException("I/O-Manger is closed.");
		}
		
		return new BlockChannelReader(channelID, this.readers[channelID.getThreadNum()].requestQueue, returnQueue, numRequestsToCombine);
	}
	
	/**
	 * Creates a block channel reader that reads blocks from the given channel. The reader reads asynchronously,
	 * such that a read request is accepted, carried out at some (close) point in time, and the full segment
	 * is pushed to the reader's return queue.
	 * 
	 * @param channelID The descriptor for the channel to write to.
	 * @return A block channel reader that reads from the given channel.
	 * @throws IOException Thrown, if the channel for the reader could not be opened.
	 */
	public BlockChannelReader createBlockChannelReader(Channel.ID channelID)
	throws IOException
	{
		if (this.isClosed) {
			throw new IllegalStateException("I/O-Manger is closed.");
		}
		
		return new BlockChannelReader(channelID, this.readers[channelID.getThreadNum()].requestQueue, new LinkedBlockingQueue<MemorySegment>(), 1);
	}
	
	/**
	 * Creates a block channel reader that reads blocks from the given channel. The reader reads asynchronously,
	 * such that a read request is accepted, carried out at some (close) point in time, and the full segment
	 * is pushed to the reader's return queue.
	 * <p>
	 * The reader will collect a specified number of read requests and carry them out
	 * in one, effectively reading one block in the size of multiple memory pages.
	 * Note that this means that no memory segment will reach the return queue before
	 * the given number of requests are collected, so the number of buffers used with
	 * the reader should be greater than the number of requests to combine. Ideally,
	 * the number of memory segments used is a multiple of the number of requests to
	 * combine.
	 * 
	 * @param channelID The descriptor for the channel to write to.
	 * @param numRequestsToCombine The number of write requests to combine to one I/O request.
	 * @return A block channel reader that reads from the given channel.
	 * @throws IOException Thrown, if the channel for the reader could not be opened.
	 */
	public BlockChannelReader createBlockChannelReader(Channel.ID channelID, int numRequestsToCombine)
	throws IOException
	{
		if (this.isClosed) {
			throw new IllegalStateException("I/O-Manger is closed.");
		}
		
		return new BlockChannelReader(channelID, this.readers[channelID.getThreadNum()].requestQueue, 
			new LinkedBlockingQueue<MemorySegment>(), numRequestsToCombine);
	}
	
	/**
	 * Creates a block channel reader that reads all blocks from the given channel directly in one bulk.
	 * The reader draws segments to read the blocks into from a supplied list, which must contain as many
	 * segments as the channel has blocks. After the reader is done, the list with the full segments can be 
	 * obtained from the reader.
	 * <p>
	 * If a channel is not to be read in one bulk, but in multiple smaller batches, a  
	 * {@link BlockChannelReader} should be used.
	 * 
	 * @param channelID The descriptor for the channel to write to.
	 * @param targetSegments The list to take the segments from into which to read the data.
	 * @param numBlocks The number of blocks in the channel to read.
	 * @return A block channel reader that reads from the given channel.
	 * @throws IOException Thrown, if the channel for the reader could not be opened.
	 */
	public BulkBlockChannelReader createBulkBlockChannelReader(Channel.ID channelID,
			List<MemorySegment> targetSegments,	int numBlocks)
	throws IOException
	{
		if (this.isClosed) {
			throw new IllegalStateException("I/O-Manger is closed.");
		}
		
		return new BulkBlockChannelReader(channelID, this.readers[channelID.getThreadNum()].requestQueue, targetSegments, numBlocks);
	}
	
	// ========================================================================
	//                             Utilities
	// ========================================================================
	
	private final int getNextPathNum()
	{
		final int next = this.nextPath;
		final int newNext = next + 1;
		this.nextPath = newNext >= this.paths.length ? 0 : newNext;
		return next;
	}
	
	
	// ========================================================================
	//                          I/O Worker Threads
	// ========================================================================

	/**
	 * A worker thread for asynchronous read.
	 * 
	 * @author Alexander Alexandrov
	 * @author Stephan Ewen
	 */
	private static final class ReaderThread extends Thread
	{
		protected final RequestQueue<ReadRequest> requestQueue;

		private volatile boolean alive;

		// ---------------------------------------------------------------------
		// Constructors / Destructors
		// ---------------------------------------------------------------------
		
		protected ReaderThread()
		{
			this.requestQueue = new RequestQueue<ReadRequest>();
			this.alive = true;
		}
		
		/**
		 * Shuts the thread down. This operation does not wait for all pending requests to be served, halts the thread
		 * immediately. All buffers of pending requests are handed back to their channel readers and an exception is
		 * reported to them, declaring their request queue as closed.
		 */
		protected void shutdown()
		{
			if (this.alive) {
				// shut down the thread
				try {
					this.alive = false;
					this.requestQueue.close();
					this.interrupt();
				}
				catch (Throwable t) {}
			}
			
			// notify all pending write requests that the thread has been shut down
			IOException ioex = new IOException("IO-Manager has been closed.");
			
			while (!this.requestQueue.isEmpty()) {
				ReadRequest request = this.requestQueue.poll();
				request.requestDone(ioex);
			}
		}

		// ---------------------------------------------------------------------
		//                             Main loop
		// ---------------------------------------------------------------------

		@Override
		public void run()
		{
			while (this.alive)
			{
				
				// get the next buffer. ignore interrupts that are not due to a shutdown.
				ReadRequest request = null;
				while (request == null) {
					try {
						request = this.requestQueue.take();
					}
					catch (InterruptedException iex) {
						if (!this.alive) {
							// exit
							return;
						}
					}
				}
				
				// remember any IO exception that occurs, so it can be reported to the writer
				IOException ioex = null;

				try {
					// read buffer from the specified channel
					request.read();
				}
				catch (IOException e) {
					ioex = e;
				}
				catch (Throwable t) {
					ioex = new IOException("The buffer could not be read: " + t.getMessage(), t);
					IOManager.LOG.error("I/O reading thread encountered an error" + 
						t.getMessage() == null ? "." : ": ", t);
				}

				// invoke the processed buffer handler of the request issuing reader object
				request.requestDone(ioex);
			} // end while alive
		}
		
	} // end reading thread
	
	/**
	 * A worker thread that asynchronously writes the buffers to disk.
	 */
	private static final class WriterThread extends Thread
	{
		protected final RequestQueue<WriteRequest> requestQueue;

		private volatile boolean alive;

		// ---------------------------------------------------------------------
		// Constructors / Destructors
		// ---------------------------------------------------------------------

		protected WriterThread()
		{
			this.requestQueue = new RequestQueue<WriteRequest>();
			this.alive = true;
		}

		/**
		 * Shuts the thread down. This operation does not wait for all pending requests to be served, halts the thread
		 * immediately. All buffers of pending requests are handed back to their channel writers and an exception is
		 * reported to them, declaring their request queue as closed.
		 */
		protected void shutdown()
		{
			if (this.alive) {
				// shut down the thread
				try {
					this.alive = false;
					this.requestQueue.close();
					this.interrupt();
				}
				catch (Throwable t) {}
				
				// notify all pending write requests that the thread has been shut down
				IOException ioex = new IOException("Writer thread has been closed.");
				
				while (!this.requestQueue.isEmpty())
				{
					WriteRequest request = this.requestQueue.poll();
					request.requestDone(ioex);
				}
			}
		}

		// ---------------------------------------------------------------------
		// Main loop
		// ---------------------------------------------------------------------

		@Override
		public void run()
		{
			while (this.alive) {
				
				WriteRequest request = null;
				
				// get the next buffer. ignore interrupts that are not due to a shutdown.
				while (request == null) {
					try {
						request = requestQueue.take();
					}
					catch (InterruptedException iex) {
						if (!this.alive) {
							// exit
							return;
						}
					}
				}
				
				// remember any IO exception that occurs, so it can be reported to the writer
				IOException ioex = null;
				
				try {
					// write buffer to the specified channel
					request.write();
				}
				catch (IOException e) {
					ioex = e;
				}
				catch (Throwable t) {
					ioex = new IOException("The buffer could not be written: " + t.getMessage(), t);
					IOManager.LOG.error("I/O reading thread encountered an error" + 
						t.getMessage() == null ? "." : ": ", t);
				}

				// invoke the processed buffer handler of the request issuing writer object
				request.requestDone(ioex);
			} // end while alive
		}
		
	}; // end writer thread
}
