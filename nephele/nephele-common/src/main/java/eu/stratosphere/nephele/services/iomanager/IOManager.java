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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.UnboundMemoryBackedException;

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
	 * The default temp path for anonymous Channels.
	 */
	private final String path;

	/**
	 * A random number generator for the anonymous ChannelIDs.
	 */
	private final Random random;

	/**
	 * The writer thread used for asynchronous block oriented channel writing.
	 */
	private final WriterThread writer;

	/**
	 * The reader thread used for asynchronous block oriented channel reading.
	 */
	private final ReaderThread reader;

	/**
	 * A boolean flag indicating whether the close() has already been invoked.
	 */
	private volatile boolean isClosed = false;

	
	// -------------------------------------------------------------------------
	//               Constructors / Destructors
	// -------------------------------------------------------------------------

	public IOManager() {
		this(System.getProperty("java.io.tmpdir"));
	}

	/**
	 * Constructs a new IOManager.
	 * 
	 * @param path
	 *        the basic directory path for files underlying anonymous
	 *        channels.
	 */
	public IOManager(String path) {
		this.path = path;
		this.random = new Random();
		this.writer = new WriterThread();
		this.reader = new ReaderThread();

		// start the ChannelWriter worker thread
		this.writer.setName("IOManager writer thread");
		this.writer.setDaemon(true);
		this.writer.setUncaughtExceptionHandler(this);
		this.writer.start();

		// start the ChannelReader worker thread
		this.reader.setName("IOManager reader thread");
		this.reader.setDaemon(true);
		this.reader.setUncaughtExceptionHandler(this);
		this.reader.start();
	}

	/**
	 * Close method. Shuts down the reader and writer threads immediately, not waiting for their
	 * pending requests to be served. This method waits until the threads have actually ceased their
	 * operation.
	 */
	public synchronized final void shutdown() {
		if (!isClosed) {
			isClosed = true;

			// close both threads by best effort and log problems
			try {
				writer.shutdown();
			}
			catch (Throwable t) {
				LOG.error("Error while shutting down IO Manager writing thread.", t);
			}
			
			try {
				reader.shutdown();
			}
			catch (Throwable t) {
				LOG.error("Error while shutting down IO Manager reading thread.", t);
			}
			
			try {
				this.writer.join();
				this.reader.join();
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
	public final boolean isProperlyShutDown() {
		return isClosed && 
			(this.writer.getState() == Thread.State.TERMINATED) && 
			(this.reader.getState() == Thread.State.TERMINATED);
	}

	/* (non-Javadoc)
	 * @see java.lang.Thread.UncaughtExceptionHandler#uncaughtException(java.lang.Thread, java.lang.Throwable)
	 */
	@Override
	public void uncaughtException(Thread t, Throwable e) {
		LOG.fatal("IO Thread '" + t.getName() + "' terminated due to an exception. Closing IO Manager.", e);
		shutdown();
		
	}

	// ------------------------------------------------------------------------
	//                          Channel Instantiations
	// ------------------------------------------------------------------------
	
	/**
	 * Creates a new {@link Channel.ID} in the default {@code path}.
	 * 
	 * @return
	 */
	public Channel.ID createChannel() {
		return createChannel(path);
	}

	/**
	 * Creates a new {@link Channel.ID} in the specified {@code path}.
	 * 
	 * @param path
	 * @return
	 */
	public Channel.ID createChannel(String path) {
		return new Channel.ID(path, random);
	}

	/**
	 * Creates a new {@link Channel.Enumerator} in the default {@code path}.
	 * 
	 * @return
	 */
	public Channel.Enumerator createChannelEnumerator() {
		return createChannelEnumerator(path);
	}

	/**
	 * Creates a new {@link Channel.Enumerator} in the specified {@code path}.
	 * 
	 * @param path
	 * @return
	 */
	public Channel.Enumerator createChannelEnumerator(String path) {
		return new Channel.Enumerator(path, random);
	}

	
	// ------------------------------------------------------------------------
	//                        Reader / Writer instantiations
	// ------------------------------------------------------------------------
	
	/**
	 * Creates a ChannelWriter for the anonymous file identified by the specified {@code channelID} using the provided
	 * {@code freeSegmens} as backing memory for an internal flow of output buffers.
	 * 
	 * @param channelID
	 * @param freeSegments
	 * @return
	 * @throws IOException
	 */
	public ChannelWriter createChannelWriter(Channel.ID channelID, Collection<MemorySegment> freeSegments)
	throws IOException
	{
		if (isClosed) {
			throw new IllegalStateException("IO-Manger is closed.");
		}
		
		return new ChannelWriter(channelID, writer.requestQueue, IOManager.createOutputBuffers(freeSegments), false);
	}

	/**
	 * Creates a ChannelWriter for the anonymous file identified by the specified {@code channelID} using the provided
	 * {@code memorySegments} as backing memory for an internal flow of output buffers. If the boolean variable {@code
	 * filled} is set, the content of the memorySegments is flushed to the file before reusing.
	 * 
	 * @param channelID
	 * @param freeSegments
	 * @param filled
	 * @return
	 * @throws IOException
	 */
	public ChannelWriter createChannelWriter(Channel.ID channelID, Collection<Buffer.Output> buffers, boolean filled)
	throws IOException
	{
		if (isClosed) {
			throw new IllegalStateException("IO-Manger is closed.");
		}
		
		return new ChannelWriter(channelID, writer.requestQueue, buffers, filled);
	}

	/**
	 * Creates a ChannelWriter for the anonymous file written on secondary storage and identified by the specified
	 * {@code channelID} using the provided {@code freeSegments} as backing memory for an internal flow of input
	 * buffers.
	 * 
	 * @param channelID
	 * @param freeSegments
	 * @return
	 * @throws IOException
	 */
	public ChannelReader createChannelReader(Channel.ID channelID, Collection<MemorySegment> freeSegments,
			boolean deleteFileAfterRead)
	throws IOException
	{
		if (this.isClosed) {
			throw new IllegalStateException("IO-Manger is closed.");
		}
		
		return new ChannelReader(channelID, reader.requestQueue, createInputBuffers(freeSegments), deleteFileAfterRead);
	}
	
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
			throw new IllegalStateException("IO-Manger is closed.");
		}
		
		return new BlockChannelWriter(channelID, this.writer.requestQueue, returnQueue);
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
			throw new IllegalStateException("IO-Manger is closed.");
		}
		
		return new BlockChannelReader(channelID, this.reader.requestQueue, returnQueue);
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
			throw new IllegalStateException("IO-Manger is closed.");
		}
		
		return new BulkBlockChannelReader(channelID, this.reader.requestQueue, targetSegments, numBlocks);
	}

	
	// ------------------------------------------------------------------------
	//       Utility methods for creating and binding / unbinding buffers
	// ------------------------------------------------------------------------
	
	/**
	 * Creates an input buffer around the given memory segment.
	 * 
	 * @return An input buffer storing its data in the given memory segment.
	 */
	public static Buffer.Input createInputBuffer(MemorySegment memory)
	{
		return new Buffer.Input(memory);
	}

	/**
	 * Creates an output buffer around the given memory segment.
	 * 
	 * @return An output buffer storing its data in the given memory segment.
	 */
	public static Buffer.Output createOutputBuffer(MemorySegment memory)
	{
		return new Buffer.Output(memory);
	}

	/**
	 * Factory method for input buffers.
	 * 
	 * @param freeSegments The memory segments around which to create the input buffers.
	 * @return An unsynchronized list of initialized input buffers.
	 */
	public static List<Buffer.Input> createInputBuffers(Collection<MemorySegment> freeSegments)
	{
		ArrayList<Buffer.Input> buffers = new ArrayList<Buffer.Input>(freeSegments.size());

		for (MemorySegment segment : freeSegments) {
			Buffer.Input buffer = createInputBuffer(segment);
			buffers.add(buffer);
		}
		return buffers;
	}
	
	/**
	 * Factory method for output buffers.
	 * 
	 * @param freeSegments The memory segments around which to create the output buffers.
	 * @return An unsynchronized list of initialized output buffers.
	 */
	public static List<Buffer.Output> createOutputBuffers(Collection<MemorySegment> freeSegments)
	{
		ArrayList<Buffer.Output> buffers = new ArrayList<Buffer.Output>(freeSegments.size());

		for (MemorySegment segment : freeSegments) {
			Buffer.Output buffer = createOutputBuffer(segment);
			buffers.add(buffer);
		}
		return buffers;
	}

	/**
	 * Unbinds the collection of IO buffers.
	 * 
	 * @param buffers The buffers to unbind.
	 * @return A list containing the freed memory segments.
	 * @throws UnboundMemoryBackedException Thrown, if the collection contains an unbound buffer.
	 */
	public static List<MemorySegment> unbindBuffers(Collection<? extends Buffer> buffers) {
		ArrayList<MemorySegment> freeSegments = new ArrayList<MemorySegment>(buffers.size());

		for (Buffer buffer : buffers) {
			freeSegments.add(buffer.dispose());
		}

		return freeSegments;
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
				
				// notify all pending write requests that the thread has been shut down
				IOException ioex = new IOException("Reading thread has been closed.");
				
				while (!this.requestQueue.isEmpty()) {
					ReadRequest request = this.requestQueue.poll();
					request.requestDone(ioex);
				}
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
				}

				// invoke the processed buffer handler of the request issuing writer object
				request.requestDone(ioex);
			} // end while alive
		}
		
	}; // end writer thread
}
