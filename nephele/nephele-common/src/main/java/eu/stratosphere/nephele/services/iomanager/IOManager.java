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

import eu.stratosphere.nephele.services.iomanager.ChannelAccess.IORequest;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.UnboundMemoryBackedException;

/**
 * The facade for the provided IO manager services.
 * 
 * @author Alexander Alexandrov
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
		if (isClosed) {
			throw new IllegalStateException("IO-Manger is closed.");
		}
		
		return new ChannelReader(channelID, reader.requestQueue, createInputBuffers(freeSegments), deleteFileAfterRead);
	}
	
	/**
	 * Creates a block channel writer that writes to the given channel.
	 * 
	 * @param channelID The descriptor for the channel to write to.
	 * @param returnQueue The queue to put buffers of handled into.
	 * @return A block channel writer that writes to the given channel.
	 * @throws IOException Thrown, if the channel for the writer could not be opened.
	 */
	public BlockChannelWriter createBlockChannWriter(Channel.ID channelID,
										LinkedBlockingQueue<MemorySegment> returnQueue)
	throws IOException
	{
		if (isClosed) {
			throw new IllegalStateException("IO-Manger is closed.");
		}
		
		return new BlockChannelWriter(channelID, this.writer.requestQueue, returnQueue);
	}

	
	// ------------------------------------------------------------------------
	//       Utility methods for creating and binding / unbinding buffers
	// ------------------------------------------------------------------------
	
	/**
	 * Creates an input buffer around the given memory segment.
	 * 
	 * @return An input buffer storing its data in the given memory segment.
	 */
	public static Buffer.Input createInputBuffer(MemorySegment memory) {
		return new Buffer.Input(memory);
	}

	/**
	 * Creates an output buffer around the given memory segment.
	 * 
	 * @return An output buffer storing its data in the given memory segment.
	 */
	public static Buffer.Output createOutputBuffer(MemorySegment memory) {
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
	 */
	private static final class ReaderThread extends Thread
	{
		protected final RequestQueue<IORequest<Buffer.Input>> requestQueue;

		private volatile boolean alive;

		// ---------------------------------------------------------------------
		// Constructors / Destructors
		// ---------------------------------------------------------------------

		protected ReaderThread() {
			this.requestQueue = new RequestQueue<IORequest<Buffer.Input>>();
			this.alive = true;
		}
		
		/**
		 * Shuts the thread down. This operation does not wait for all pending requests to be served, halts the thread
		 * immediately. All buffers of pending requests are handed back to their channel readers and an exception is
		 * reported to them, declaring their request queue as closed.
		 */
		protected void shutdown() {
			if (alive) {
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
					IORequest<Buffer.Input> request = this.requestQueue.poll();
					request.channel.handleProcessedBuffer(request.buffer, ioex);
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
				IORequest<Buffer.Input> request = null;
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
					if (!request.buffer.memory.isFree()) {
						request.buffer.readFromChannel(request.channel.fileChannel);
					}
				}
				catch (IOException e) {
					ioex = e;
				}
				catch (Throwable t) {
					ioex = new IOException("The buffer could not be read: " + t.getMessage(), t);
				}

				// invoke the processed buffer handler of the request issuing reader object
				request.channel.handleProcessedBuffer(request.buffer, ioex);
			} // end while alive
		}
		
	} // end reading thread
	
	/**
	 * A worker thread that asynchronously writes the buffers to disk.
	 */
	private static final class WriterThread extends Thread
	{
		protected final RequestQueue<IORequest<Buffer.Output>> requestQueue;

		private volatile boolean alive;

		// ---------------------------------------------------------------------
		// Constructors / Destructors
		// ---------------------------------------------------------------------

		protected WriterThread() {
			this.requestQueue = new RequestQueue<IORequest<Buffer.Output>>();
			this.alive = true;
		}

		/**
		 * Shuts the thread down. This operation does not wait for all pending requests to be served, halts the thread
		 * immediately. All buffers of pending requests are handed back to their channel writers and an exception is
		 * reported to them, declaring their request queue as closed.
		 */
		protected void shutdown() {
			if (alive) {
				// shut down the thread
				try {
					this.alive = false;
					this.requestQueue.close();
					this.interrupt();
				}
				catch (Throwable t) {}
				
				// notify all pending write requests that the thread has been shut down
				IOException ioex = new IOException("Writer thread has been closed.");
				
				while (!this.requestQueue.isEmpty()) {
					IORequest<Buffer.Output> request = this.requestQueue.poll();
					request.channel.handleProcessedBuffer(request.buffer, ioex);
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
				
				IORequest<Buffer.Output> request = null;
				
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
					if (!request.buffer.memory.isFree()) {
						request.buffer.writeToChannel(request.channel.fileChannel);
					}
				}
				catch (IOException e) {
					ioex = e;
				}
				catch (Throwable t) {
					ioex = new IOException("The buffer could not be written: " + t.getMessage(), t);
				}

				// invoke the processed buffer handler of the request issuing writer object
				request.channel.handleProcessedBuffer(request.buffer, ioex);
			} // end while alive
		}
		
	}; // end writer thread
}
