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

package org.apache.flink.runtime.io.disk.iomanager;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.ShutdownHookUtil;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A version of the {@link IOManager} that uses asynchronous I/O.
 */
public class IOManagerAsync extends IOManager implements UncaughtExceptionHandler {
	
	/** The writer threads used for asynchronous block oriented channel writing. */
	private final WriterThread[] writers;

	/** The reader threads used for asynchronous block oriented channel reading. */
	private final ReaderThread[] readers;

	/** Flag to signify that the IOManager has been shut down already */
	private final AtomicBoolean isShutdown = new AtomicBoolean();

	/** Shutdown hook to make sure that the directories are removed on exit */
	private final Thread shutdownHook;

	
	// -------------------------------------------------------------------------
	//               Constructors / Destructors
	// -------------------------------------------------------------------------

	/**
	 * Constructs a new asynchronous I/O manager, writing files to the system 's temp directory.
	 */
	public IOManagerAsync() {
		this(EnvironmentInformation.getTemporaryFileDirectory());
	}
	
	/**
	 * Constructs a new asynchronous I/O manager, writing file to the given directory.
	 * 
	 * @param tempDir The directory to write temporary files to.
	 */
	public IOManagerAsync(String tempDir) {
		this(new String[] {tempDir});
	}

	/**
	 * Constructs a new asynchronous I/O manager, writing file round robin across the given directories.
	 * 
	 * @param tempDirs The directories to write temporary files to.
	 */
	public IOManagerAsync(String[] tempDirs) {
		super(tempDirs);
		
		// start a write worker thread for each directory
		this.writers = new WriterThread[tempDirs.length];
		for (int i = 0; i < this.writers.length; i++) {
			final WriterThread t = new WriterThread();
			this.writers[i] = t;
			t.setName("IOManager writer thread #" + (i + 1));
			t.setDaemon(true);
			t.setUncaughtExceptionHandler(this);
			t.start();
		}

		// start a reader worker thread for each directory
		this.readers = new ReaderThread[tempDirs.length];
		for (int i = 0; i < this.readers.length; i++) {
			final ReaderThread t = new ReaderThread();
			this.readers[i] = t;
			t.setName("IOManager reader thread #" + (i + 1));
			t.setDaemon(true);
			t.setUncaughtExceptionHandler(this);
			t.start();
		}

		// install a shutdown hook that makes sure the temp directories get deleted
		this.shutdownHook = ShutdownHookUtil.addShutdownHook(this::close, getClass().getSimpleName(), LOG);
	}

	/**
	 * Close method. Shuts down the reader and writer threads immediately, not waiting for their
	 * pending requests to be served. This method waits until the threads have actually ceased their
	 * operation.
	 */
	@Override
	public void close() throws Exception {
		// mark shut down and exit if it already was shut down
		if (!isShutdown.compareAndSet(false, true)) {
			return;
		}

		// Remove shutdown hook to prevent resource leaks
		ShutdownHookUtil.removeShutdownHook(shutdownHook, getClass().getSimpleName(), LOG);


		if (LOG.isDebugEnabled()) {
			LOG.debug("Shutting down I/O manager.");
		}

		// close writing and reading threads with best effort and log problems
		// first notify all to close, then wait until all are closed

		List<AutoCloseable> closeables = new ArrayList<>(writers.length + readers.length + 2);

		for (WriterThread wt : writers) {
			closeables.add(getWriterThreadCloser(wt));
		}

		for (ReaderThread rt : readers) {
			closeables.add(getReaderThreadCloser(rt));
		}

		closeables.add(() -> {
			try {
				for (WriterThread wt : writers) {
					wt.join();
				}
				for (ReaderThread rt : readers) {
					rt.join();
				}
			} catch (InterruptedException ie) {
				Thread.currentThread().interrupt();
			}
		});

		// make sure we call the super implementation in any case and at the last point,
		// because this will clean up the I/O directories
		closeables.add(super::close);

		IOUtils.closeAll(closeables);
	}

	private static AutoCloseable getWriterThreadCloser(WriterThread thread) {
		return () -> {
			try {
				thread.shutdown();
			} catch (Throwable t) {
				throw new IOException("Error while shutting down IO Manager writer thread.", t);
			}
		};
	}

	private static AutoCloseable getReaderThreadCloser(ReaderThread thread) {
		return () -> {
			try {
				thread.shutdown();
			} catch (Throwable t) {
				throw new IOException("Error while shutting down IO Manager reader thread.", t);
			}
		};
	}

	@Override
	public void uncaughtException(Thread t, Throwable e) {
		LOG.error("IO Thread '" + t.getName() + "' terminated due to an exception. Shutting down I/O Manager.", e);
		try {
			close();
		} catch (Exception ex) {
			LOG.warn("IOManagerAsync did not shut down properly.", ex);
		}
	}
	
	// ------------------------------------------------------------------------
	//                        Reader / Writer instantiations
	// ------------------------------------------------------------------------
	
	@Override
	public BlockChannelWriter<MemorySegment> createBlockChannelWriter(FileIOChannel.ID channelID,
								LinkedBlockingQueue<MemorySegment> returnQueue) throws IOException
	{
		checkState(!isShutdown.get(), "I/O-Manager is shut down.");
		return new AsynchronousBlockWriter(channelID, this.writers[channelID.getThreadNum()].requestQueue, returnQueue);
	}
	
	@Override
	public BlockChannelWriterWithCallback<MemorySegment> createBlockChannelWriter(FileIOChannel.ID channelID, RequestDoneCallback<MemorySegment> callback) throws IOException {
		checkState(!isShutdown.get(), "I/O-Manager is shut down.");
		return new AsynchronousBlockWriterWithCallback(channelID, this.writers[channelID.getThreadNum()].requestQueue, callback);
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
	@Override
	public BlockChannelReader<MemorySegment> createBlockChannelReader(FileIOChannel.ID channelID,
										LinkedBlockingQueue<MemorySegment> returnQueue) throws IOException
	{
		checkState(!isShutdown.get(), "I/O-Manager is shut down.");
		return new AsynchronousBlockReader(channelID, this.readers[channelID.getThreadNum()].requestQueue, returnQueue);
	}

	@Override
	public BufferFileWriter createBufferFileWriter(FileIOChannel.ID channelID) throws IOException {
		checkState(!isShutdown.get(), "I/O-Manager is shut down.");

		return new AsynchronousBufferFileWriter(channelID, writers[channelID.getThreadNum()].requestQueue);
	}

	@Override
	public BufferFileReader createBufferFileReader(FileIOChannel.ID channelID, RequestDoneCallback<Buffer> callback) throws IOException {
		checkState(!isShutdown.get(), "I/O-Manager is shut down.");

		return new AsynchronousBufferFileReader(channelID, readers[channelID.getThreadNum()].requestQueue, callback);
	}

	@Override
	public BufferFileSegmentReader createBufferFileSegmentReader(FileIOChannel.ID channelID, RequestDoneCallback<FileSegment> callback) throws IOException {
		checkState(!isShutdown.get(), "I/O-Manager is shut down.");

		return new AsynchronousBufferFileSegmentReader(channelID, readers[channelID.getThreadNum()].requestQueue, callback);
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
	@Override
	public BulkBlockChannelReader createBulkBlockChannelReader(FileIOChannel.ID channelID,
			List<MemorySegment> targetSegments, int numBlocks) throws IOException
	{
		checkState(!isShutdown.get(), "I/O-Manager is shut down.");
		return new AsynchronousBulkBlockReader(channelID, this.readers[channelID.getThreadNum()].requestQueue, targetSegments, numBlocks);
	}
	
	// -------------------------------------------------------------------------
	//                             For Testing
	// -------------------------------------------------------------------------
	
	RequestQueue<ReadRequest> getReadRequestQueue(FileIOChannel.ID channelID) {
		return this.readers[channelID.getThreadNum()].requestQueue;
	}
	
	RequestQueue<WriteRequest> getWriteRequestQueue(FileIOChannel.ID channelID) {
		return this.writers[channelID.getThreadNum()].requestQueue;
	}

	// -------------------------------------------------------------------------
	//                           I/O Worker Threads
	// -------------------------------------------------------------------------
	
	/**
	 * A worker thread for asynchronous reads.
	 */
	private static final class ReaderThread extends Thread {
		
		protected final RequestQueue<ReadRequest> requestQueue;

		private volatile boolean alive;

		// ---------------------------------------------------------------------
		// Constructors / Destructors
		// ---------------------------------------------------------------------
		
		protected ReaderThread() {
			this.requestQueue = new RequestQueue<ReadRequest>();
			this.alive = true;
		}
		
		/**
		 * Shuts the thread down. This operation does not wait for all pending requests to be served, halts the thread
		 * immediately. All buffers of pending requests are handed back to their channel readers and an exception is
		 * reported to them, declaring their request queue as closed.
		 */
		protected void shutdown() {
			synchronized (this) {
				if (alive) {
					alive = false;
					requestQueue.close();
					interrupt();
				}

				try {
					join(1000);
				}
				catch (InterruptedException ignored) {}
				
				// notify all pending write requests that the thread has been shut down
				IOException ioex = new IOException("IO-Manager has been closed.");
					
				while (!this.requestQueue.isEmpty()) {
					ReadRequest request = this.requestQueue.poll();
					if (request != null) {
						try {
							request.requestDone(ioex);
						}
						catch (Throwable t) {
							IOManagerAsync.LOG.error("The handler of the request complete callback threw an exception"
									+ (t.getMessage() == null ? "." : ": " + t.getMessage()), t);
						}
					}
				}
			}
		}

		// ---------------------------------------------------------------------
		//                             Main loop
		// ---------------------------------------------------------------------

		@Override
		public void run() {
			
			while (alive) {
				
				// get the next buffer. ignore interrupts that are not due to a shutdown.
				ReadRequest request = null;
				while (alive && request == null) {
					try {
						request = this.requestQueue.take();
					}
					catch (InterruptedException e) {
						if (!this.alive) {
							return;
						} else {
							IOManagerAsync.LOG.warn(Thread.currentThread() + " was interrupted without shutdown.");
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
					IOManagerAsync.LOG.error("I/O reading thread encountered an error" + (t.getMessage() == null ? "." : ": " + t.getMessage()), t);
				}

				// invoke the processed buffer handler of the request issuing reader object
				try {
					request.requestDone(ioex);
				}
				catch (Throwable t) {
					IOManagerAsync.LOG.error("The handler of the request-complete-callback threw an exception" + (t.getMessage() == null ? "." : ": " + t.getMessage()), t);
				}
			} // end while alive
		}
		
	} // end reading thread
	
	/**
	 * A worker thread that asynchronously writes the buffers to disk.
	 */
	private static final class WriterThread extends Thread {
		
		protected final RequestQueue<WriteRequest> requestQueue;

		private volatile boolean alive;

		// ---------------------------------------------------------------------
		// Constructors / Destructors
		// ---------------------------------------------------------------------

		protected WriterThread() {
			this.requestQueue = new RequestQueue<WriteRequest>();
			this.alive = true;
		}

		/**
		 * Shuts the thread down. This operation does not wait for all pending requests to be served, halts the thread
		 * immediately. All buffers of pending requests are handed back to their channel writers and an exception is
		 * reported to them, declaring their request queue as closed.
		 */
		protected void shutdown() {
			synchronized (this) {
				if (alive) {
					alive = false;
					requestQueue.close();
					interrupt();
				}

				try {
					join(1000);
				}
				catch (InterruptedException ignored) {}
				
				// notify all pending write requests that the thread has been shut down
				IOException ioex = new IOException("IO-Manager has been closed.");
					
				while (!this.requestQueue.isEmpty()) {
					WriteRequest request = this.requestQueue.poll();
					if (request != null) {
						try {
							request.requestDone(ioex);
						}
						catch (Throwable t) {
							IOManagerAsync.LOG.error("The handler of the request complete callback threw an exception"
									+ (t.getMessage() == null ? "." : ": " + t.getMessage()), t);
						}
					}
				}
			}
		}

		// ---------------------------------------------------------------------
		// Main loop
		// ---------------------------------------------------------------------

		@Override
		public void run() {
			
			while (this.alive) {
				
				WriteRequest request = null;
				
				// get the next buffer. ignore interrupts that are not due to a shutdown.
				while (alive && request == null) {
					try {
						request = requestQueue.take();
					}
					catch (InterruptedException e) {
						if (!this.alive) {
							return;
						} else {
							IOManagerAsync.LOG.warn(Thread.currentThread() + " was interrupted without shutdown.");
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
					IOManagerAsync.LOG.error("I/O writing thread encountered an error" + (t.getMessage() == null ? "." : ": " + t.getMessage()), t);
				}

				// invoke the processed buffer handler of the request issuing writer object
				try {
					request.requestDone(ioex);
				}
				catch (Throwable t) {
					IOManagerAsync.LOG.error("The handler of the request-complete-callback threw an exception" + (t.getMessage() == null ? "." : ": " + t.getMessage()), t);
				}
			} // end while alive
		}
		
	}; // end writer thread
}
