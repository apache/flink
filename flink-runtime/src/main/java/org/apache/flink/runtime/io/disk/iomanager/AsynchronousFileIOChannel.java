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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.flink.core.memory.MemorySegment;

/**
 * A base class for readers and writers that accept read or write requests for whole blocks.
 * The request is delegated to an asynchronous I/O thread. After completion of the I/O request, the memory
 * segment of the block is added to a collection to be returned.
 * <p>
 * The asynchrony of the access makes it possible to implement read-ahead or write-behind types of I/O accesses.
 * 
 * @param <R> The type of request (e.g. <tt>ReadRequest</tt> or <tt>WriteRequest</tt> issued by this access to the I/O threads.
 */
public abstract class AsynchronousFileIOChannel<R extends IORequest> extends AbstractFileIOChannel {
	
	/** The lock that is used during closing to synchronize the thread that waits for all
	 * requests to be handled with the asynchronous I/O thread. */
	protected final Object closeLock = new Object();
	
	/** A request queue for submitting asynchronous requests to the corresponding IO worker thread. */
	protected final RequestQueue<R> requestQueue;
	
	/** An atomic integer that counts the number of requests that we still wait for to return. */
	protected final AtomicInteger requestsNotReturned = new AtomicInteger(0);
	
	/** Hander for completed requests */
	protected final RequestDoneCallback resultHander;
	
	/** An exception that was encountered by the asynchronous request handling thread.*/
	protected volatile IOException exception;
	
	/** Flag marking this channel as closed */
	protected volatile boolean closed;

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a new channel access to the path indicated by the given ID. The channel accepts buffers to be
	 * read/written and hands them to the asynchronous I/O thread. After being processed, the buffers 
	 * are returned by adding the to the given queue.
	 * 
	 * @param channelID The id describing the path of the file that the channel accessed.
	 * @param requestQueue The queue that this channel hands its IO requests to.
	 * @param callback The callback to be invoked when a request is done.
	 * @param writeEnabled Flag describing whether the channel should be opened in read/write mode, rather
	 *                     than in read-only mode.
	 * @throws IOException Thrown, if the channel could no be opened.
	 */
	protected AsynchronousFileIOChannel(FileIOChannel.ID channelID, RequestQueue<R> requestQueue, 
			RequestDoneCallback callback, boolean writeEnabled) throws IOException
	{
		super(channelID, writeEnabled);
		
		if (requestQueue == null) {
			throw new NullPointerException();
		}
		
		this.requestQueue = requestQueue;
		this.resultHander = callback;
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public boolean isClosed() {
		return this.closed;
	}
	
	/**
	 * Closes the reader and waits until all pending asynchronous requests are
	 * handled. Even if an exception interrupts the closing, the underlying <tt>FileChannel</tt> is closed.
	 * 
	 * @throws IOException Thrown, if an I/O exception occurred while waiting for the buffers, or if
	 *                     the closing was interrupted.
	 */
	public void close() throws IOException {
		// atomically set the close flag
		synchronized (this.closeLock) {
			if (this.closed) {
				return;
			}
			this.closed = true;
			
			try {
				// wait until as many buffers have been returned as were written
				// only then is everything guaranteed to be consistent.{
				while (this.requestsNotReturned.get() > 0) {
					try {
						// we add a timeout here, because it is not guaranteed that the
						// decrementing during buffer return and the check here are deadlock free.
						// the deadlock situation is however unlikely and caught by the timeout
						this.closeLock.wait(1000);
						checkErroneous();
					}
					catch (InterruptedException iex) {}
				}
			}
			finally {
				// close the file
				if (this.fileChannel.isOpen()) {
					this.fileChannel.close();
				}
			}
		}
	}
	
	/**
	 * This method waits for all pending asynchronous requests to return. When the
	 * last request has returned, the channel is closed and deleted.
	 * <p>
	 * Even if an exception interrupts the closing, such that not all request are handled,
	 * the underlying <tt>FileChannel</tt> is closed and deleted.
	 * 
	 * @throws IOException Thrown, if an I/O exception occurred while waiting for the buffers, or if the closing was interrupted.
	 */
	public void closeAndDelete() throws IOException {
		try {
			close();
		}
		finally {
			deleteChannel();
		}
	}
	
	/**
	 * Checks the exception state of this channel. The channel is erroneous, if one of its requests could not
	 * be processed correctly.
	 * 
	 * @throws IOException Thrown, if the channel is erroneous. The thrown exception contains the original exception
	 *                     that defined the erroneous state as its cause.
	 */
	public final void checkErroneous() throws IOException {
		if (this.exception != null) {
			throw this.exception;
		}
	}
	
	/**
	 * Handles a processed <tt>Buffer</tt>. This method is invoked by the
	 * asynchronous IO worker threads upon completion of the IO request with the
	 * provided buffer and/or an exception that occurred while processing the request
	 * for that buffer.
	 * 
	 * @param buffer The buffer to be processed.
	 * @param ex The exception that occurred in the I/O threads when processing the buffer's request.
	 */
	final void handleProcessedBuffer(MemorySegment buffer, IOException ex) {
		// even if the callbacks throw an error, we need to maintain our bookkeeping
		try {
			if (ex != null && this.exception == null) {
				this.exception = ex;
				this.resultHander.requestFailed(buffer, ex);
			}
			else {
				this.resultHander.requestSuccessful(buffer);
			}
		}
		finally {
			// decrement the number of missing buffers. If we are currently closing, notify the 
			if (this.closed) {
				synchronized (this.closeLock) {
					int num = this.requestsNotReturned.decrementAndGet();
					if (num == 0) {
						this.closeLock.notifyAll();
					}
				}
			}
			else {
				this.requestsNotReturned.decrementAndGet();
			}
		}
	}
}

//--------------------------------------------------------------------------------------------

/**
 * Read request that reads an entire memory segment from a block reader.
 */
final class SegmentReadRequest implements ReadRequest {
	
	private final AsynchronousFileIOChannel<ReadRequest> channel;
	
	private final MemorySegment segment;
	
	protected SegmentReadRequest(AsynchronousFileIOChannel<ReadRequest> targetChannel, MemorySegment segment) {
		this.channel = targetChannel;
		this.segment = segment;
	}

	@Override
	public void read() throws IOException {
		final FileChannel c = this.channel.fileChannel;
		if (c.size() - c.position() > 0) {
			try {
				final ByteBuffer wrapper = this.segment.wrap(0, this.segment.size());
				this.channel.fileChannel.read(wrapper);
			}
			catch (NullPointerException npex) {
				throw new IOException("Memory segment has been released.");
			}
		}
	}

	@Override
	public void requestDone(IOException ioex) {
		this.channel.handleProcessedBuffer(this.segment, ioex);
	}
}

//--------------------------------------------------------------------------------------------

/**
 * Write request that writes an entire memory segment to the block writer.
 */
final class SegmentWriteRequest implements WriteRequest {
	
	private final AsynchronousFileIOChannel<WriteRequest> channel;
	
	private final MemorySegment segment;
	
	protected SegmentWriteRequest(AsynchronousFileIOChannel<WriteRequest> targetChannel, MemorySegment segment) {
		this.channel = targetChannel;
		this.segment = segment;
	}

	@Override
	public void write() throws IOException {
		try {
			this.channel.fileChannel.write(this.segment.wrap(0, this.segment.size()));
		}
		catch (NullPointerException npex) {
			throw new IOException("Memory segment has been released.");
		}
	}

	@Override
	public void requestDone(IOException ioex) {
		this.channel.handleProcessedBuffer(this.segment, ioex);
	}
}
