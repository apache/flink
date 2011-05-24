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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import eu.stratosphere.nephele.services.memorymanager.MemorySegment;


/**
 * A base class for readers and writers that accept read or write requests for whole blocks.
 * The request is delegated to an asynchronous I/O thread. After completion of the I/O request, the memory
 * segment of the block is added to a queue to be returned.
 * 
 * @author Stephan Ewen
 * @param <T> The buffer type used for the underlying IO operations, which is sent as a request.
 * @param <R> The type of segment asynchronously returned from the access. 
 */
public abstract class BlockChannelAccess<R extends IORequest> extends ChannelAccess<MemorySegment, R>
{	
	/**
	 * The lock that is used during closing to synchronize the thread that waits for all
	 * requests to be handled, and the asynchronous I/O thread.
	 */
	private final Object closeLock = new Object();
	
	/**
	 * An atomic integer that counts the number of buffers we still wait for to return.
	 */
	protected final AtomicInteger requestsNotReturned = new AtomicInteger(0);
	
	/**
	 * The queue containing the processed buffers that are ready to be (re)used.
	 */
	protected final LinkedBlockingQueue<MemorySegment> returnBuffers;
	
	/**
	 * Flag marking this channel as closed;
	 */
	protected volatile boolean closed;

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a new channel to the path indicated by the given ID. The channel accepts buffers to be written
	 * and hands them to the asynchronous I/O thread. After being processed, the buffers' memory segments are
	 * returned by adding the to the given queue.
	 * 
	 * @param channelID The id describing the path of the file that the channel accessed.
	 * @param requestQueue The queue that this channel hands its IO requests to.
	 * @param returnQueue The queue to which the segments are added after their buffer was written.
	 * @param writeEnabled Flag describing whether the channel should be opened in read/write mode, rather
	 *                     than in read-only mode.
	 * @throws IOException Thrown, if the channel could no be opened.
	 */
	protected BlockChannelAccess(Channel.ID channelID, RequestQueue<R> requestQueue,
			LinkedBlockingQueue<MemorySegment> returnQueue, boolean writeEnabled)
	throws IOException
	{
		super(channelID, requestQueue, writeEnabled);
		
		if (requestQueue == null) {
			throw new NullPointerException();
		}
		
		this.returnBuffers = returnQueue;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.services.iomanager.ChannelAccess#isClosed()
	 */
	@Override
	public boolean isClosed()
	{
		return this.closed;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.services.iomanager.ChannelAccess#returnBuffer(eu.stratosphere.nephele.services.iomanager.Buffer)
	 */
	@Override
	protected void returnBuffer(MemorySegment buffer)
	{
		this.returnBuffers.add(buffer);
		
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
	
	/**
	 * Closes the reader and waits until all asynchronous requests that have not yet been handled are
	 * handled. Even if an exception interrupts the closing, the underlying <tt>FileChannel</tt> is
	 * closed.
	 * 
	 * @throws IOException Thrown, if an I/O exception occurred while waiting for the buffers, or if
	 *                     the closing was interrupted.
	 */
	public void close() throws IOException
	{
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
					catch (InterruptedException iex) {
						throw new IOException("Block channel access was interrupted while closing.");
					}
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
	 * This method waits for until all asynchronous requests not yet been handled to return. When the
	 * last request has returned, the channel is closed and deleted.
	 * 
	 * Even if an exception interrupts the closing, such that not all request are handled,
	 * the underlying <tt>FileChannel</tt> is closed and deleted.
	 * 
	 * @throws IOException Thrown, if an I/O exception occurred while waiting for the buffers, or if
	 *                     the closing was interrupted.
	 */
	public void closeAndDelete() throws IOException
	{
		try {
			close();
		}
		finally {
			deleteChannel();
		}
	}
	
}
