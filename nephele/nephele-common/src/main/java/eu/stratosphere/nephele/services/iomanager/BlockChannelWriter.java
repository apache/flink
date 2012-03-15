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
import java.util.concurrent.TimeUnit;

import eu.stratosphere.nephele.services.memorymanager.MemorySegment;


/**
 * A writer that writes data in blocks to a file channel. The writer receives the data blocks in the form of 
 * {@link eu.stratosphere.nephele.services.memorymanager.MemorySegment}, which it writes entirely to the channel,
 * regardless of how space in the segment is used. The writing happens in an asynchronous fashion. That is, a write
 * request is not processed by the thread that issues it, but by an asynchronous writer thread. Once the request
 * is done, the asynchronous writer adds the MemorySegment to a <i>return queue</i> where it can be popped by the
 * worker thread, to be reused. The return queue is in this case a
 * {@link java.util.concurrent.LinkedBlockingQueue}, such that the working thread blocks until the request has been served,
 * if the request is still pending when the it requires the segment back. 
 * <p>
 * Typical write behind is realized, by having a small set of segments in the return queue at all times. When a
 * memory segment must be written, the request is issued to the writer and a new segment is immediately popped from
 * the return queue. Once too many requests have been issued and the I/O thread cannot keep up, the working thread
 * naturally blocks until another segment is available again.
 */
public class BlockChannelWriter extends BlockChannelAccess<WriteRequest, LinkedBlockingQueue<MemorySegment>>
{
	/**
	 * Creates a new block channel writer for the given channel.
	 *  
	 * @param channelID The ID of the channel to write to.
	 * @param requestQueue The request queue of the asynchronous writer thread, to which the I/O requests
	 *                     are added.
	 * @param returnSegments The return queue, to which the processed Memory Segments are added.
	 * @throws IOException Thrown, if the underlying file channel could not be opened exclusively.
	 */
	protected BlockChannelWriter(Channel.ID channelID, RequestQueue<WriteRequest> requestQueue,
			LinkedBlockingQueue<MemorySegment> returnSegments)
	throws IOException
	{
		super(channelID, requestQueue, returnSegments, true);
	}

	/**
	 * Issues a asynchronous write request to the writer.
	 * 
	 * @param segment The segment to be written.
	 * @throws IOException Thrown, when the writer encounters an I/O error. Due to the asynchronous nature of the
	 *                     writer, the exception thrown here may have been caused by an earlier write request. 
	 */
	public void writeBlock(MemorySegment segment) throws IOException
	{
		// check the error state of this channel
		checkErroneous();
		
		// write the current buffer and get the next one
		this.requestsNotReturned.incrementAndGet();
		if (this.closed || this.requestQueue.isClosed()) {
			// if we found ourselves closed after the counter increment,
			// decrement the counter again and do not forward the request
			this.requestsNotReturned.decrementAndGet();
			throw new IOException("The writer has been closed.");
		}
		this.requestQueue.add(new SegmentWriteRequest(this, segment));
	}
	
	/**
	 * Gets the next memory segment that has been written and is available again.
	 * This method blocks until such a segment is available, or until an error occurs in the writer, or the
	 * writer is closed.
	 * <p>
	 * WARNING: If this method is invoked without any segment ever returning (for example, because the
	 * {@link #writeBlock(MemorySegment)} method has not been invoked appropriately), the method may block
	 * forever.
	 * 
	 * @return The next memory segment from the writers's return queue.
	 * @throws IOException Thrown, if an I/O error occurs in the writer while waiting for the request to return.
	 */
	public MemorySegment getNextReturnedSegment() throws IOException
	{
		try {
			while (true) {
				final MemorySegment next = this.returnBuffers.poll(2000, TimeUnit.MILLISECONDS);
				if (next != null) {
					return next;
				} else {
					if (this.closed) {
						throw new IOException("The writer has been asynchronously closed.");
					}
					checkErroneous();
				}
			}
		} catch (InterruptedException iex) {
			throw new IOException("Writer was interrupted while waiting for the next returning segment.");
		}
	}
}
