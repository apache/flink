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
 * A reader that reads data in blocks from a file channel. The reader reads the blocks into a 
 * {@link eu.stratosphere.nephele.services.memorymanager.MemorySegment} in an asynchronous fashion. That is, a read
 * request is not processed by the thread that issues it, but by an asynchronous reader thread. Once the read request
 * is done, the asynchronous reader adds the full MemorySegment to a <i>return queue</i> where it can be popped by the
 * worker thread, once it needs the data. The return queue is in this case a
 * {@link java.util.concurrent.LinkedBlockingQueue}, such that the working thread blocks until the request has been served,
 * if the request is still pending when the it requires the data. 
 * <p>
 * Typical pre-fetching reads are done by issuing the read requests early and popping the return queue once the data
 * is actually needed.
 * <p>
 * The reader has no notion whether the size of the memory segments is actually the size of the blocks on disk,
 * or even whether the file was written in blocks of the same size, or in blocks at all. Ensuring that the
 * writing and reading is consistent with each other (same blocks sizes) is up to the programmer.  
 */
public class BlockChannelReader extends BlockChannelAccess<ReadRequest, LinkedBlockingQueue<MemorySegment>>
{
	/**
	 * Creates a new block channel reader for the given channel.
	 *  
	 * @param channelID The ID of the channel to read.
	 * @param requestQueue The request queue of the asynchronous reader thread, to which the I/O requests
	 *                     are added.
	 * @param returnSegments The return queue, to which the full Memory Segments are added.
	 * @throws IOException Thrown, if the underlying file channel could not be opened.
	 */
	protected BlockChannelReader(Channel.ID channelID, RequestQueue<ReadRequest> requestQueue,
			LinkedBlockingQueue<MemorySegment> returnSegments)
	throws IOException
	{
		super(channelID, requestQueue, returnSegments, false);
	}	

	/**
	 * Issues a read request, which will asynchronously fill the given segment with the next block in the
	 * underlying file channel. Once the read request is fulfilled, the segment will be added to this reader's
	 * return queue.
	 *  
	 * @param segment The segment to read the block into.
	 * @throws IOException Thrown, when the reader encounters an I/O error. Due to the asynchronous nature of the
	 *                     reader, the exception thrown here may have been caused by an earlier read request. 
	 */
	public void readBlock(MemorySegment segment) throws IOException
	{
		// check the error state of this channel
		checkErroneous();
		
		// write the current buffer and get the next one
		// the statements have to be in this order to avoid incrementing the counter
		// after the channel has been closed
		this.requestsNotReturned.incrementAndGet();
		if (this.closed || this.requestQueue.isClosed()) {
			// if we found ourselves closed after the counter increment,
			// decrement the counter again and do not forward the request
			this.requestsNotReturned.decrementAndGet();
			throw new IOException("The reader has been closed.");
		}
		this.requestQueue.add(new SegmentReadRequest(this, segment));
	}
	
	/**
	 * Gets the next memory segment that has been filled with data by the reader. This method blocks until
	 * such a segment is available, or until an error occurs in the reader, or the reader is closed.
	 * <p>
	 * WARNING: If this method is invoked without any segment ever returning (for example, because the
	 * {@link #readBlock(MemorySegment)} method has not been invoked appropriately), the method may block
	 * forever.
	 * 
	 * @return The next memory segment from the reader's return queue.
	 * @throws IOException Thrown, if an I/O error occurs in the reader while waiting for the request to return.
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
						throw new IOException("The reader has been asynchronously closed.");
					}
					checkErroneous();
				}
			}
		} catch (InterruptedException iex) {
			throw new IOException("Reader was interrupted while waiting for the next returning segment.");
		}
	}
}
