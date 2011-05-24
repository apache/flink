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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import eu.stratosphere.nephele.services.memorymanager.MemorySegment;

/**
 * A base class for readers and writers that offer a stream like interface. Internally, the
 * readers and writers use a collection of buffers to generate asynchronous block I/O against the filesystem.
 * 
 * @author Alexander Alexandrov
 * @author Stephan Ewen
 * 
 * @param <T> The buffer type used for the underlying IO operations.
 */
public abstract class StreamChannelAccess<T extends Buffer, R extends IORequest> 
	extends ChannelAccess<T, R>
{
	/**
	 * The queue containing the empty buffers that are ready to be reused.
	 */
	protected final ArrayBlockingQueue<T> returnBuffers;
	
	/**
	 * The number of buffers that this channel worked with.
	 */
	private final int numBuffers;
	
	/**
	 * Flag marking this channel as closed;
	 */
	protected volatile boolean closed;

	
	// -------------------------------------------------------------------------
	//                  Constructors / Destructors
	// -------------------------------------------------------------------------

	/**
	 * Creates a new channel to the path indicated by the given ID. The channel hands IO requests to
	 * the given request queue to be processed. A collection of buffers is used for turning the steam
	 * abstraction into block I/O, pre-fetching ahead for the readers or writing behind for the writers. 
	 * 
	 * @param channelID The id describing the path of the file that the channel accessed.
	 * @param requestQueue The queue that this channel hands its IO requests to.
	 * @param buffers The buffers to be used for the asynchronous passing of data between the channel access
	 *                and the I/O thread.
	 * @param writeEnabled Flag describing whether the channel should be opened in read/write mode, rather
	 *                     than in read-only mode.
	 * @throws IOException Thrown, if the channel could no be opened.
	 */
	protected StreamChannelAccess(Channel.ID channelID, RequestQueue<R> requestQueue,
			Collection<T> buffers, boolean writeEnabled)
	throws IOException
	{
		super(channelID, requestQueue, writeEnabled);
		
		if (buffers == null || buffers.isEmpty()) {
			throw new IllegalArgumentException("Stream channel cannot work without buffers.");
		}
		
		this.numBuffers = buffers.size();
		this.returnBuffers = new ArrayBlockingQueue<T>(buffers.size(), false);
	}
	
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.services.iomanager.ChannelAccess#isClosed()
	 */
	@Override
	public boolean isClosed()
	{
		return this.closed;
	}

	/**
	 * Release resources and return the externally supplied memory segments. This method does not 
	 * necessarily return the memory segments, in the case when an exception occurred. 
	 * 
	 * @throws IOException Thrown, if an exception occurred while processing remaining requests.
	 */
	public List<MemorySegment> close() throws IOException
	{
		// list to collect segments in
		final ArrayList<MemorySegment> segments = new ArrayList<MemorySegment>(this.numBuffers);
		
		// get all segments from the return buffer queue
		try {
			while (segments.size() < this.numBuffers) {
				final T buffer = this.returnBuffers.take();
				segments.add(buffer.dispose());
			
				// check the error state of this channel after each segment and raise the exception in case
				checkErroneous();
			}
		}
		catch (InterruptedException iex) {
			// when the process of waiting for the empty segments is interrupted, we call for an exception
			throw new IOException("The channel closing was interrupted. " +
					"Its not guaranteed that all buffers have been properly written.");
		}
		
		return segments;
	}
	
	/**
	 * Gets the next input buffer from the empty buffers queue.
	 * 
	 * @return the next output buffer from the empty buffer queue
	 */
	protected final T nextBuffer() throws InterruptedException
	{
		return this.returnBuffers.take();
	}
	
	/**
	 * Adds the returned buffer back to this channel's collection of I/O buffers.
	 * 
	 * @param buffer The buffer to be returned.
	 * @see eu.stratosphere.nephele.services.iomanager.ChannelAccess#returnBuffer(eu.stratosphere.nephele.services.iomanager.Buffer)
	 */
	@Override
	protected void returnBuffer(T buffer)
	{
		this.returnBuffers.add(buffer);
	}
	
}
