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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import eu.stratosphere.nephele.services.memorymanager.MemorySegment;

/**
 * A base class for reading from / writing to a file system file.
 * 
 * @author Alexander Alexandrov
 * @author Stephan Ewen
 * 
 * @param <T> The buffer type used for the underlying IO operations.
 */
public abstract class ChannelAccess<T extends Buffer> {
	
	/**
	 * The ID of the underlying channel.
	 */
	protected final Channel.ID id;

	/**
	 * A file channel for NIO access to the file.
	 */
	protected final FileChannel fileChannel;
	
	/**
	 * A request queue for submitting asynchronous requests to the corresponding
	 * IO worker thread.
	 */
	protected final RequestQueue<IORequest<T>> requestQueue;
	
	/**
	 * The queue containing the empty buffers that are ready to be reused.
	 */
	protected final ArrayBlockingQueue<T> returnBuffers;
	
	/**
	 * An exception that was encountered by the asynchronous request handling thread.
	 */
	private IOException exception;
	
	/**
	 * The number of buffers that this channel worked with.
	 */
	private final int numBuffers;

	
	// -------------------------------------------------------------------------
	//                  Constructors / Destructors
	// -------------------------------------------------------------------------

	/**
	 * Creates a new channel to the path indicated by the given ID. The channel hands IO requests to
	 * the given request queue to be processed.
	 * 
	 * @param channelID The id describing the path of the file that the channel accessed.
	 * @param requestQueue The queue that this channel hands its IO requests to.
	 */
	protected ChannelAccess(Channel.ID channelID, RequestQueue<IORequest<T>> requestQueue,
			Collection<T> buffers)
	throws IOException
	{
		if (channelID == null || requestQueue == null) {
			throw new NullPointerException();
		}
		
		this.id = channelID;
		this.numBuffers = buffers.size();
		this.requestQueue = requestQueue;
		this.returnBuffers = new ArrayBlockingQueue<T>(buffers.size(), false);
		
		try {
			RandomAccessFile file = new RandomAccessFile(id.getPath(), "rw");
			this.fileChannel = file.getChannel();
		}
		catch (IOException e) {
			throw new IOException("Channel to path '" + channelID.getPath() + "' could not be opened.", e);
		}
	}

	/**
	 * Release resources and return the externally supplied memory segments. This method does not 
	 * necessarily return the memory segments, in the case when an exception occurred. 
	 * 
	 * @throws IOException Thrown, if an exception occurred while processing remaining requests.
	 */
	public List<MemorySegment> close() throws IOException
	{
		final ArrayList<MemorySegment> segments = new ArrayList<MemorySegment>(this.numBuffers);
		
		try {
			while (segments.size() < this.numBuffers) {
				final T buffer = this.returnBuffers.take();
				segments.add(buffer.unbind());
			
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
	 * Deletes this channel by physically removing the file beneath it.
	 * This method may only be called on a closed channel.
	 */
	public void deleteChannel() {
		if (fileChannel.isOpen()) {
			throw new IllegalStateException("Cannot delete a channel that is open.");
		}
	
		// make a best effort to delete the file. Don't report exceptions
		try {
			File f = new File(this.id.getPath());
			if (f.exists()) {
				f.delete();
			}
		} catch (Throwable t) {
		}
	}
	
	
	//  -----------------------------------------------------------------------
	//                          Buffer handling
	//  -----------------------------------------------------------------------

	/**
	 * Handle a processed {@code buffer}. This method is invoked by the
	 * asynchronous IO worker threads upon completion of the IO request with the
	 * provided {@code buffer}.
	 * 
	 * @param buffer The buffer to be processed.
	 */
	public void handleProcessedBuffer(T buffer, IOException ex) {
		
		if (ex != null && this.exception != null) {
			this.exception = ex;
		}
		
		this.returnBuffers.add(buffer);
	}
	
	/**
	 * Gets the next input buffer from the empty buffers queue.
	 * 
	 * @return the next output buffer from the empty buffer queue
	 */
	protected final T nextBuffer() throws InterruptedException
	{
		return returnBuffers.take();
	}

	/**
	 * Checks the exception state of this channel. The channel is erroneous, if one of its requests could not
	 * be processed correctly.
	 * 
	 * @throws IOException Thrown, if the channel is erroneous. The thrown exception contains the original exception
	 *                     that defined the erroneous state as its cause.
	 */
	protected final void checkErroneous() throws IOException
	{
		if (this.exception != null) {
			throw new IOException("The channel is erroneous.", this.exception);
		}
	}
	
	
	// ------------------------------------------------------------------------
	
	/**
	 * An wrapper class for submitting IO requests to a centralized asynchronous
	 * IO worker thread. A request consists of an IO buffer to be processed and
	 * a reference to the {@link ChannelAccess} object submitting the request.
	 * 
	 * @author Alexander Alexandrov
	 * @param <T> The buffer type for this request.
	 */
	protected static final class IORequest<T extends Buffer>
	{
		protected final ChannelAccess<T> channel;

		protected final T buffer;

		protected IORequest(ChannelAccess<T> targetChannel, T buffer) {
			this.channel = targetChannel;
			this.buffer = buffer;
		}
		
	}
}
