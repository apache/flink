/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.services.iomanager;


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;


/**
 * A base class for readers and writers that read data from I/O manager channels, or write data to them.
 * Requests handled by channels that inherit from this class are executed asynchronously, which allows
 * write-behind for writers and pre-fetching for readers.
 * 
 * 
 * @param <T> The buffer type used for the underlying IO operations.
 */
public abstract class ChannelAccess<T, R extends IORequest>
{
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
	protected final RequestQueue<R> requestQueue;
	
	/**
	 * An exception that was encountered by the asynchronous request handling thread.
	 */
	protected volatile IOException exception;
	
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a new channel to the path indicated by the given ID. The channel hands IO requests to
	 * the given request queue to be processed.
	 * 
	 * @param channelID The id describing the path of the file that the channel accessed.
	 * @param requestQueue The queue that this channel hands its IO requests to.
	 * @param writeEnabled Flag describing whether the channel should be opened in read/write mode, rather
	 *                     than in read-only mode.
	 * @throws IOException Thrown, if the channel could no be opened.
	 */
	protected ChannelAccess(Channel.ID channelID, RequestQueue<R> requestQueue, boolean writeEnabled)
	throws IOException
	{
		if (channelID == null || requestQueue == null) {
			throw new NullPointerException();
		}
		
		this.id = channelID;
		this.requestQueue = requestQueue;
		
		try {
			RandomAccessFile file = new RandomAccessFile(id.getPath(), writeEnabled ? "rw" : "r");
			this.fileChannel = file.getChannel();
		}
		catch (IOException e) {
			throw new IOException("Channel to path '" + channelID.getPath() + "' could not be opened.", e);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Checks, whether this channel has been closed;
	 * 
	 * @return True, if the channel has been closed, false otherwise.
	 */
	public abstract boolean isClosed();
	
	/**
	 * This method is invoked by the asynchronous I/O thread to return a buffer after the I/O request
	 * completed.
	 * 
	 * @param buffer The buffer to be returned.
	 */
	protected abstract void returnBuffer(T buffer); 
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the channel ID of this channel.
	 * 
	 * @return This channel's ID.
	 */
	public final Channel.ID getChannelID()
	{
		return this.id;
	}
	
	/**
	 * Checks the exception state of this channel. The channel is erroneous, if one of its requests could not
	 * be processed correctly.
	 * 
	 * @throws IOException Thrown, if the channel is erroneous. The thrown exception contains the original exception
	 *                     that defined the erroneous state as its cause.
	 */
	public final void checkErroneous() throws IOException
	{
		if (this.exception != null) {
			throw new IOException("The channel is erroneous.", this.exception);
		}
	}
	
	/**
	 * Deletes this channel by physically removing the file beneath it.
	 * This method may only be called on a closed channel.
	 */
	public void deleteChannel()
	{
		if (this.fileChannel.isOpen()) {
			throw new IllegalStateException("Cannot delete a channel that is open.");
		}
	
		// make a best effort to delete the file. Don't report exceptions.
		try {
			File f = new File(this.id.getPath());
			if (f.exists()) {
				f.delete();
			}
		} catch (Throwable t) {}
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
	final void handleProcessedBuffer(T buffer, IOException ex) {
		
		if (ex != null && this.exception == null) {
			this.exception = ex;
		}
		
		returnBuffer(buffer);
	}
}
