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
import java.util.Collection;
import java.util.List;

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;

/**
 * A writer to an underlying {@link Channel}. This writer buffers written objects in a buffer, which is handed to
 * a queue for write requests. The queue is polled by a thread, typically a single thread for all instances of this
 * writer, which actually writes the buffers to disk.
 * 
 * @author Alexander Alexandrov
 * @author Stephan Ewen
 */
public final class ChannelWriter extends StreamChannelAccess<Buffer.Output, WriteRequest> implements Writer
{
	/**
	 * The current buffer that write requests go to.
	 */
	private Buffer.Output currentBuffer;

	
	// -------------------------------------------------------------------------
	//                      Constructors / Destructors
	// -------------------------------------------------------------------------

	/**
	 * 
	 * @param channelID
	 * @param requestQueue
	 * @param buffers
	 * @param filledBuffers
	 * @throws IOException
	 */
	protected ChannelWriter(Channel.ID channelID, RequestQueue<WriteRequest> requestQueue,
			Collection<Buffer.Output> buffers, boolean filledBuffers)
	throws IOException
	{
		super(channelID, requestQueue, buffers, true);

		try {
			this.fileChannel.truncate(0);
		}
		catch (IOException e) {
			throw new IOException("Channel to path '" + channelID.getPath() + "' could not be opened.", e);
		}

		if (filledBuffers) {
			for (Buffer.Output buffer : buffers) {
				this.requestQueue.add(new BufferWriteRequest(this, buffer));
			}
		}
		else {
			this.returnBuffers.addAll(buffers);
		}

		// get the first buffer from the empty buffer queue as current
		try {
			this.currentBuffer = nextBuffer();
			this.currentBuffer.rewind();
			checkErroneous();
		}
		catch (InterruptedException iex) {
			throw new IOException("");
		}
	}
	
	/**
	 * Closes this writer. Sends a request to write the current buffer, makes sure all data is written out
	 * and waits for all memory segments to come back.
	 * 
	 * @see eu.stratosphere.nephele.services.iomanager.StreamChannelAccess#close()
	 */
	@Override
	public synchronized List<MemorySegment> close() throws IOException
	{
		synchronized (this) {
			if (this.closed) {
				throw new IllegalStateException("Writer is already closing or has been closed.");
			}
			this.closed = true;
		}
		
		checkErroneous();
		
		// create a new write request for the current buffer
		if (this.currentBuffer != null) {
			this.requestQueue.add(new BufferWriteRequest(this, this.currentBuffer));
			this.currentBuffer = null;
		}

		final List<MemorySegment> segments = super.close();
		
		// flush contents to the underlying channel and close the file
		if (this.fileChannel.isOpen()) {
			this.fileChannel.close();
		}
		
		return segments;
	}


	/**
	 * Writes the contents of the {@code writable} to the current output buffer.
	 * If the buffer is exhausted while writing, transparently swaps the buffers
	 * and retries writing to the next buffer. Returns {@code true} if the write
	 * operation was successful or {@code false} if the provided {@code
	 * writable} is larger than the size provided by an empty output buffer.
	 * 
	 * @param writable The object writing itself to the current output buffer.
	 * @return A boolean flag indicating the success of the write operation,
	 */
	@Override
	public boolean write(IOReadableWritable writable) throws IOException
	{
		if (this.currentBuffer.write(writable)) {
			// object was written on current buffer without a problem
			return true;
		}
		else {
			// current buffer is full, check the error state of this channel
			checkErroneous();
			
			if (this.requestQueue.isClosed()) {
				throw new IOException("The writer's IO path has been closed.");
			}
			
			// write the current buffer and get the next one
			this.requestQueue.add(new BufferWriteRequest(this, currentBuffer));
			
			try {
				this.currentBuffer = nextBuffer();
				this.currentBuffer.rewind();
				checkErroneous();
			}
			catch (InterruptedException iex) {
				throw new IOException("IO channel corrupt. Writer was interrupted getting a new buffer.");
			}
			
			// retry writing with an empty input buffer
			if (this.currentBuffer.write(writable)) {
				return true;
			}
			else {
				throw new IOException("Object to be written is too large for IO-buffer.");
			}
		}
	}
}

//--------------------------------------------------------------------------------------------

final class BufferWriteRequest implements WriteRequest
{
	private final ChannelWriter channel;
	
	private final Buffer.Output buffer;
	
	protected BufferWriteRequest(ChannelWriter targetChannel, Buffer.Output buffer)
	{
		this.channel = targetChannel;
		this.buffer = buffer;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.services.iomanager.ReadRequest#read(java.nio.channels.FileChannel)
	 */
	@Override
	public void write() throws IOException
	{
		if (!this.buffer.memory.isFree()) {
			this.buffer.writeToChannel(this.channel.fileChannel);
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.services.iomanager.IORequest#requestDone(java.io.IOException)
	 */
	@Override
	public void requestDone(IOException ioex)
	{
		this.channel.handleProcessedBuffer(this.buffer, ioex);
	}
}
