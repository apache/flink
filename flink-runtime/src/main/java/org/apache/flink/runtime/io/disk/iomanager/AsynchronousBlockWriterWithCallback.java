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

import org.apache.flink.core.memory.MemorySegment;

/**
 * An asynchronous implementation of the {@link BlockChannelWriterWithCallback} that queues I/O requests
 * and calls a callback once they have been handled.
 */
public class AsynchronousBlockWriterWithCallback extends AsynchronousFileIOChannel<WriteRequest> implements BlockChannelWriterWithCallback {
	
	/**
	 * Creates a new asynchronous block writer for the given channel.
	 *  
	 * @param channelID The ID of the channel to write to.
	 * @param requestQueue The request queue of the asynchronous writer thread, to which the I/O requests are added.
	 * @param callback The callback to be invoked when requests are done.
	 * @throws IOException Thrown, if the underlying file channel could not be opened exclusively.
	 */
	protected AsynchronousBlockWriterWithCallback(FileIOChannel.ID channelID, RequestQueue<WriteRequest> requestQueue,
			RequestDoneCallback callback) throws IOException
	{
		super(channelID, requestQueue, callback, true);
	}

	/**
	 * Issues a asynchronous write request to the writer.
	 * 
	 * @param segment The segment to be written.
	 * @throws IOException Thrown, when the writer encounters an I/O error. Due to the asynchronous nature of the
	 *                     writer, the exception thrown here may have been caused by an earlier write request. 
	 */
	@Override
	public void writeBlock(MemorySegment segment) throws IOException {
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
}
