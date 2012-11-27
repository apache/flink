/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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
import java.util.List;

import eu.stratosphere.nephele.services.memorymanager.MemorySegment;


/**
 *
 *
 */
public class BulkBlockChannelReader extends BlockChannelAccess<ReadRequest, ArrayList<MemorySegment>>
{
	
	
	protected BulkBlockChannelReader(Channel.ID channelID, RequestQueue<ReadRequest> requestQueue, 
			List<MemorySegment> sourceSegments, int numBlocks)
	throws IOException
	{
		super(channelID, requestQueue, new ArrayList<MemorySegment>(numBlocks), false);
		
		// sanity check
		if (sourceSegments.size() < numBlocks) {
			throw new IllegalArgumentException("The list of source memory segments must contain at least" +
					" as many segments as the number of blocks to read.");
		}
		
		// send read requests for all blocks
		for (int i = 0; i < numBlocks; i++) {
			readBlock(sourceSegments.remove(sourceSegments.size() - 1));
		}
	}
	

	
	private void readBlock(MemorySegment segment) throws IOException
	{
		// check the error state of this channel
		checkErroneous();
		
		// write the current buffer and get the next one
		this.requestsNotReturned.incrementAndGet();
		if (this.closed || this.requestQueue.isClosed()) {
			// if we found ourselves closed after the counter increment,
			// decrement the counter again and do not forward the request
			this.requestsNotReturned.decrementAndGet();
			throw new IOException("The reader has been closed.");
		}
		this.requestQueue.add(new SegmentReadRequest(this, segment));
	}
	
	public List<MemorySegment> getFullSegments()
	{
		synchronized (this.closeLock) {
			if (!this.isClosed() || this.requestsNotReturned.get() > 0) {
				throw new IllegalStateException("Full segments can only be obtained after the reader was properly closed.");
			}
		}
		
		return this.returnBuffers;
	}

}


