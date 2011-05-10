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


/**
 *
 *
 */
public class BlockChannelWriter extends BlockChannelAccess<Buffer.Output>
{
	
	
	protected BlockChannelWriter(Channel.ID channelID, RequestQueue<IORequest<Buffer.Output>> requestQueue,
			LinkedBlockingQueue<Buffer.Output> returnSegments)
	throws IOException
	{
		super(channelID, requestQueue, returnSegments, true);
	}
	

	
	public void writeBlock(Buffer.Output buffer) throws IOException
	{
		// make sure the entire buffer is written
		buffer.markEndAsPosition();
		
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
		this.requestQueue.add(new IORequest<Buffer.Output>(this, buffer));
	}

}
