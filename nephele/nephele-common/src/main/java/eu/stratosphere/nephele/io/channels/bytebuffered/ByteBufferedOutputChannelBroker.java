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

package eu.stratosphere.nephele.io.channels.bytebuffered;

import java.io.IOException;

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.compression.Compressor;

public interface ByteBufferedOutputChannelBroker {
	
	/**
	 * Requests an empty write buffer from the broker. This method will block
	 * until the requested write buffer is available.
	 * 
	 * @return the byte buffers to write in
	 * @throws InterruptedException
	 *         thrown if the connected task is interrupted while waiting for the buffer
	 * @throws IOException
	 *         thrown if an error occurs while requesting the empty write buffer.
	 */
	Buffer requestEmptyWriteBuffer() throws InterruptedException, IOException;

	/**
	 * Returns a filled write buffer to the broker. The broker will take care
	 * of the buffers and transfer the user data to the connected input channel on a best effort basis.
	 * 
	 * @param buffer
	 *        the buffer to be returned to the broker
	 * @throws InterruptedException
	 *         thrown if the thread is interrupted while waiting for the buffers to be released
	 * @throws IOException
	 *         thrown if an I/O error occurs while releasing the buffers
	 */
	void releaseWriteBuffer(Buffer buffer) throws IOException, InterruptedException;

	/**
	 * Checks if there is still data created by this output channel that must be transfered to the corresponding input
	 * channel.
	 * 
	 * @return <code>true</code> if the channel has data left to transmit, <code>false</code> otherwise
	 * @throws InterruptedException
	 *         thrown if the connected task is interrupted while waiting for the remaining data to be transmitted
	 * @throws IOException
	 *         thrown if an error occurs while transmitting the remaining data
	 */
	boolean hasDataLeftToTransmit() throws IOException, InterruptedException;

	/**
	 * Forwards the given event to the connected network input channel on a best effort basis.
	 * 
	 * @param event
	 *        the event to be transferred
	 * @throws InterruptedException
	 *         thrown if the thread is interrupted while waiting for the event to be transfered
	 * @throws IOException
	 *         thrown if an I/O error occurs while transfering the event
	 */
	void transferEventToInputChannel(AbstractEvent event) throws IOException, InterruptedException;
}
