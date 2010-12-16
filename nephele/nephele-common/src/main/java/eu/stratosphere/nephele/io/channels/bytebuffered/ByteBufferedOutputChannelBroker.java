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

import eu.stratosphere.nephele.event.task.AbstractEvent;

public interface ByteBufferedOutputChannelBroker {

	/**
	 * Requests empty write buffers from the broker. This method will block
	 * until the requested write buffers are available.
	 * 
	 * @return one or possibly two byte buffers to write in, depending on whether compression is enabled or not
	 * @throws InterruptedException
	 *         thrown if the connected task is interrupted while waiting for the buffers
	 */
	BufferPairResponse requestEmptyWriteBuffers() throws InterruptedException;

	/**
	 * Returns a filled write buffers to the broker. The broker will take care
	 * of the buffers and transfer the one with the user data to the connected input channel on a best effort basis.
	 */
	void releaseWriteBuffers();

	/**
	 * Forwards the given event to the connected network input channel on a best effort basis.
	 * 
	 * @param event
	 *        the event to be transferred
	 */
	void transferEventToInputChannel(AbstractEvent event);

}
