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

package eu.stratosphere.nephele.taskmanager.bufferprovider;

import eu.stratosphere.nephele.io.channels.bytebuffered.BufferPairRequest;
import eu.stratosphere.nephele.io.channels.bytebuffered.BufferPairResponse;

/**
 * A write buffer provider provides empty write buffers which are filled by {@link AbstractByteBufferedOutputChannel}
 * objects.
 * 
 * @author warneke
 */
public interface WriteBufferProvider {

	/**
	 * Registers a new {@link OutOfByteBuffersListener} object that is notified when the buffer provider runs out of
	 * write buffers.
	 * 
	 * @param listener
	 *        the listener object to register
	 */
	void registerOutOfWriteBuffersListener(OutOfByteBuffersListener listener);

	/**
	 * Unregisters the given {@link OutOfByteBuffersListener} object so it does no longer receive notifications about a
	 * lack of write buffers.
	 * 
	 * @param listener
	 *        the listener object to unregister
	 */
	void unregisterOutOfWriteBuffersLister(OutOfByteBuffersListener listener);

	/**
	 * Requests up to two buffers from the pool of write buffers. The request is specified the given
	 * {@link BufferPairRequest} object. This method blocks until the request can be fulfilled.
	 * 
	 * @param bufferPairRequest
	 *        a buffer pair request specified the characteristics of the requested buffers
	 * @return a buffer pair response containing the requested buffers
	 * @throws InterruptedException
	 *         thrown if the thread waiting for the buffers is interrupted
	 */
	BufferPairResponse requestEmptyWriteBuffers(BufferPairRequest bufferPairRequest) throws InterruptedException;
	
	/**
	 * Returns the maximum buffer size in bytes available at this buffer provider.
	 * 
	 * @return the maximum buffer size in bytes available at this buffer provider
	 */
	int getMaximumBufferSize();
}
