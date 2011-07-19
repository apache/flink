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

import java.io.IOException;

import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.FileBufferManager;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedInputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.BufferPairRequest;
import eu.stratosphere.nephele.io.channels.bytebuffered.BufferPairResponse;

/**
 * A read buffer provider provides empty read buffers which are then consumed by
 * {@link AbstractByteBufferedInputChannel} objects.
 * 
 * @author warneke
 */
public interface ReadBufferProvider {

	/**
	 * Requests an empty read buffer with a minimum size of <code>minimumSizeOfBuffer</code> for a specific
	 * {@link AbstractByteBufferedInputChannel}. The method may returns immediately, even if the request could not be
	 * fulfilled.
	 * 
	 * @param minimumSizeOfBuffer
	 *        the minimum size of the requested read buffer in bytes
	 * @param targetChannelID
	 *        the ID of the input channel this buffer is for
	 * @return the buffer with at least the requested size or <code>null</code> if no such buffer is currently available
	 * @throws IOException
	 *         thrown if an I/O error occurs while allocating the buffer
	 */
	Buffer requestEmptyReadBuffer(int minimumSizeOfBuffer, ChannelID targetChannelID) throws IOException;

	/**
	 * Requests up to two buffers from the pool of read buffers. The request is specified the given
	 * {@link BufferPairRequest} object. This method blocks until the request can be fulfilled.
	 * 
	 * @param bufferPairRequest
	 *        a buffer pair request specified the characteristics of the requested buffers
	 * @return a buffer pair response containing the requested buffers
	 * @throws InterruptedException
	 *         thrown if the thread waiting for the buffers is interrupted
	 */
	BufferPairResponse requestEmptyReadBuffers(BufferPairRequest bufferPairRequest) throws InterruptedException;

	/**
	 * Returns the file buffer manager used by this buffer provider.
	 * 
	 * @return the file buffer manager used by this buffer provider
	 */
	FileBufferManager getFileBufferManager();

	/**
	 * Returns the maximum buffer size in bytes available at this buffer provider.
	 * 
	 * @return the maximum buffer size in bytes available at this buffer provider
	 */
	int getMaximumBufferSize();
}
