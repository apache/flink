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

public interface BufferProvider {

	/**
	 * Requests an empty buffer with a minimum size of <code>minimumSizeOfBuffer</code>. The method returns
	 * immediately, even if the request could not be fulfilled. Note that <code>minimumSizeOfBuffer</code> must not
	 * exceed the value returned by the method <code>getMaximumBufferSize()</code>.
	 * 
	 * @param minimumSizeOfBuffer
	 *        the minimum size of the requested read buffer in bytes
	 * @param minimumReserve
	 *        the minimum buffer reserve that must be kept by the buffer provider
	 * @return the buffer with at least the requested size or <code>null</code> if no such buffer is currently available
	 * @throws IOException
	 *         thrown if an I/O error occurs while allocating the buffer
	 */
	Buffer requestEmptyBuffer(int minimumSizeOfBuffer, int minimumReserve) throws IOException;

	/**
	 * Requests an empty buffer with a minimum size of <code>minimumSizeOfBuffer</code>. The method blocks
	 * until the request can be fulfilled. Note that <code>minimumSizeOfBuffer</code> must not
	 * exceed the value returned by the method <code>getMaximumBufferSize()</code>.
	 * 
	 * @param minimumSizeOfBuffer
	 *        the minimum size of the requested read buffer in bytes
	 * @param minimumReserve
	 *        the minimum buffer reserve that must be kept by the buffer provider
	 * @return the buffer with at least the requested size
	 * @throws IOException
	 *         thrown if an I/O error occurs while allocating the buffer
	 * @throws InterruptedException
	 *         thrown if the thread waiting for the buffer is interrupted
	 */
	Buffer requestEmptyBufferBlocking(int minimumSizeOfBuffer, int minimumReserve) throws IOException,
			InterruptedException;

	/**
	 * Returns the maximum buffer size in bytes available at this buffer provider.
	 * 
	 * @return the maximum buffer size in bytes available at this buffer provider
	 */
	int getMaximumBufferSize();

	/**
	 * Returns if this buffer provider is shared between different entities (for examples tasks).
	 * 
	 * @return <code>true</code> if this buffer provider is shared, <code>false</code> otherwise
	 */
	boolean isShared();
}
