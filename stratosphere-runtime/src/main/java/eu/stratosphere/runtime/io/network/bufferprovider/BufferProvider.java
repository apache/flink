/***********************************************************************************************************************
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.runtime.io.network.bufferprovider;

import eu.stratosphere.runtime.io.Buffer;

import java.io.IOException;

public interface BufferProvider {

	/**
	 * Requests a buffer with a minimum size of <code>minBufferSize</code>. The method returns immediately, even if the
	 * request could not be fulfilled.
	 *
	 * @param minBufferSize minimum size of the requested buffer (in bytes)
	 * @return buffer with at least the requested size or <code>null</code> if no such buffer is currently available
	 * @throws IOException
	 */
	Buffer requestBuffer(int minBufferSize) throws IOException;

	/**
	 * Requests a buffer with a minimum size of <code>minBufferSize</code>. The method blocks until the request has
	 * been fulfilled or {@link #reportAsynchronousEvent()} has been called.
	 *
	 * @param minBufferSize minimum size of the requested buffer (in bytes)
	 * @return buffer with at least the requested size
	 * @throws IOException
	 * @throws InterruptedException
	 */
	Buffer requestBufferBlocking(int minBufferSize) throws IOException, InterruptedException;

	/**
	 * Returns the size of buffers (in bytes) available at this buffer provider.
	 * 
	 * @return size of buffers (in bytes) available at this buffer provider
	 */
	int getBufferSize();

	/**
	 * Reports an asynchronous event and interrupts each blocking method of this buffer provider in order to allow the
	 * blocked thread to respond to the event.
	 */
	void reportAsynchronousEvent();

	/**
	 * Registers the given {@link BufferAvailabilityListener} with an empty buffer pool.
	 * <p>
	 * The registration only succeeds, if the buffer pool is empty and has not been destroyed yet.
	 * <p>
	 * The registered listener will receive a notification when at least one buffer has become available again. After
	 * the notification, the listener will be unregistered.
	 *
	 * @param listener the listener to be registered
	 * @return <code>true</code> if the registration has been successful; <code>false</code> if the registration
	 *         failed, because the buffer pool was not empty or has already been destroyed
	 */
	boolean registerBufferAvailabilityListener(BufferAvailabilityListener listener);
}
