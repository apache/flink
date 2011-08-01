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

package eu.stratosphere.nephele.taskmanager.bytebuffered;

import java.io.IOException;

import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferProvider;
import eu.stratosphere.nephele.taskmanager.bufferprovider.LocalBufferCache;

final class TaskContext implements BufferProvider {

	private final LocalBufferCache localBufferCache;

	private final String taskName;

	public TaskContext(final String taskName) {

		this.localBufferCache = new LocalBufferCache(1, false);

		this.taskName = taskName;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Buffer requestEmptyBuffer(final int minimumSizeOfBuffer, final int minimumReserve) throws IOException {

		return this.localBufferCache.requestEmptyBuffer(minimumSizeOfBuffer, minimumReserve);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Buffer requestEmptyBufferBlocking(int minimumSizeOfBuffer, final int minimumReserve) throws IOException,
			InterruptedException {

		return this.localBufferCache.requestEmptyBufferBlocking(minimumSizeOfBuffer, minimumReserve);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getMaximumBufferSize() {

		return this.localBufferCache.getMaximumBufferSize();
	}

	public void releaseAllResources() {

		// Clear the buffer cache
		this.localBufferCache.clear();
	}

	public void setBufferLimit(int bufferLimit) {

		this.localBufferCache.setDesignatedNumberOfBuffers(bufferLimit);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isShared() {

		return false;
	}

	public void logBufferUtilization() {

		final int ava = this.localBufferCache.getNumberOfAvailableBuffers();
		final int req = this.localBufferCache.getRequestedNumberOfBuffers();
		final int des = this.localBufferCache.getDesignatedNumberOfBuffers();

		System.out.println("\t\t" + this.taskName + ": " + ava + " available, " + req + " requested, " + des
			+ " designated");

	}
}
