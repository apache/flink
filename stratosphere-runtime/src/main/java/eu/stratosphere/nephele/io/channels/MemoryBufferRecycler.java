/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.io.channels;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.core.memory.MemorySegment;

/**
 * A memory buffer recycler takes care of the correct recycling of the internal byte buffer which backs a memory buffer.
 * Since buffer objects can be duplicated, i.e. multiple buffer objects point to the same physical buffer, it is
 * necessary to coordinate the recycling of the physical buffer.
 * <p>
 * This class is thread-safe.
 * 
 */
public final class MemoryBufferRecycler {

	/**
	 * The log object used to report debug information and possible errors.
	 */
	private static final Log LOG = LogFactory.getLog(MemoryBufferRecycler.class);

	/**
	 * The original memory segment which has been taken from byte buffered channel manager's buffer pool.
	 */
	private final MemorySegment originalSegment;

	/**
	 * The connection to the pool from which the byte buffer has originally been taken.
	 */
	private final MemoryBufferPoolConnector bufferPoolConnector;

	/**
	 * The number of memory buffer objects which may still access the physical buffer.
	 */
	public final AtomicInteger referenceCounter = new AtomicInteger(1);

	/**
	 * Constructs a new memory buffer recycler.
	 * 
	 * @param originalBuffer
	 *        the original byte buffer
	 * @param bufferPoolConnector
	 *        the connection to the pool from which the byte buffer has originally been taken
	 */
	MemoryBufferRecycler(final MemorySegment originalSegment, final MemoryBufferPoolConnector bufferPoolConnector) {

		this.originalSegment = originalSegment;
		this.bufferPoolConnector = bufferPoolConnector;
	}

	/**
	 * Increases the number of references to the physical buffer by one.
	 */
	void increaseReferenceCounter() {

		if (this.referenceCounter.getAndIncrement() == 0) {
			LOG.error("Increasing reference counter from 0 to 1");
		}
	}

	/**
	 * Decreases the number of references to the physical buffer by one. If the number of references becomes zero the
	 * physical buffer is recycled.
	 */
	void decreaseReferenceCounter() {

		final int val = this.referenceCounter.decrementAndGet();
		if (val == 0) {
			this.bufferPoolConnector.recycle(this.originalSegment);

		} else if (val < 0) {
			LOG.error("reference counter is negative");
		}
	}
}
