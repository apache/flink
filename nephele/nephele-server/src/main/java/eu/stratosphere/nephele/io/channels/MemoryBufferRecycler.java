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

package eu.stratosphere.nephele.io.channels;

import java.nio.ByteBuffer;
import java.util.Deque;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A memory buffer recycler takes care of the correct recycling of the internal byte buffer which backs a memory buffer.
 * Since buffer objects can be duplicated, i.e. multiple buffer objects point to the same physical buffer, it is
 * necessary to coordinate the recycling of the physical buffer.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class MemoryBufferRecycler {

	/**
	 * The log object used to report debug information and possible errors.
	 */
	private static final Log LOG = LogFactory.getLog(MemoryBufferRecycler.class);

	/**
	 * The original byte buffer which has been taken from byte buffered channel manager's buffer pool.
	 */
	private final ByteBuffer originalBuffer;

	/**
	 * The queue to which the byte buffer shall be appended after all copies have been processed.
	 */
	private final Deque<ByteBuffer> queueForRecycledBuffers;

	/**
	 * The number of memory buffer objects which may still access the physical buffer.
	 */
	private int referenceCounter = 1;

	/**
	 * Stores if the physical buffer has already been recycled.
	 */
	private boolean bufferAlreadyRecycled = false;

	/**
	 * Constructs a new memory buffer recycler.
	 * 
	 * @param originalBuffer
	 *        the original byte buffer
	 * @param queueForRecycledBuffers
	 *        the queue to append the buffer for recycling
	 */
	MemoryBufferRecycler(final ByteBuffer originalBuffer, final Deque<ByteBuffer> queueForRecycledBuffers) {

		this.originalBuffer = originalBuffer;
		this.queueForRecycledBuffers = queueForRecycledBuffers;
	}

	/**
	 * Increases the number of references to the physical buffer by one.
	 */
	synchronized void increaseReferenceCounter() {

		if (this.bufferAlreadyRecycled) {
			LOG.error("increaseReferenceCounter called although buffer has already been recycled");
		}

		++this.referenceCounter;
	}

	/**
	 * Decreases the number of references to the physical buffer by one. If the number of references becomes zero the
	 * physical buffer is recycled.
	 */
	synchronized void decreaseReferenceCounter() {

		if (this.bufferAlreadyRecycled) {
			LOG.error("decreaseReferenceCounter called although buffer has already been recycled");
		}

		--this.referenceCounter;

		if (this.referenceCounter <= 0) {

			this.originalBuffer.clear();
			
			synchronized (this.queueForRecycledBuffers) {
				this.queueForRecycledBuffers.add(this.originalBuffer);
				this.queueForRecycledBuffers.notify();
			}
		}

		this.bufferAlreadyRecycled = true;
	}
}
