/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.buffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.core.memory.MemorySegment;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;

/**
 * A fixed size pool of {@link MemorySegment} instances for the network stack.
 * <p/>
 * This class is thread-safe.
 */
public class NetworkBufferPool implements MemorySegmentPool, BufferPoolFactory {

	private final static Log LOG = LogFactory.getLog(NetworkBufferPool.class);

	private final Object factoryLock = new Object();

	private final int memorySegmentSize;

	private final int numMemorySegments;

	private final Queue<MemorySegment> availableMemorySegments;

	private final Queue<MemorySegmentFuture> pendingRequests = new ArrayDeque<MemorySegmentFuture>();

	private final Set<LocalBufferPool> managedBufferPools = new HashSet<LocalBufferPool>();

	private boolean isDestroyed;

	private int numTotalRequiredBuffers;

	/**
	 * Allocates all {@link MemorySegment} instances managed by this pool.
	 */
	public NetworkBufferPool(int numberOfMemorySegmentsToAllocate, int memorySegmentSize) {
		this.numMemorySegments = numberOfMemorySegmentsToAllocate;
		this.memorySegmentSize = memorySegmentSize;
		this.availableMemorySegments = new ArrayDeque<MemorySegment>(numberOfMemorySegmentsToAllocate);

		try {
			for (int i = 0; i < numberOfMemorySegmentsToAllocate; i++) {
				byte[] buf = new byte[memorySegmentSize];
				availableMemorySegments.add(new MemorySegment(buf));
			}
		} catch (OutOfMemoryError err) {
			int memRequiredMb = (numberOfMemorySegmentsToAllocate * memorySegmentSize) >> 20;
			int memAllocatedMb = ((availableMemorySegments.size()) * memorySegmentSize) >> 20;
			int memMissingMb = memRequiredMb - memAllocatedMb;

			throw new OutOfMemoryError(
					"Could not allocate enough memory segments for GlobalBufferPool (" +
							"required (Mb): " + memRequiredMb + ", " +
							"allocated (Mb): " + memAllocatedMb + ", " +
							"missing (Mb): " + memMissingMb + ")."
			);
		}

		int memAllocatedMb = ((availableMemorySegments.size()) * memorySegmentSize) >> 20;
		LOG.info(String.format("Allocated " + memAllocatedMb + " Mb for memory segment pool (" +
				"memory segments: " + availableMemorySegments + ", " +
				"bytes per segment: " + memorySegmentSize + ")."));
	}

	@Override
	public MemorySegmentFuture requestMemorySegment() {
		synchronized (availableMemorySegments) {
			checkState(!isDestroyed, "Memory segment pool already destroyed.");

			MemorySegment segment = availableMemorySegments.poll();

			if (segment != null) {
				return new MemorySegmentFuture(segment);
			}

			MemorySegmentFuture future = new MemorySegmentFuture();
			pendingRequests.add(future);

			return future;
		}
	}

	@Override
	public void recycle(MemorySegment memorySegment) {
		synchronized (availableMemorySegments) {
			if (isDestroyed) {
				memorySegment.free();
			}
			else if (!pendingRequests.isEmpty()) {
				MemorySegmentFuture future = pendingRequests.poll();
				if (future.isCancelled()) {
					recycle(memorySegment);
				}
				else {
					future.handInMemorySegment(memorySegment);
				}
			}
			else {
				availableMemorySegments.add(memorySegment);
			}
		}
	}

	@Override
	public void destroy() {
		synchronized (availableMemorySegments) {
			checkState(!isDestroyed, "Memory segment pool already destroyed.");

			while (!availableMemorySegments.isEmpty()) {
				availableMemorySegments.remove().free();
			}

			while (!pendingRequests.isEmpty()) {
				pendingRequests.poll().cancel();
			}

			isDestroyed = true;
		}
	}

	@Override
	public int getMemorySegmentSize() {
		return memorySegmentSize;
	}

	@Override
	public int getNumMemorySegments() {
		return numMemorySegments;
	}

	@Override
	public int getNumAvailableMemorySegments() {
		synchronized (availableMemorySegments) {
			return availableMemorySegments.size();
		}
	}

	@Override
	public boolean isDestroyed() {
		synchronized (availableMemorySegments) {
			return isDestroyed;
		}
	}

	// ------------------------------------------------------------------------
	// BufferPoolFactory
	// ------------------------------------------------------------------------

	@Override
	public BufferPool createBufferPool(BufferPoolOwner owner, int numRequiredBuffers, boolean isFixedSize) {
		// It is necessary to use a separate lock from the one used for buffer
		// requests to ensure deadlock freedom for failure cases.
		synchronized (factoryLock) {
			// Ensure that the number of required buffers can be satisfied.
			// With dynamic memory management this should become obsolete.
			if (numTotalRequiredBuffers + numRequiredBuffers > numMemorySegments) {
				String msg = String.format("Insufficient number of network buffers: required %d, but only %d of %d available.",
						numRequiredBuffers, numMemorySegments - numTotalRequiredBuffers, numMemorySegments);

				throw new IllegalStateException(msg);
			}

			this.numTotalRequiredBuffers += numRequiredBuffers;

			// We are good to go, create a new buffer pool and redistribute
			// non-fixed size buffers.
			LocalBufferPool localBufferPool = new LocalBufferPool(owner, this, numRequiredBuffers);

			// The fixed size pools get their share of buffers and don't change
			// it during their lifetime.
			if (!isFixedSize) {
				managedBufferPools.add(localBufferPool);
			}

			redistributeBuffers();

			return localBufferPool;
		}
	}

	@Override
	public void destroyBufferPool(BufferPool bufferPool) {
		synchronized (factoryLock) {
			numTotalRequiredBuffers -= bufferPool.getNumRequiredBuffers();

			managedBufferPools.remove(bufferPool);

			redistributeBuffers();
		}
	}

	// Must be called from synchronized block
	private void redistributeBuffers() {
		int numManagedBufferPools = managedBufferPools.size();

		if (numManagedBufferPools == 0) {
			return; // necessary to avoid div by zero when no managed pools
		}

		// All buffers, which are not among the required ones
		int numAvailableMemorySegment = numMemorySegments - numTotalRequiredBuffers;

		// Available excess (not required) buffers per pool
		int numExcessBuffersPerPool = numAvailableMemorySegment / numManagedBufferPools;

		// Distribute leftover buffers in round robin fashion
		int numLeftoverBuffers = numAvailableMemorySegment % numManagedBufferPools;

		int bufferPoolIndex = 0;

		for (LocalBufferPool bufferPool : managedBufferPools) {
			int leftoverBuffers = bufferPoolIndex++ < numLeftoverBuffers ? 1 : 0;

			bufferPool.setNumBuffers(bufferPool.getNumRequiredBuffers() + numExcessBuffersPerPool + leftoverBuffers);
		}
	}
}
