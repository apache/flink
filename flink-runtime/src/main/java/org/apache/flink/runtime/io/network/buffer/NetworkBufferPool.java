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

import org.apache.flink.core.memory.MemorySegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * A fixed size pool of {@link MemorySegment} instances for the network stack.
 * <p>
 * This class is thread-safe.
 */
public class NetworkBufferPool implements BufferPoolFactory {

	private static final Logger LOG = LoggerFactory.getLogger(NetworkBufferPool.class);

	private final int totalNumberOfMemorySegments;

	private final int memorySegmentSize;

	private final Queue<MemorySegment> availableMemorySegments;

	private volatile boolean isDestroyed;

	// ---- Managed buffer pools ----------------------------------------------

	private final Object factoryLock = new Object();

	private final Set<LocalBufferPool> managedBufferPools = new HashSet<LocalBufferPool>();

	public final Set<LocalBufferPool> allBufferPools = new HashSet<LocalBufferPool>();

	private int numTotalRequiredBuffers;

	/**
	 * Allocates all {@link MemorySegment} instances managed by this pool.
	 */
	public NetworkBufferPool(int numberOfSegmentsToAllocate, int segmentSize) {
		this.totalNumberOfMemorySegments = numberOfSegmentsToAllocate;
		this.memorySegmentSize = segmentSize;
		this.availableMemorySegments = new ArrayBlockingQueue<MemorySegment>(numberOfSegmentsToAllocate);

		try {
			for (int i = 0; i < numberOfSegmentsToAllocate; i++) {
				availableMemorySegments.add(new MemorySegment(new byte[segmentSize]));
			}
		}
		catch (OutOfMemoryError err) {
			int requiredMb = (numberOfSegmentsToAllocate * segmentSize) >> 20;
			int allocatedMb = ((availableMemorySegments.size()) * segmentSize) >> 20;
			int missingMb = requiredMb - allocatedMb;

			throw new OutOfMemoryError("Could not allocate enough memory segments for GlobalBufferPool (required (Mb): " + requiredMb + ", allocated (Mb): " + allocatedMb + ", missing (Mb): " + missingMb + ").");
		}

		int allocatedMb = ((availableMemorySegments.size()) * segmentSize) >> 20;

		LOG.info("Allocated {} MB for network buffer pool (number of memory segments: {}, bytes per segment: {}).", allocatedMb, availableMemorySegments.size(), segmentSize);
	}

	public MemorySegment requestMemorySegment() {
		return availableMemorySegments.poll();
	}

	// This is not safe with regard to destroy calls, but it does not hurt, because destroy happens
	// only once at clean up time (task manager shutdown).
	public void recycle(MemorySegment segment) {
		availableMemorySegments.add(segment);
	}

	public void destroy() {
		synchronized (factoryLock) {
			isDestroyed = true;

			MemorySegment segment;
			while ((segment = availableMemorySegments.poll()) != null) {
				segment.free();
			}
		}
	}

	public boolean isDestroyed() {
		return isDestroyed;
	}

	public int getMemorySegmentSize() {
		return memorySegmentSize;
	}

	public int getTotalNumberOfMemorySegments() {
		return totalNumberOfMemorySegments;
	}

	public int getNumberOfAvailableMemorySegments() {
		return availableMemorySegments.size();
	}

	public int getNumberOfRegisteredBufferPools() {
		synchronized (factoryLock) {
			return allBufferPools.size();
		}
	}

	public int countBuffers() {
		int buffers = 0;

		synchronized (factoryLock) {
			for (BufferPool bp : allBufferPools) {
				buffers += bp.getNumBuffers();
			}
		}

		return buffers;
	}

	// ------------------------------------------------------------------------
	// BufferPoolFactory
	// ------------------------------------------------------------------------

	@Override
	public BufferPool createBufferPool(int numRequiredBuffers, boolean isFixedSize) throws IOException {
		// It is necessary to use a separate lock from the one used for buffer
		// requests to ensure deadlock freedom for failure cases.
		synchronized (factoryLock) {
			if (isDestroyed) {
				throw new IllegalStateException("Network buffer pool has already been destroyed.");
			}

			// Ensure that the number of required buffers can be satisfied.
			// With dynamic memory management this should become obsolete.
			if (numTotalRequiredBuffers + numRequiredBuffers > totalNumberOfMemorySegments) {
				throw new IOException(String.format("Insufficient number of network buffers: required %d, but only %d of %d available.", numRequiredBuffers, totalNumberOfMemorySegments - numTotalRequiredBuffers, totalNumberOfMemorySegments));
			}

			this.numTotalRequiredBuffers += numRequiredBuffers;

			// We are good to go, create a new buffer pool and redistribute
			// non-fixed size buffers.
			LocalBufferPool localBufferPool = new LocalBufferPool(this, numRequiredBuffers);

			// The fixed size pools get their share of buffers and don't change
			// it during their lifetime.
			if (!isFixedSize) {
				managedBufferPools.add(localBufferPool);
			}

			allBufferPools.add(localBufferPool);

			redistributeBuffers();

			return localBufferPool;
		}
	}

	@Override
	public void destroyBufferPool(BufferPool bufferPool) throws IOException {
		synchronized (factoryLock) {
			if (allBufferPools.remove(bufferPool)) {
				managedBufferPools.remove(bufferPool);

				numTotalRequiredBuffers -= bufferPool.getNumberOfRequiredMemorySegments();

				redistributeBuffers();
			}
		}
	}

	// Must be called from synchronized block
	private void redistributeBuffers() throws IOException {
		int numManagedBufferPools = managedBufferPools.size();

		if (numManagedBufferPools == 0) {
			return; // necessary to avoid div by zero when no managed pools
		}

		// All buffers, which are not among the required ones
		int numAvailableMemorySegment = totalNumberOfMemorySegments - numTotalRequiredBuffers;

		// Available excess (not required) buffers per pool
		int numExcessBuffersPerPool = numAvailableMemorySegment / numManagedBufferPools;

		// Distribute leftover buffers in round robin fashion
		int numLeftoverBuffers = numAvailableMemorySegment % numManagedBufferPools;

		int bufferPoolIndex = 0;

		for (LocalBufferPool bufferPool : managedBufferPools) {
			int leftoverBuffers = bufferPoolIndex++ < numLeftoverBuffers ? 1 : 0;

			bufferPool.setNumBuffers(bufferPool.getNumberOfRequiredMemorySegments() + numExcessBuffersPerPool + leftoverBuffers);
		}
	}
}
