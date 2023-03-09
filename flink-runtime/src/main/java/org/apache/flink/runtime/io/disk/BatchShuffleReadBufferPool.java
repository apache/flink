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

package org.apache.flink.runtime.io.disk;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A fixed-size {@link MemorySegment} pool used by batch shuffle for shuffle data read (currently
 * only used by sort-merge blocking shuffle).
 */
public class BatchShuffleReadBufferPool {

    private static final Logger LOG = LoggerFactory.getLogger(BatchShuffleReadBufferPool.class);

    /**
     * Memory size in bytes can be allocated from this buffer pool for a single request (4M is for
     * better sequential read).
     */
    public static final int NUM_BYTES_PER_REQUEST = 4 * 1024 * 1024;

    /**
     * Wait for at most 2 seconds before return if there is no enough available buffers currently.
     */
    private static final Duration WAITING_TIME = Duration.ofSeconds(2);

    /** Total direct memory size in bytes can can be allocated and used by this buffer pool. */
    private final long totalBytes;

    /** The number of total buffers in this buffer pool. */
    private final int numTotalBuffers;

    /** Size of each buffer in bytes in this buffer pool. */
    private final int bufferSize;

    /** The number of buffers to be returned for a single request. */
    private final int numBuffersPerRequest;

    /** All requesters which need to request buffers from this pool currently. */
    private final Set<Object> bufferRequesters = ConcurrentHashMap.newKeySet();

    /** All available buffers in this buffer pool currently. */
    @GuardedBy("buffers")
    private final Queue<MemorySegment> buffers = new ArrayDeque<>();

    /** The timestamp when the last buffer is recycled or allocated. */
    @GuardedBy("buffers")
    private long lastBufferOperationTimestamp = System.currentTimeMillis();

    /** Whether this buffer pool has been destroyed or not. */
    @GuardedBy("buffers")
    private boolean destroyed;

    /** Whether this buffer pool has been initialized or not. */
    @GuardedBy("buffers")
    private boolean initialized;

    public BatchShuffleReadBufferPool(long totalBytes, int bufferSize) {
        checkArgument(totalBytes > 0, "Total memory size must be positive.");
        checkArgument(bufferSize > 0, "Size of buffer must be positive.");
        checkArgument(
                totalBytes >= bufferSize,
                String.format(
                        "Illegal configuration, config value for '%s' must be no smaller than '%s',"
                                + " please increase '%s' to at least %d bytes.",
                        TaskManagerOptions.NETWORK_BATCH_SHUFFLE_READ_MEMORY.key(),
                        TaskManagerOptions.MEMORY_SEGMENT_SIZE.key(),
                        TaskManagerOptions.NETWORK_BATCH_SHUFFLE_READ_MEMORY.key(),
                        bufferSize));

        this.totalBytes = totalBytes;
        this.bufferSize = bufferSize;

        this.numTotalBuffers = (int) Math.min(totalBytes / bufferSize, Integer.MAX_VALUE);
        this.numBuffersPerRequest =
                Math.min(numTotalBuffers, Math.max(1, NUM_BYTES_PER_REQUEST / bufferSize));
    }

    @VisibleForTesting
    long getTotalBytes() {
        return totalBytes;
    }

    @VisibleForTesting
    public int getNumTotalBuffers() {
        return numTotalBuffers;
    }

    @VisibleForTesting
    public int getAvailableBuffers() {
        synchronized (buffers) {
            return buffers.size();
        }
    }

    public int getNumBuffersPerRequest() {
        return numBuffersPerRequest;
    }

    public int getMaxConcurrentRequests() {
        return numBuffersPerRequest > 0 ? numTotalBuffers / numBuffersPerRequest : 0;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    /** Initializes this buffer pool which allocates all the buffers. */
    public void initialize() {
        synchronized (buffers) {
            checkState(!destroyed, "Buffer pool is already destroyed.");

            if (initialized) {
                return;
            }
            initialized = true;

            try {
                for (int i = 0; i < numTotalBuffers; ++i) {
                    buffers.add(MemorySegmentFactory.allocateUnpooledOffHeapMemory(bufferSize));
                }
            } catch (OutOfMemoryError outOfMemoryError) {
                int allocated = buffers.size();
                buffers.forEach(MemorySegment::free);
                buffers.clear();
                throw new OutOfMemoryError(
                        String.format(
                                "Can't allocate enough direct buffer for batch shuffle read buffer "
                                        + "pool (bytes allocated: %d, bytes still needed: %d). To "
                                        + "avoid the exception, you need to do one of the following"
                                        + " adjustments: 1) If you have ever decreased %s, you need"
                                        + " to undo the decrement; 2) If you ever increased %s, you"
                                        + " should also increase %s; 3) If neither the above cases,"
                                        + " it usually means some other parts of your application "
                                        + "have consumed too many direct memory and the value of %s"
                                        + " should be increased.",
                                allocated * bufferSize,
                                (numTotalBuffers - allocated) * bufferSize,
                                TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY.key(),
                                TaskManagerOptions.NETWORK_BATCH_SHUFFLE_READ_MEMORY.key(),
                                TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY.key(),
                                TaskManagerOptions.TASK_OFF_HEAP_MEMORY.key()));
            }
        }

        LOG.info(
                "Batch shuffle IO buffer pool initialized: numBuffers={}, bufferSize={}.",
                numTotalBuffers,
                bufferSize);
    }

    public void registerRequester(Object requester) {
        bufferRequesters.add(requester);
    }

    public void unregisterRequester(Object requester) {
        bufferRequesters.remove(requester);
    }

    public int getAverageBuffersPerRequester() {
        return Math.max(1, numTotalBuffers / Math.max(1, bufferRequesters.size()));
    }

    /**
     * Requests a collection of buffers (determined by {@link #numBuffersPerRequest}) from this
     * buffer pool.
     */
    public List<MemorySegment> requestBuffers() throws Exception {
        List<MemorySegment> allocated = new ArrayList<>(numBuffersPerRequest);
        synchronized (buffers) {
            checkState(!destroyed, "Buffer pool is already destroyed.");

            if (!initialized) {
                initialize();
            }

            Deadline deadline = Deadline.fromNow(WAITING_TIME);
            while (buffers.size() < numBuffersPerRequest) {
                checkState(!destroyed, "Buffer pool is already destroyed.");

                buffers.wait(WAITING_TIME.toMillis());
                if (!deadline.hasTimeLeft()) {
                    return allocated; // return the empty list
                }
            }

            while (allocated.size() < numBuffersPerRequest) {
                allocated.add(buffers.poll());
            }
            lastBufferOperationTimestamp = System.currentTimeMillis();
        }
        return allocated;
    }

    /**
     * Recycles the target buffer to this buffer pool. This method should never throw any exception.
     */
    public void recycle(MemorySegment segment) {
        checkArgument(segment != null, "Buffer must be not null.");

        recycle(Collections.singletonList(segment));
    }

    /**
     * Recycles a collection of buffers to this buffer pool. This method should never throw any
     * exception.
     */
    public void recycle(Collection<MemorySegment> segments) {
        checkArgument(segments != null, "Buffer list must be not null.");

        if (segments.isEmpty()) {
            return;
        }

        synchronized (buffers) {
            checkState(initialized, "Recycling a buffer before initialization.");

            if (destroyed) {
                segments.forEach(MemorySegment::free);
                return;
            }

            boolean shouldNotify =
                    buffers.size() < numBuffersPerRequest
                            && buffers.size() + segments.size() >= numBuffersPerRequest;
            buffers.addAll(segments);
            lastBufferOperationTimestamp = System.currentTimeMillis();
            if (shouldNotify) {
                buffers.notifyAll();
            }
        }
    }

    public long getLastBufferOperationTimestamp() {
        synchronized (buffers) {
            return lastBufferOperationTimestamp;
        }
    }

    /** Destroys this buffer pool and after which, no buffer can be allocated any more. */
    public void destroy() {
        synchronized (buffers) {
            destroyed = true;

            buffers.clear();
            buffers.notifyAll();
        }
    }

    public boolean isDestroyed() {
        synchronized (buffers) {
            return destroyed;
        }
    }
}
