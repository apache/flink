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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The NetworkBufferPool is a fixed size pool of {@link MemorySegment} instances for the network
 * stack.
 *
 * <p>The NetworkBufferPool creates {@link LocalBufferPool}s from which the individual tasks draw
 * the buffers for the network data transfer. When new local buffer pools are created, the
 * NetworkBufferPool dynamically redistributes the buffers between the pools. The redistribution
 * logic is to make the number of buffers of each buffer pool fall within the minimum and maximum
 * numbers while endeavoring to closely align with its individual expected number.
 */
public class NetworkBufferPool
        implements BufferPoolFactory, MemorySegmentProvider, AvailabilityProvider {

    public static final int UNBOUNDED_POOL_SIZE = Integer.MAX_VALUE;

    private static final int USAGE_WARNING_THRESHOLD = 100;

    private static final Logger LOG = LoggerFactory.getLogger(NetworkBufferPool.class);

    private final int totalNumberOfMemorySegments;

    private final int memorySegmentSize;

    private final ArrayDeque<MemorySegment> availableMemorySegments;

    private volatile boolean isDestroyed;

    // ---- Managed buffer pools ----------------------------------------------

    private final Object factoryLock = new Object();

    private final Set<LocalBufferPool> allBufferPools = new HashSet<>();

    private final Set<LocalBufferPool> resizableBufferPools = new HashSet<>();

    private int numTotalRequiredBuffers;

    private final Duration requestSegmentsTimeout;

    private final AvailabilityHelper availabilityHelper = new AvailabilityHelper();

    private int lastCheckedUsage = -1;

    @VisibleForTesting
    public NetworkBufferPool(int numberOfSegmentsToAllocate, int segmentSize) {
        this(numberOfSegmentsToAllocate, segmentSize, Duration.ofMillis(Integer.MAX_VALUE));
    }

    /** Allocates all {@link MemorySegment} instances managed by this pool. */
    public NetworkBufferPool(
            int numberOfSegmentsToAllocate, int segmentSize, Duration requestSegmentsTimeout) {
        this.totalNumberOfMemorySegments = numberOfSegmentsToAllocate;
        this.memorySegmentSize = segmentSize;

        Preconditions.checkNotNull(requestSegmentsTimeout);
        checkArgument(
                requestSegmentsTimeout.toMillis() > 0,
                "The timeout for requesting exclusive buffers should be positive.");
        this.requestSegmentsTimeout = requestSegmentsTimeout;

        final long sizeInLong = (long) segmentSize;

        try {
            this.availableMemorySegments = new ArrayDeque<>(numberOfSegmentsToAllocate);
        } catch (OutOfMemoryError err) {
            throw new OutOfMemoryError(
                    "Could not allocate buffer queue of length "
                            + numberOfSegmentsToAllocate
                            + " - "
                            + err.getMessage());
        }

        try {
            for (int i = 0; i < numberOfSegmentsToAllocate; i++) {
                availableMemorySegments.add(
                        MemorySegmentFactory.allocateUnpooledOffHeapMemory(segmentSize, null));
            }
        } catch (OutOfMemoryError err) {
            int allocated = availableMemorySegments.size();

            // free some memory
            availableMemorySegments.clear();

            long requiredMb = (sizeInLong * numberOfSegmentsToAllocate) >> 20;
            long allocatedMb = (sizeInLong * allocated) >> 20;
            long missingMb = requiredMb - allocatedMb;

            throw new OutOfMemoryError(
                    "Could not allocate enough memory segments for NetworkBufferPool "
                            + "(required (MB): "
                            + requiredMb
                            + ", allocated (MB): "
                            + allocatedMb
                            + ", missing (MB): "
                            + missingMb
                            + "). Cause: "
                            + err.getMessage());
        }

        availabilityHelper.resetAvailable();

        long allocatedMb = (sizeInLong * availableMemorySegments.size()) >> 20;

        LOG.info(
                "Allocated {} MB for network buffer pool (number of memory segments: {}, bytes per segment: {}).",
                allocatedMb,
                availableMemorySegments.size(),
                segmentSize);
    }

    /**
     * Different from {@link #requestUnpooledMemorySegments} for unpooled segments allocation. This
     * method and the below {@link #requestPooledMemorySegmentsBlocking} method are designed to be
     * used from {@link LocalBufferPool} for pooled memory segments allocation. Note that these
     * methods for pooled memory segments requesting and recycling are prohibited from acquiring the
     * factoryLock to avoid deadlock.
     */
    @Nullable
    public MemorySegment requestPooledMemorySegment() {
        synchronized (availableMemorySegments) {
            return internalRequestMemorySegment();
        }
    }

    public List<MemorySegment> requestPooledMemorySegmentsBlocking(int numberOfSegmentsToRequest)
            throws IOException {
        return internalRequestMemorySegments(numberOfSegmentsToRequest);
    }

    /**
     * Corresponding to {@link #requestPooledMemorySegmentsBlocking} and {@link
     * #requestPooledMemorySegment}, this method is for pooled memory segments recycling.
     */
    public void recyclePooledMemorySegment(MemorySegment segment) {
        // Adds the segment back to the queue, which does not immediately free the memory
        // however, since this happens when references to the global pool are also released,
        // making the availableMemorySegments queue and its contained object reclaimable
        internalRecycleMemorySegments(Collections.singleton(checkNotNull(segment)));
    }

    /**
     * Unpooled memory segments are requested directly from {@link NetworkBufferPool}, as opposed to
     * pooled segments, that are requested through {@link BufferPool} that was created from this
     * {@link NetworkBufferPool} (see {@link #createBufferPool}). They are used for example for
     * exclusive {@link RemoteInputChannel} credits, that are permanently assigned to that channel,
     * and never returned to any {@link BufferPool}. As opposed to pooled segments, when requested,
     * unpooled segments needs to be accounted against {@link #numTotalRequiredBuffers}, which might
     * require redistribution of the segments.
     */
    @Override
    public List<MemorySegment> requestUnpooledMemorySegments(int numberOfSegmentsToRequest)
            throws IOException {
        checkArgument(
                numberOfSegmentsToRequest >= 0,
                "Number of buffers to request must be non-negative.");

        synchronized (factoryLock) {
            if (isDestroyed) {
                throw new IllegalStateException("Network buffer pool has already been destroyed.");
            }

            if (numberOfSegmentsToRequest == 0) {
                return Collections.emptyList();
            }

            tryRedistributeBuffers(numberOfSegmentsToRequest);
        }

        try {
            return internalRequestMemorySegments(numberOfSegmentsToRequest);
        } catch (IOException exception) {
            revertRequiredBuffers(numberOfSegmentsToRequest);
            ExceptionUtils.rethrowIOException(exception);
            return null;
        }
    }

    private List<MemorySegment> internalRequestMemorySegments(int numberOfSegmentsToRequest)
            throws IOException {
        final List<MemorySegment> segments = new ArrayList<>(numberOfSegmentsToRequest);
        try {
            final Deadline deadline = Deadline.fromNow(requestSegmentsTimeout);
            while (true) {
                if (isDestroyed) {
                    throw new IllegalStateException("Buffer pool is destroyed.");
                }

                MemorySegment segment;
                synchronized (availableMemorySegments) {
                    if ((segment = internalRequestMemorySegment()) == null) {
                        availableMemorySegments.wait(2000);
                    }
                }
                if (segment != null) {
                    segments.add(segment);
                }

                if (segments.size() >= numberOfSegmentsToRequest) {
                    break;
                }

                if (!deadline.hasTimeLeft()) {
                    throw new IOException(
                            String.format(
                                    "Timeout triggered when requesting exclusive buffers: %s, "
                                            + " or you may increase the timeout which is %dms by setting the key '%s'.",
                                    getConfigDescription(),
                                    requestSegmentsTimeout.toMillis(),
                                    NettyShuffleEnvironmentOptions
                                            .NETWORK_EXCLUSIVE_BUFFERS_REQUEST_TIMEOUT_MILLISECONDS
                                            .key()));
                }
            }
        } catch (Throwable e) {
            internalRecycleMemorySegments(segments);
            ExceptionUtils.rethrowIOException(e);
        }

        return segments;
    }

    @Nullable
    private MemorySegment internalRequestMemorySegment() {
        assert Thread.holdsLock(availableMemorySegments);

        final MemorySegment segment = availableMemorySegments.poll();
        if (availableMemorySegments.isEmpty() && segment != null) {
            availabilityHelper.resetUnavailable();
        }
        return segment;
    }

    /**
     * Corresponding to {@link #requestUnpooledMemorySegments}, this method is for unpooled memory
     * segments recycling.
     */
    @Override
    public void recycleUnpooledMemorySegments(Collection<MemorySegment> segments) {
        internalRecycleMemorySegments(segments);
        revertRequiredBuffers(segments.size());
    }

    private void revertRequiredBuffers(int size) {
        synchronized (factoryLock) {
            numTotalRequiredBuffers -= size;

            // note: if this fails, we're fine for the buffer pool since we already recycled the
            // segments
            redistributeBuffers();
        }
    }

    private void internalRecycleMemorySegments(Collection<MemorySegment> segments) {
        CompletableFuture<?> toNotify = null;
        synchronized (availableMemorySegments) {
            if (availableMemorySegments.isEmpty() && !segments.isEmpty()) {
                toNotify = availabilityHelper.getUnavailableToResetAvailable();
            }
            availableMemorySegments.addAll(segments);
            availableMemorySegments.notifyAll();
        }

        if (toNotify != null) {
            toNotify.complete(null);
        }
    }

    public void destroy() {
        synchronized (factoryLock) {
            isDestroyed = true;
        }

        synchronized (availableMemorySegments) {
            MemorySegment segment;
            while ((segment = availableMemorySegments.poll()) != null) {
                segment.free();
            }
        }
    }

    public boolean isDestroyed() {
        return isDestroyed;
    }

    public int getTotalNumberOfMemorySegments() {
        return isDestroyed() ? 0 : totalNumberOfMemorySegments;
    }

    public long getTotalMemory() {
        return (long) getTotalNumberOfMemorySegments() * memorySegmentSize;
    }

    public int getNumberOfAvailableMemorySegments() {
        synchronized (availableMemorySegments) {
            return availableMemorySegments.size();
        }
    }

    public long getAvailableMemory() {
        return (long) getNumberOfAvailableMemorySegments() * memorySegmentSize;
    }

    public int getNumberOfUsedMemorySegments() {
        return getTotalNumberOfMemorySegments() - getNumberOfAvailableMemorySegments();
    }

    public long getUsedMemory() {
        return (long) getNumberOfUsedMemorySegments() * memorySegmentSize;
    }

    public int getNumberOfRegisteredBufferPools() {
        synchronized (factoryLock) {
            return allBufferPools.size();
        }
    }

    public long getEstimatedNumberOfRequestedMemorySegments() {
        long requestedSegments = 0;
        synchronized (factoryLock) {
            for (LocalBufferPool bufferPool : allBufferPools) {
                requestedSegments += bufferPool.getEstimatedNumberOfRequestedMemorySegments();
            }
        }
        return requestedSegments;
    }

    public long getEstimatedRequestedMemory() {
        return getEstimatedNumberOfRequestedMemorySegments() * memorySegmentSize;
    }

    public int getEstimatedRequestedSegmentsUsage() {
        int totalNumberOfMemorySegments = getTotalNumberOfMemorySegments();
        return totalNumberOfMemorySegments == 0
                ? 0
                : Math.toIntExact(
                        100L
                                * getEstimatedNumberOfRequestedMemorySegments()
                                / totalNumberOfMemorySegments);
    }

    @VisibleForTesting
    Optional<String> getUsageWarning() {
        int currentUsage = getEstimatedRequestedSegmentsUsage();
        Optional<String> message = Optional.empty();
        // do not log warning if the value hasn't changed to avoid spamming warnings.
        if (currentUsage >= USAGE_WARNING_THRESHOLD && lastCheckedUsage != currentUsage) {
            long totalMemory = getTotalMemory();
            long requestedMemory = getEstimatedRequestedMemory();
            long missingMemory = requestedMemory - totalMemory;
            message =
                    Optional.of(
                            String.format(
                                    "Memory usage [%d%%] is too high to satisfy all of the requests. "
                                            + "This can severely impact network throughput. "
                                            + "Please consider increasing available network memory, "
                                            + "or decreasing configured size of network buffer pools. "
                                            + "(totalMemory=%s, requestedMemory=%s, missingMemory=%s)",
                                    currentUsage,
                                    new MemorySize(totalMemory).toHumanReadableString(),
                                    new MemorySize(requestedMemory).toHumanReadableString(),
                                    new MemorySize(missingMemory).toHumanReadableString()));
        } else if (currentUsage < USAGE_WARNING_THRESHOLD
                && lastCheckedUsage >= USAGE_WARNING_THRESHOLD) {
            message =
                    Optional.of(
                            String.format("Memory usage [%s%%] went back to normal", currentUsage));
        }
        lastCheckedUsage = currentUsage;
        return message;
    }

    public void maybeLogUsageWarning() {
        Optional<String> usageWarning = getUsageWarning();
        if (usageWarning.isPresent()) {
            LOG.warn(usageWarning.get());
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

    /** Returns a future that is completed when there are free segments in this pool. */
    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return availabilityHelper.getAvailableFuture();
    }

    // ------------------------------------------------------------------------
    // BufferPoolFactory
    // ------------------------------------------------------------------------

    @Override
    public BufferPool createBufferPool(
            int numExpectedBuffers, int minUsedBuffers, int maxUsedBuffers) throws IOException {
        return internalCreateBufferPool(
                numExpectedBuffers, minUsedBuffers, maxUsedBuffers, 0, Integer.MAX_VALUE, 0);
    }

    @Override
    public BufferPool createBufferPool(
            int numExpectedBuffers,
            int minUsedBuffers,
            int maxUsedBuffers,
            int numSubpartitions,
            int maxBuffersPerChannel,
            int maxOverdraftBuffersPerGate)
            throws IOException {
        return internalCreateBufferPool(
                numExpectedBuffers,
                minUsedBuffers,
                maxUsedBuffers,
                numSubpartitions,
                maxBuffersPerChannel,
                maxOverdraftBuffersPerGate);
    }

    private BufferPool internalCreateBufferPool(
            int numExpectedBuffers,
            int minUsedBuffers,
            int maxUsedBuffers,
            int numSubpartitions,
            int maxBuffersPerChannel,
            int maxOverdraftBuffersPerGate)
            throws IOException {

        // It is necessary to use a separate lock from the one used for buffer
        // requests to ensure deadlock freedom for failure cases.
        synchronized (factoryLock) {
            if (isDestroyed) {
                throw new IllegalStateException("Network buffer pool has already been destroyed.");
            }

            // Ensure that the number of required buffers can be satisfied.
            // With dynamic memory management this should become obsolete.
            if (numTotalRequiredBuffers + minUsedBuffers > totalNumberOfMemorySegments) {
                throw new IOException(
                        String.format(
                                "Insufficient number of network buffers: "
                                        + "required %d, but only %d available. %s.",
                                minUsedBuffers,
                                totalNumberOfMemorySegments - numTotalRequiredBuffers,
                                getConfigDescription()));
            }

            this.numTotalRequiredBuffers += minUsedBuffers;

            // We are good to go, create a new buffer pool and redistribute
            // non-fixed size buffers.
            LocalBufferPool localBufferPool =
                    new LocalBufferPool(
                            this,
                            numExpectedBuffers,
                            minUsedBuffers,
                            maxUsedBuffers,
                            numSubpartitions,
                            maxBuffersPerChannel,
                            maxOverdraftBuffersPerGate);

            allBufferPools.add(localBufferPool);

            if (minUsedBuffers < maxUsedBuffers) {
                resizableBufferPools.add(localBufferPool);
            }

            redistributeBuffers();

            return localBufferPool;
        }
    }

    @Override
    public void destroyBufferPool(BufferPool bufferPool) {
        if (!(bufferPool instanceof LocalBufferPool)) {
            throw new IllegalArgumentException("bufferPool is no LocalBufferPool");
        }

        synchronized (factoryLock) {
            if (allBufferPools.remove(bufferPool)) {
                numTotalRequiredBuffers -= bufferPool.getMinNumberOfMemorySegments();
                resizableBufferPools.remove(bufferPool);

                redistributeBuffers();
            }
        }
    }

    /**
     * Destroys all buffer pools that allocate their buffers from this buffer pool (created via
     * {@link #createBufferPool(int, int, int)}).
     */
    public void destroyAllBufferPools() {
        synchronized (factoryLock) {
            // create a copy to avoid concurrent modification exceptions
            LocalBufferPool[] poolsCopy =
                    allBufferPools.toArray(new LocalBufferPool[allBufferPools.size()]);

            for (LocalBufferPool pool : poolsCopy) {
                pool.lazyDestroy();
            }

            // some sanity checks
            if (allBufferPools.size() > 0
                    || numTotalRequiredBuffers > 0
                    || resizableBufferPools.size() > 0) {
                throw new IllegalStateException(
                        "NetworkBufferPool is not empty after destroying all LocalBufferPools");
            }
        }
    }

    // Must be called from synchronized block
    private void tryRedistributeBuffers(int numberOfSegmentsToRequest) throws IOException {
        assert Thread.holdsLock(factoryLock);

        if (numTotalRequiredBuffers + numberOfSegmentsToRequest > totalNumberOfMemorySegments) {
            throw new IOException(
                    String.format(
                            "Insufficient number of network buffers: "
                                    + "required %d, but only %d available. %s.",
                            numberOfSegmentsToRequest,
                            totalNumberOfMemorySegments - numTotalRequiredBuffers,
                            getConfigDescription()));
        }

        this.numTotalRequiredBuffers += numberOfSegmentsToRequest;

        try {
            redistributeBuffers();
        } catch (Throwable t) {
            this.numTotalRequiredBuffers -= numberOfSegmentsToRequest;

            redistributeBuffers();
            ExceptionUtils.rethrow(t);
        }
    }

    // Must be called from synchronized block
    private void redistributeBuffers() {
        assert Thread.holdsLock(factoryLock);

        if (resizableBufferPools.isEmpty()) {
            return;
        }

        // All buffers, which are not among the required ones
        int numAvailableMemorySegment = totalNumberOfMemorySegments - numTotalRequiredBuffers;

        if (numAvailableMemorySegment == 0) {
            // in this case, we need to redistribute buffers so that every pool gets its minimum
            for (LocalBufferPool bufferPool : resizableBufferPools) {
                bufferPool.setNumBuffers(bufferPool.getMinNumberOfMemorySegments());
            }
            return;
        }

        // Calculates the number of buffers that can be redistributed and the total weight of buffer
        // pools that are resizable
        int totalWeight = 0;
        int numBuffersToBeRedistributed = numAvailableMemorySegment;
        for (LocalBufferPool bufferPool : resizableBufferPools) {
            numBuffersToBeRedistributed += bufferPool.getMinNumberOfMemorySegments();
            totalWeight += bufferPool.getExpectedNumberOfMemorySegments();
        }

        // First, all buffers are allocated proportionally according to the expected values of each
        // pool as weights. However, due to the constraints of minimum and maximum values, the
        // actual number of buffers distributed may be more or less than the total number of
        // buffers.
        int totalAllocated = 0;
        Map<LocalBufferPool, Integer> cachedPoolSize = new HashMap<>(resizableBufferPools.size());
        for (LocalBufferPool bufferPool : resizableBufferPools) {
            int expectedNumBuffers =
                    bufferPool.getExpectedNumberOfMemorySegments()
                            * numBuffersToBeRedistributed
                            / totalWeight;
            int actualAllocated =
                    Math.min(
                            bufferPool.getMaxNumberOfMemorySegments(),
                            Math.max(
                                    bufferPool.getMinNumberOfMemorySegments(), expectedNumBuffers));
            cachedPoolSize.put(bufferPool, actualAllocated);
            totalAllocated += actualAllocated;
        }

        // Now we need to deal with this difference, which may be greater than zero or less than
        // zero.
        int delta = numBuffersToBeRedistributed - totalAllocated;

        int remaining = Integer.MAX_VALUE;
        while (remaining != 0) {
            remaining = redistributeBuffers(delta, cachedPoolSize);

            // Stop the loop iteration when there is no remaining segments to be redistributed
            // or all local buffer pools have reached the max number.
            if (remaining == delta) {
                break;
            }
            delta = remaining;
        }

        for (LocalBufferPool bufferPool : resizableBufferPools) {
            bufferPool.setNumBuffers(
                    cachedPoolSize.getOrDefault(
                            bufferPool, bufferPool.getMinNumberOfMemorySegments()));
        }
    }

    /**
     * @param delta the buffers to be redistributed.
     * @param cachedPoolSize the map to cache the intermediate result.
     * @return the remaining buffers that can continue to be redistributed.
     */
    private int redistributeBuffers(int delta, Map<LocalBufferPool, Integer> cachedPoolSize) {
        Set<LocalBufferPool> poolsToBeRedistributed = new HashSet<>();

        if (delta > 0) {
            // In this case, we need to allocate the remaining buffers to pools that have
            // not yet reached their maximum number.

            int totalWeight = 0;
            for (LocalBufferPool bufferPool : resizableBufferPools) {
                if (cachedPoolSize.get(bufferPool) < bufferPool.getMaxNumberOfMemorySegments()) {
                    poolsToBeRedistributed.add(bufferPool);
                    totalWeight += bufferPool.getExpectedNumberOfMemorySegments();
                }
            }

            int totalAllocated = 0;
            float pieceOfDelta = ((float) delta) / totalWeight;
            for (LocalBufferPool bufferPool : poolsToBeRedistributed) {
                int extraAllocated =
                        Math.min(
                                delta - totalAllocated,
                                (int)
                                        Math.ceil(
                                                pieceOfDelta
                                                        * bufferPool
                                                                .getExpectedNumberOfMemorySegments()));
                int numBuffers =
                        Math.min(
                                bufferPool.getMaxNumberOfMemorySegments(),
                                cachedPoolSize.get(bufferPool) + extraAllocated);
                totalAllocated += numBuffers - cachedPoolSize.get(bufferPool);
                cachedPoolSize.put(bufferPool, numBuffers);
                if (totalAllocated == delta) {
                    break;
                }
            }
            return delta - totalAllocated;
        } else if (delta < 0) {
            // In this case, we need to take back the previously allocated buffers from pools that
            // have not yet reached their minimum number.

            int totalWeight = 0;
            for (LocalBufferPool bufferPool : resizableBufferPools) {
                if (cachedPoolSize.get(bufferPool) == bufferPool.getMinNumberOfMemorySegments()) {
                    continue;
                }
                poolsToBeRedistributed.add(bufferPool);
                totalWeight += bufferPool.getExpectedNumberOfMemorySegments();
            }

            int totalDeallocated = 0;
            int deltaAbs = Math.abs(delta);
            float pieceOfDelta = ((float) deltaAbs) / totalWeight;
            for (LocalBufferPool bufferPool : poolsToBeRedistributed) {
                int extraDeallocated =
                        Math.min(
                                deltaAbs - totalDeallocated,
                                (int)
                                        Math.ceil(
                                                pieceOfDelta
                                                        * bufferPool
                                                                .getExpectedNumberOfMemorySegments()));
                int numBuffers =
                        Math.max(
                                bufferPool.getMinNumberOfMemorySegments(),
                                cachedPoolSize.get(bufferPool) - extraDeallocated);
                totalDeallocated += cachedPoolSize.get(bufferPool) - numBuffers;
                cachedPoolSize.put(bufferPool, numBuffers);
                if (totalDeallocated == deltaAbs) {
                    break;
                }
            }
            return delta + totalDeallocated;
        } else {
            return 0;
        }
    }

    private String getConfigDescription() {
        return String.format(
                "The total number of network buffers is currently set to %d of %d bytes each. "
                        + "You can increase this number by setting the configuration keys '%s', '%s', and '%s'",
                totalNumberOfMemorySegments,
                memorySegmentSize,
                TaskManagerOptions.NETWORK_MEMORY_FRACTION.key(),
                TaskManagerOptions.NETWORK_MEMORY_MIN.key(),
                TaskManagerOptions.NETWORK_MEMORY_MAX.key());
    }
}
