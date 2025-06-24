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

package org.apache.flink.runtime.io.network.partition;

/** Type of a result partition. */
public enum ResultPartitionType {

    /**
     * Blocking partitions represent blocking data exchanges, where the data stream is first fully
     * produced and then consumed. This is an option that is only applicable to bounded streams and
     * can be used in bounded stream runtime and recovery.
     *
     * <p>Blocking partitions can be consumed multiple times and concurrently.
     *
     * <p>The partition is not automatically released after being consumed (like for example the
     * {@link #PIPELINED} partitions), but only released through the scheduler, when it determines
     * that the partition is no longer needed.
     */
    BLOCKING(true, false, false, ConsumingConstraint.BLOCKING, ReleaseBy.SCHEDULER),

    /**
     * BLOCKING_PERSISTENT partitions are similar to {@link #BLOCKING} partitions, but have a
     * user-specified life cycle.
     *
     * <p>BLOCKING_PERSISTENT partitions are dropped upon explicit API calls to the JobManager or
     * ResourceManager, rather than by the scheduler.
     *
     * <p>Otherwise, the partition may only be dropped by safety-nets during failure handling
     * scenarios, like when the TaskManager exits or when the TaskManager loses connection to
     * JobManager / ResourceManager for too long.
     */
    BLOCKING_PERSISTENT(true, false, true, ConsumingConstraint.BLOCKING, ReleaseBy.SCHEDULER),

    /**
     * A pipelined streaming data exchange. This is applicable to both bounded and unbounded
     * streams.
     *
     * <p>Pipelined results can be consumed only once by a single consumer and are automatically
     * disposed when the stream has been consumed.
     *
     * <p>This result partition type may keep an arbitrary amount of data in-flight, in contrast to
     * the {@link #PIPELINED_BOUNDED} variant.
     */
    PIPELINED(false, false, false, ConsumingConstraint.MUST_BE_PIPELINED, ReleaseBy.UPSTREAM),

    /**
     * Pipelined partitions with a bounded (local) buffer pool.
     *
     * <p>For streaming jobs, a fixed limit on the buffer pool size should help avoid that too much
     * data is being buffered and checkpoint barriers are delayed. In contrast to limiting the
     * overall network buffer pool size, this, however, still allows to be flexible with regards to
     * the total number of partitions by selecting an appropriately big network buffer pool size.
     *
     * <p>For batch jobs, it will be best to keep this unlimited ({@link #PIPELINED}) since there
     * are no checkpoint barriers.
     */
    PIPELINED_BOUNDED(
            false, true, false, ConsumingConstraint.MUST_BE_PIPELINED, ReleaseBy.UPSTREAM),

    /**
     * Pipelined partitions with a bounded (local) buffer pool to support downstream task to
     * continue consuming data after reconnection in Approximate Local-Recovery.
     *
     * <p>Pipelined results can be consumed only once by a single consumer at one time. {@link
     * #PIPELINED_APPROXIMATE} is different from {@link #PIPELINED} and {@link #PIPELINED_BOUNDED}
     * in that {@link #PIPELINED_APPROXIMATE} partition can be reconnected after down stream task
     * fails.
     */
    PIPELINED_APPROXIMATE(
            false, true, false, ConsumingConstraint.CAN_BE_PIPELINED, ReleaseBy.UPSTREAM),

    /**
     * Hybrid partitions with a bounded (local) buffer pool to support downstream task to
     * simultaneous reading and writing shuffle data.
     *
     * <p>Hybrid partitions can be consumed any time, whether fully produced or not.
     *
     * <p>HYBRID_FULL partitions is re-consumable, so double calculation can be avoided during
     * failover.
     */
    HYBRID_FULL(true, false, false, ConsumingConstraint.CAN_BE_PIPELINED, ReleaseBy.SCHEDULER),

    /**
     * HYBRID_SELECTIVE partitions are similar to {@link #HYBRID_FULL} partitions, but it is not
     * re-consumable.
     */
    HYBRID_SELECTIVE(
            false, false, false, ConsumingConstraint.CAN_BE_PIPELINED, ReleaseBy.SCHEDULER);

    /**
     * Can this result partition be consumed by multiple downstream consumers for multiple times.
     */
    private final boolean isReconsumable;

    /** Does this partition use a limited number of (network) buffers? */
    private final boolean isBounded;

    /** This partition will not be released after consuming if 'isPersistent' is true. */
    private final boolean isPersistent;

    private final ConsumingConstraint consumingConstraint;

    private final ReleaseBy releaseBy;

    /** ConsumingConstraint indicates when can the downstream consume the upstream. */
    private enum ConsumingConstraint {
        /** Upstream must be finished before downstream consume. */
        BLOCKING,
        /** Downstream can consume while upstream is running. */
        CAN_BE_PIPELINED,
        /** Downstream must consume while upstream is running. */
        MUST_BE_PIPELINED
    }

    /**
     * ReleaseBy indicates who is responsible for releasing the result partition.
     *
     * <p>Attention: This may only be a short-term solution to deal with the partition release
     * logic. We can discuss the issue of unifying the partition release logic in FLINK-27948. Once
     * the ticket is resolved, we can remove the enumeration here.
     */
    private enum ReleaseBy {
        UPSTREAM,
        SCHEDULER
    }

    /** Specifies the behaviour of an intermediate result partition at runtime. */
    ResultPartitionType(
            boolean isReconsumable,
            boolean isBounded,
            boolean isPersistent,
            ConsumingConstraint consumingConstraint,
            ReleaseBy releaseBy) {
        this.isReconsumable = isReconsumable;
        this.isBounded = isBounded;
        this.isPersistent = isPersistent;
        this.consumingConstraint = consumingConstraint;
        this.releaseBy = releaseBy;
    }

    /** return if this partition's upstream and downstream must be scheduled in the same time. */
    public boolean mustBePipelinedConsumed() {
        return consumingConstraint == ConsumingConstraint.MUST_BE_PIPELINED;
    }

    /** return if this partition's upstream and downstream support scheduling in the same time. */
    public boolean canBePipelinedConsumed() {
        return consumingConstraint == ConsumingConstraint.CAN_BE_PIPELINED
                || consumingConstraint == ConsumingConstraint.MUST_BE_PIPELINED;
    }

    public boolean isReleaseByScheduler() {
        return releaseBy == ReleaseBy.SCHEDULER;
    }

    public boolean isReleaseByUpstream() {
        return releaseBy == ReleaseBy.UPSTREAM;
    }

    /**
     * {@link #isBlockingOrBlockingPersistentResultPartition()} is used to judge whether it is the
     * specified {@link #BLOCKING} or {@link #BLOCKING_PERSISTENT} resultPartitionType.
     *
     * <p>this method suitable for judgment conditions related to the specific implementation of
     * {@link ResultPartitionType}.
     *
     * <p>this method not related to data consumption and partition release. As for the logic
     * related to partition release, use {@link #isReleaseByScheduler()} instead, and as consume
     * type, use {@link #mustBePipelinedConsumed()} or {@link #canBePipelinedConsumed()} instead.
     */
    public boolean isBlockingOrBlockingPersistentResultPartition() {
        return this == BLOCKING || this == BLOCKING_PERSISTENT;
    }

    /**
     * {@link #isHybridResultPartition()} is used to judge whether it is the specified {@link
     * #HYBRID_FULL} or {@link #HYBRID_SELECTIVE} resultPartitionType.
     *
     * <p>this method suitable for judgment conditions related to the specific implementation of
     * {@link ResultPartitionType}.
     *
     * <p>this method not related to data consumption and partition release. As for the logic
     * related to partition release, use {@link #isReleaseByScheduler()} instead, and as consume
     * type, use {@link #mustBePipelinedConsumed()} or {@link #canBePipelinedConsumed()} instead.
     */
    public boolean isHybridResultPartition() {
        return this == HYBRID_FULL || this == HYBRID_SELECTIVE;
    }

    /**
     * {@link #isPipelinedOrPipelinedBoundedResultPartition()} is used to judge whether it is the
     * specified {@link #PIPELINED} or {@link #PIPELINED_BOUNDED} resultPartitionType.
     *
     * <p>This method suitable for judgment conditions related to the specific implementation of
     * {@link ResultPartitionType}.
     *
     * <p>This method not related to data consumption and partition release. As for the logic
     * related to partition release, use {@link #isReleaseByScheduler()} instead, and as consume
     * type, use {@link #mustBePipelinedConsumed()} or {@link #canBePipelinedConsumed()} instead.
     */
    public boolean isPipelinedOrPipelinedBoundedResultPartition() {
        return this == PIPELINED || this == PIPELINED_BOUNDED;
    }

    /**
     * Whether this partition uses a limited number of (network) buffers or not.
     *
     * @return <tt>true</tt> if the number of buffers should be bound to some limit
     */
    public boolean isBounded() {
        return isBounded;
    }

    public boolean isPersistent() {
        return isPersistent;
    }

    public boolean supportCompression() {
        return isBlockingOrBlockingPersistentResultPartition()
                || this == HYBRID_FULL
                || this == HYBRID_SELECTIVE;
    }

    public boolean isReconsumable() {
        return isReconsumable;
    }
}
