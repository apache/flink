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
    BLOCKING(false, false, false, true, ConsumingConstraint.BLOCKING, ReleaseBy.SCHEDULER),

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
    BLOCKING_PERSISTENT(
            false, false, true, true, ConsumingConstraint.BLOCKING, ReleaseBy.SCHEDULER),

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
    PIPELINED(true, false, false, false, ConsumingConstraint.MUST_BE_PIPELINED, ReleaseBy.UPSTREAM),

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
            true, true, false, false, ConsumingConstraint.MUST_BE_PIPELINED, ReleaseBy.UPSTREAM),

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
            true, true, false, true, ConsumingConstraint.CAN_BE_PIPELINED, ReleaseBy.UPSTREAM);

    /** Can the partition be consumed while being produced? */
    private final boolean isPipelined;

    /** Does this partition use a limited number of (network) buffers? */
    private final boolean isBounded;

    /** This partition will not be released after consuming if 'isPersistent' is true. */
    private final boolean isPersistent;

    /**
     * Can the partition be reconnected.
     *
     * <p>Attention: this attribute is introduced temporally for
     * ResultPartitionType.PIPELINED_APPROXIMATE It will be removed afterwards: TODO: 1. Approximate
     * local recovery has its won failover strategy to restart the failed set of tasks instead of
     * restarting downstream of failed tasks depending on {@code
     * RestartPipelinedRegionFailoverStrategy} 2. FLINK-19895: Unify the life cycle of
     * ResultPartitionType Pipelined Family
     */
    private final boolean isReconnectable;

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
            boolean isPipelined,
            boolean isBounded,
            boolean isPersistent,
            boolean isReconnectable,
            ConsumingConstraint consumingConstraint,
            ReleaseBy releaseBy) {
        this.isPipelined = isPipelined;
        this.isBounded = isBounded;
        this.isPersistent = isPersistent;
        this.isReconnectable = isReconnectable;
        this.consumingConstraint = consumingConstraint;
        this.releaseBy = releaseBy;
    }

    public boolean isBlocking() {
        return !isPipelined;
    }

    public boolean isPipelined() {
        return isPipelined;
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

    public boolean isReconnectable() {
        return isReconnectable;
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
}
