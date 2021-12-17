/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.time.Duration;

/**
 * The default implementation of the {@link RollingPolicy}.
 *
 * <p>This policy rolls a part file if:
 *
 * <ol>
 *   <li>there is no open part file,
 *   <li>the current file has reached the maximum bucket size (by default 128MB),
 *   <li>the current file is older than the roll over interval (by default 60 sec), or
 *   <li>the current file has not been written to for more than the allowed inactivityTime (by
 *       default 60 sec).
 * </ol>
 */
@PublicEvolving
public final class DefaultRollingPolicy<IN, BucketID> implements RollingPolicy<IN, BucketID> {

    private static final long serialVersionUID = 1L;

    private static final long DEFAULT_INACTIVITY_INTERVAL = 60L * 1000L;

    private static final long DEFAULT_ROLLOVER_INTERVAL = 60L * 1000L;

    private static final long DEFAULT_MAX_PART_SIZE = 1024L * 1024L * 128L;

    private final long partSize;

    private final long rolloverInterval;

    private final long inactivityInterval;

    /** Private constructor to avoid direct instantiation. */
    private DefaultRollingPolicy(long partSize, long rolloverInterval, long inactivityInterval) {
        Preconditions.checkArgument(partSize > 0L);
        Preconditions.checkArgument(rolloverInterval > 0L);
        Preconditions.checkArgument(inactivityInterval > 0L);

        this.partSize = partSize;
        this.rolloverInterval = rolloverInterval;
        this.inactivityInterval = inactivityInterval;
    }

    @Override
    public boolean shouldRollOnCheckpoint(PartFileInfo<BucketID> partFileState) throws IOException {
        return partFileState.getSize() > partSize;
    }

    @Override
    public boolean shouldRollOnEvent(PartFileInfo<BucketID> partFileState, IN element)
            throws IOException {
        return partFileState.getSize() > partSize;
    }

    @Override
    public boolean shouldRollOnProcessingTime(
            final PartFileInfo<BucketID> partFileState, final long currentTime) {
        return currentTime - partFileState.getCreationTime() >= rolloverInterval
                || currentTime - partFileState.getLastUpdateTime() >= inactivityInterval;
    }

    /**
     * Returns the maximum part file size before rolling.
     *
     * @return Max size in bytes
     */
    public long getMaxPartSize() {
        return partSize;
    }

    /**
     * Returns the maximum time duration a part file can stay open before rolling.
     *
     * @return Time duration in milliseconds
     */
    public long getRolloverInterval() {
        return rolloverInterval;
    }

    /**
     * Returns time duration of allowed inactivity after which a part file will have to roll.
     *
     * @return Time duration in milliseconds
     */
    public long getInactivityInterval() {
        return inactivityInterval;
    }

    /**
     * Creates a new {@link PolicyBuilder} that is used to configure and build an instance of {@code
     * DefaultRollingPolicy}.
     */
    public static DefaultRollingPolicy.PolicyBuilder builder() {
        return new DefaultRollingPolicy.PolicyBuilder(
                DEFAULT_MAX_PART_SIZE, DEFAULT_ROLLOVER_INTERVAL, DEFAULT_INACTIVITY_INTERVAL);
    }

    /** This method is {@link Deprecated}, use {@link DefaultRollingPolicy#builder()} instead. */
    @Deprecated
    public static DefaultRollingPolicy.PolicyBuilder create() {
        return builder();
    }

    /**
     * A helper class that holds the configuration properties for the {@link DefaultRollingPolicy}.
     * The {@link PolicyBuilder#build()} method must be called to instantiate the policy.
     */
    @PublicEvolving
    public static final class PolicyBuilder {

        private final long partSize;

        private final long rolloverInterval;

        private final long inactivityInterval;

        private PolicyBuilder(
                final long partSize, final long rolloverInterval, final long inactivityInterval) {
            this.partSize = partSize;
            this.rolloverInterval = rolloverInterval;
            this.inactivityInterval = inactivityInterval;
        }

        /**
         * Sets the part size above which a part file will have to roll.
         *
         * @param size the allowed part size.
         */
        public DefaultRollingPolicy.PolicyBuilder withMaxPartSize(final MemorySize size) {
            Preconditions.checkNotNull(size, "Rolling policy memory size cannot be null");
            return new PolicyBuilder(size.getBytes(), rolloverInterval, inactivityInterval);
        }

        /**
         * Sets the part size above which a part file will have to roll.
         *
         * @param size the allowed part size.
         * @deprecated Use {@link #withMaxPartSize(MemorySize)} instead.
         */
        @Deprecated
        public DefaultRollingPolicy.PolicyBuilder withMaxPartSize(final long size) {
            Preconditions.checkState(size > 0L);
            return new PolicyBuilder(size, rolloverInterval, inactivityInterval);
        }

        /**
         * Sets the interval of allowed inactivity after which a part file will have to roll. The
         * frequency at which this is checked is controlled by the {@link
         * org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink.RowFormatBuilder#withBucketCheckInterval(long)}
         * setting.
         *
         * @param interval the allowed inactivity interval.
         * @deprecated Use {@link #withInactivityInterval(Duration)} instead.
         */
        @Deprecated
        public DefaultRollingPolicy.PolicyBuilder withInactivityInterval(final long interval) {
            Preconditions.checkState(interval > 0L);
            return new PolicyBuilder(partSize, rolloverInterval, interval);
        }

        /**
         * Sets the interval of allowed inactivity after which a part file will have to roll. The
         * frequency at which this is checked is controlled by the {@link
         * org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink.RowFormatBuilder#withBucketCheckInterval(long)}
         * setting.
         *
         * @param interval the allowed inactivity interval.
         */
        public DefaultRollingPolicy.PolicyBuilder withInactivityInterval(final Duration interval) {
            Preconditions.checkNotNull(
                    interval, "Rolling policy inactivity interval cannot be null");
            return new PolicyBuilder(partSize, rolloverInterval, interval.toMillis());
        }

        /**
         * Sets the max time a part file can stay open before having to roll. The frequency at which
         * this is checked is controlled by the {@link
         * org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink.RowFormatBuilder#withBucketCheckInterval(long)}
         * setting.
         *
         * @param interval the desired rollover interval.
         * @deprecated Use {@link #withRolloverInterval(Duration)} instead.
         */
        @Deprecated
        public DefaultRollingPolicy.PolicyBuilder withRolloverInterval(final long interval) {
            Preconditions.checkState(interval > 0L);
            return new PolicyBuilder(partSize, interval, inactivityInterval);
        }

        /**
         * Sets the max time a part file can stay open before having to roll. The frequency at which
         * this is checked is controlled by the {@link
         * org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink.RowFormatBuilder#withBucketCheckInterval(long)}
         * setting.
         *
         * @param interval the desired rollover interval.
         */
        public DefaultRollingPolicy.PolicyBuilder withRolloverInterval(final Duration interval) {
            Preconditions.checkNotNull(interval, "Rolling policy rollover interval cannot be null");
            return new PolicyBuilder(partSize, interval.toMillis(), inactivityInterval);
        }

        /** Creates the actual policy. */
        public <IN, BucketID> DefaultRollingPolicy<IN, BucketID> build() {
            return new DefaultRollingPolicy<>(partSize, rolloverInterval, inactivityInterval);
        }
    }
}
