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

package org.apache.flink.api.common.state;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.EnumMap;

import static org.apache.flink.api.common.state.StateTtlConfig.CleanupStrategies.EMPTY_STRATEGY;
import static org.apache.flink.api.common.state.StateTtlConfig.IncrementalCleanupStrategy.DEFAULT_INCREMENTAL_CLEANUP_STRATEGY;
import static org.apache.flink.api.common.state.StateTtlConfig.RocksdbCompactFilterCleanupStrategy.DEFAULT_ROCKSDB_COMPACT_FILTER_CLEANUP_STRATEGY;
import static org.apache.flink.api.common.state.StateTtlConfig.StateVisibility.NeverReturnExpired;
import static org.apache.flink.api.common.state.StateTtlConfig.TtlTimeCharacteristic.ProcessingTime;
import static org.apache.flink.api.common.state.StateTtlConfig.UpdateType.OnCreateAndWrite;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Configuration of state TTL logic.
 *
 * <p>Note: The map state with TTL currently supports {@code null} user values only if the user
 * value serializer can handle {@code null} values. If the serializer does not support {@code null}
 * values, it can be wrapped with {@link
 * org.apache.flink.api.java.typeutils.runtime.NullableSerializer} at the cost of an extra byte in
 * the serialized form.
 */
@PublicEvolving
public class StateTtlConfig implements Serializable {

    private static final long serialVersionUID = -7592693245044289793L;

    public static final StateTtlConfig DISABLED =
            newBuilder(Time.milliseconds(Long.MAX_VALUE))
                    .setUpdateType(UpdateType.Disabled)
                    .build();

    /**
     * This option value configures when to update last access timestamp which prolongs state TTL.
     */
    public enum UpdateType {
        /** TTL is disabled. State does not expire. */
        Disabled,
        /**
         * Last access timestamp is initialised when state is created and updated on every write
         * operation.
         */
        OnCreateAndWrite,
        /** The same as <code>OnCreateAndWrite</code> but also updated on read. */
        OnReadAndWrite
    }

    /** This option configures whether expired user value can be returned or not. */
    public enum StateVisibility {
        /** Return expired user value if it is not cleaned up yet. */
        ReturnExpiredIfNotCleanedUp,
        /** Never return expired user value. */
        NeverReturnExpired
    }

    /** This option configures time scale to use for ttl. */
    public enum TtlTimeCharacteristic {
        /**
         * Processing time, see also <code>
         * org.apache.flink.streaming.api.TimeCharacteristic.ProcessingTime</code>.
         */
        ProcessingTime
    }

    private final UpdateType updateType;
    private final StateVisibility stateVisibility;
    private final TtlTimeCharacteristic ttlTimeCharacteristic;
    private final Time ttl;
    private final CleanupStrategies cleanupStrategies;

    private StateTtlConfig(
            UpdateType updateType,
            StateVisibility stateVisibility,
            TtlTimeCharacteristic ttlTimeCharacteristic,
            Time ttl,
            CleanupStrategies cleanupStrategies) {
        this.updateType = checkNotNull(updateType);
        this.stateVisibility = checkNotNull(stateVisibility);
        this.ttlTimeCharacteristic = checkNotNull(ttlTimeCharacteristic);
        this.ttl = checkNotNull(ttl);
        this.cleanupStrategies = cleanupStrategies;
        checkArgument(ttl.toMilliseconds() > 0, "TTL is expected to be positive.");
    }

    @Nonnull
    public UpdateType getUpdateType() {
        return updateType;
    }

    @Nonnull
    public StateVisibility getStateVisibility() {
        return stateVisibility;
    }

    @Nonnull
    public Time getTtl() {
        return ttl;
    }

    @Nonnull
    public TtlTimeCharacteristic getTtlTimeCharacteristic() {
        return ttlTimeCharacteristic;
    }

    public boolean isEnabled() {
        return updateType != UpdateType.Disabled;
    }

    @Nonnull
    public CleanupStrategies getCleanupStrategies() {
        return cleanupStrategies;
    }

    @Override
    public String toString() {
        return "StateTtlConfig{"
                + "updateType="
                + updateType
                + ", stateVisibility="
                + stateVisibility
                + ", ttlTimeCharacteristic="
                + ttlTimeCharacteristic
                + ", ttl="
                + ttl
                + '}';
    }

    @Nonnull
    public static Builder newBuilder(@Nonnull Time ttl) {
        return new Builder(ttl);
    }

    /** Builder for the {@link StateTtlConfig}. */
    public static class Builder {

        private UpdateType updateType = OnCreateAndWrite;
        private StateVisibility stateVisibility = NeverReturnExpired;
        private TtlTimeCharacteristic ttlTimeCharacteristic = ProcessingTime;
        private Time ttl;
        private boolean isCleanupInBackground = true;
        private final EnumMap<CleanupStrategies.Strategies, CleanupStrategies.CleanupStrategy>
                strategies = new EnumMap<>(CleanupStrategies.Strategies.class);

        public Builder(@Nonnull Time ttl) {
            this.ttl = ttl;
        }

        /**
         * Sets the ttl update type.
         *
         * @param updateType The ttl update type configures when to update last access timestamp
         *     which prolongs state TTL.
         */
        @Nonnull
        public Builder setUpdateType(UpdateType updateType) {
            this.updateType = updateType;
            return this;
        }

        @Nonnull
        public Builder updateTtlOnCreateAndWrite() {
            return setUpdateType(UpdateType.OnCreateAndWrite);
        }

        @Nonnull
        public Builder updateTtlOnReadAndWrite() {
            return setUpdateType(UpdateType.OnReadAndWrite);
        }

        /**
         * Sets the state visibility.
         *
         * @param stateVisibility The state visibility configures whether expired user value can be
         *     returned or not.
         */
        @Nonnull
        public Builder setStateVisibility(@Nonnull StateVisibility stateVisibility) {
            this.stateVisibility = stateVisibility;
            return this;
        }

        @Nonnull
        public Builder returnExpiredIfNotCleanedUp() {
            return setStateVisibility(StateVisibility.ReturnExpiredIfNotCleanedUp);
        }

        @Nonnull
        public Builder neverReturnExpired() {
            return setStateVisibility(StateVisibility.NeverReturnExpired);
        }

        /**
         * Sets the time characteristic.
         *
         * @param ttlTimeCharacteristic The time characteristic configures time scale to use for
         *     ttl.
         */
        @Nonnull
        public Builder setTtlTimeCharacteristic(
                @Nonnull TtlTimeCharacteristic ttlTimeCharacteristic) {
            this.ttlTimeCharacteristic = ttlTimeCharacteristic;
            return this;
        }

        @Nonnull
        public Builder useProcessingTime() {
            return setTtlTimeCharacteristic(ProcessingTime);
        }

        /** Cleanup expired state in full snapshot on checkpoint. */
        @Nonnull
        public Builder cleanupFullSnapshot() {
            strategies.put(CleanupStrategies.Strategies.FULL_STATE_SCAN_SNAPSHOT, EMPTY_STRATEGY);
            return this;
        }

        /**
         * Cleanup expired state incrementally cleanup local state.
         *
         * <p>Upon every state access this cleanup strategy checks a bunch of state keys for
         * expiration and cleans up expired ones. It keeps a lazy iterator through all keys with
         * relaxed consistency if backend supports it. This way all keys should be regularly checked
         * and cleaned eventually over time if any state is constantly being accessed.
         *
         * <p>Additionally to the incremental cleanup upon state access, it can also run per every
         * record. Caution: if there are a lot of registered states using this option, they all will
         * be iterated for every record to check if there is something to cleanup.
         *
         * <p>Note: if no access happens to this state or no records are processed in case of {@code
         * runCleanupForEveryRecord}, expired state will persist.
         *
         * <p>Note: Time spent for the incremental cleanup increases record processing latency.
         *
         * <p>Note: At the moment incremental cleanup is implemented only for Heap state backend.
         * Setting it for RocksDB will have no effect.
         *
         * <p>Note: If heap state backend is used with synchronous snapshotting, the global iterator
         * keeps a copy of all keys while iterating because of its specific implementation which
         * does not support concurrent modifications. Enabling of this feature will increase memory
         * consumption then. Asynchronous snapshotting does not have this problem.
         *
         * @param cleanupSize max number of keys pulled from queue for clean up upon state touch for
         *     any key
         * @param runCleanupForEveryRecord run incremental cleanup per each processed record
         */
        @Nonnull
        public Builder cleanupIncrementally(
                @Nonnegative int cleanupSize, boolean runCleanupForEveryRecord) {
            strategies.put(
                    CleanupStrategies.Strategies.INCREMENTAL_CLEANUP,
                    new IncrementalCleanupStrategy(cleanupSize, runCleanupForEveryRecord));
            return this;
        }

        /**
         * Cleanup expired state while Rocksdb compaction is running.
         *
         * <p>RocksDB compaction filter will query current timestamp, used to check expiration, from
         * Flink every time after processing {@code queryTimeAfterNumEntries} number of state
         * entries. Updating the timestamp more often can improve cleanup speed but it decreases
         * compaction performance because it uses JNI call from native code.
         *
         * @param queryTimeAfterNumEntries number of state entries to process by compaction filter
         *     before updating current timestamp
         */
        @Nonnull
        public Builder cleanupInRocksdbCompactFilter(long queryTimeAfterNumEntries) {
            strategies.put(
                    CleanupStrategies.Strategies.ROCKSDB_COMPACTION_FILTER,
                    new RocksdbCompactFilterCleanupStrategy(queryTimeAfterNumEntries));
            return this;
        }

        /**
         * Disable default cleanup of expired state in background (enabled by default).
         *
         * <p>If some specific cleanup is configured, e.g. {@link #cleanupIncrementally(int,
         * boolean)} or {@link #cleanupInRocksdbCompactFilter(long)}, this setting does not disable
         * it.
         */
        @Nonnull
        public Builder disableCleanupInBackground() {
            isCleanupInBackground = false;
            return this;
        }

        /**
         * Sets the ttl time.
         *
         * @param ttl The ttl time.
         */
        @Nonnull
        public Builder setTtl(@Nonnull Time ttl) {
            this.ttl = ttl;
            return this;
        }

        @Nonnull
        public StateTtlConfig build() {
            return new StateTtlConfig(
                    updateType,
                    stateVisibility,
                    ttlTimeCharacteristic,
                    ttl,
                    new CleanupStrategies(strategies, isCleanupInBackground));
        }
    }

    /**
     * TTL cleanup strategies.
     *
     * <p>This class configures when to cleanup expired state with TTL. By default, state is always
     * cleaned up on explicit read access if found expired. Currently cleanup of state full snapshot
     * can be additionally activated.
     */
    public static class CleanupStrategies implements Serializable {
        private static final long serialVersionUID = -1617740467277313524L;

        static final CleanupStrategy EMPTY_STRATEGY = new EmptyCleanupStrategy();

        private final boolean isCleanupInBackground;

        private final EnumMap<Strategies, CleanupStrategy> strategies;

        /** Fixed strategies ordinals in {@code strategies} config field. */
        enum Strategies {
            FULL_STATE_SCAN_SNAPSHOT,
            INCREMENTAL_CLEANUP,
            ROCKSDB_COMPACTION_FILTER
        }

        /** Base interface for cleanup strategies configurations. */
        interface CleanupStrategy extends Serializable {}

        static class EmptyCleanupStrategy implements CleanupStrategy {
            private static final long serialVersionUID = 1373998465131443873L;
        }

        private CleanupStrategies(
                EnumMap<Strategies, CleanupStrategy> strategies, boolean isCleanupInBackground) {
            this.strategies = strategies;
            this.isCleanupInBackground = isCleanupInBackground;
        }

        public boolean inFullSnapshot() {
            return strategies.containsKey(Strategies.FULL_STATE_SCAN_SNAPSHOT);
        }

        public boolean isCleanupInBackground() {
            return isCleanupInBackground;
        }

        @Nullable
        public IncrementalCleanupStrategy getIncrementalCleanupStrategy() {
            IncrementalCleanupStrategy defaultStrategy =
                    isCleanupInBackground ? DEFAULT_INCREMENTAL_CLEANUP_STRATEGY : null;
            return (IncrementalCleanupStrategy)
                    strategies.getOrDefault(Strategies.INCREMENTAL_CLEANUP, defaultStrategy);
        }

        public boolean inRocksdbCompactFilter() {
            return getRocksdbCompactFilterCleanupStrategy() != null;
        }

        @Nullable
        public RocksdbCompactFilterCleanupStrategy getRocksdbCompactFilterCleanupStrategy() {
            RocksdbCompactFilterCleanupStrategy defaultStrategy =
                    isCleanupInBackground ? DEFAULT_ROCKSDB_COMPACT_FILTER_CLEANUP_STRATEGY : null;
            return (RocksdbCompactFilterCleanupStrategy)
                    strategies.getOrDefault(Strategies.ROCKSDB_COMPACTION_FILTER, defaultStrategy);
        }
    }

    /** Configuration of cleanup strategy while taking the full snapshot. */
    public static class IncrementalCleanupStrategy implements CleanupStrategies.CleanupStrategy {
        private static final long serialVersionUID = 3109278696501988780L;

        static final IncrementalCleanupStrategy DEFAULT_INCREMENTAL_CLEANUP_STRATEGY =
                new IncrementalCleanupStrategy(5, false);

        /** Max number of keys pulled from queue for clean up upon state touch for any key. */
        private final int cleanupSize;

        /** Whether to run incremental cleanup per each processed record. */
        private final boolean runCleanupForEveryRecord;

        private IncrementalCleanupStrategy(int cleanupSize, boolean runCleanupForEveryRecord) {
            Preconditions.checkArgument(
                    cleanupSize > 0,
                    "Number of incrementally cleaned up state entries should be positive.");
            this.cleanupSize = cleanupSize;
            this.runCleanupForEveryRecord = runCleanupForEveryRecord;
        }

        public int getCleanupSize() {
            return cleanupSize;
        }

        public boolean runCleanupForEveryRecord() {
            return runCleanupForEveryRecord;
        }
    }

    /** Configuration of cleanup strategy using custom compaction filter in RocksDB. */
    public static class RocksdbCompactFilterCleanupStrategy
            implements CleanupStrategies.CleanupStrategy {
        private static final long serialVersionUID = 3109278796506988980L;

        static final RocksdbCompactFilterCleanupStrategy
                DEFAULT_ROCKSDB_COMPACT_FILTER_CLEANUP_STRATEGY =
                        new RocksdbCompactFilterCleanupStrategy(1000L);

        /**
         * Number of state entries to process by compaction filter before updating current
         * timestamp.
         */
        private final long queryTimeAfterNumEntries;

        private RocksdbCompactFilterCleanupStrategy(long queryTimeAfterNumEntries) {
            this.queryTimeAfterNumEntries = queryTimeAfterNumEntries;
        }

        public long getQueryTimeAfterNumEntries() {
            return queryTimeAfterNumEntries;
        }
    }
}
