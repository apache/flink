/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;

/** Utils for RocksDB Incremental Checkpoint. */
public class RocksDBIncrementalCheckpointUtils {
    /**
     * Evaluates state handle's "score" regarding to the target range when choosing the best state
     * handle to init the initial db for recovery, if the overlap fraction is less than
     * overlapFractionThreshold, then just return {@code Score.MIN} to mean the handle has no chance
     * to be the initial handle.
     */
    private static Score stateHandleEvaluator(
            KeyedStateHandle stateHandle,
            KeyGroupRange targetKeyGroupRange,
            double overlapFractionThreshold) {
        final KeyGroupRange handleKeyGroupRange = stateHandle.getKeyGroupRange();
        final KeyGroupRange intersectGroup =
                handleKeyGroupRange.getIntersection(targetKeyGroupRange);

        final double overlapFraction =
                (double) intersectGroup.getNumberOfKeyGroups()
                        / handleKeyGroupRange.getNumberOfKeyGroups();

        if (overlapFraction < overlapFractionThreshold) {
            return Score.MIN;
        }
        return new Score(intersectGroup.getNumberOfKeyGroups(), overlapFraction);
    }

    /**
     * Score of the state handle, intersect group range is compared first, and then compare the
     * overlap fraction.
     */
    private static class Score implements Comparable<Score> {

        public static final Score MIN = new Score(Integer.MIN_VALUE, -1.0);

        private final int intersectGroupRange;

        private final double overlapFraction;

        public Score(int intersectGroupRange, double overlapFraction) {
            this.intersectGroupRange = intersectGroupRange;
            this.overlapFraction = overlapFraction;
        }

        public int getIntersectGroupRange() {
            return intersectGroupRange;
        }

        public double getOverlapFraction() {
            return overlapFraction;
        }

        @Override
        public int compareTo(@Nonnull Score other) {
            return Comparator.comparing(Score::getIntersectGroupRange)
                    .thenComparing(Score::getOverlapFraction)
                    .compare(this, other);
        }
    }

    /**
     * The method to clip the db instance according to the target key group range using the {@link
     * RocksDB#delete(ColumnFamilyHandle, byte[])}.
     *
     * @param db the RocksDB instance to be clipped.
     * @param columnFamilyHandles the column families in the db instance.
     * @param targetKeyGroupRange the target key group range.
     * @param currentKeyGroupRange the key group range of the db instance.
     * @param keyGroupPrefixBytes Number of bytes required to prefix the key groups.
     */
    public static void clipDBWithKeyGroupRange(
            @Nonnull RocksDB db,
            @Nonnull List<ColumnFamilyHandle> columnFamilyHandles,
            @Nonnull KeyGroupRange targetKeyGroupRange,
            @Nonnull KeyGroupRange currentKeyGroupRange,
            @Nonnegative int keyGroupPrefixBytes)
            throws RocksDBException {

        final byte[] beginKeyGroupBytes = new byte[keyGroupPrefixBytes];
        final byte[] endKeyGroupBytes = new byte[keyGroupPrefixBytes];

        if (currentKeyGroupRange.getStartKeyGroup() < targetKeyGroupRange.getStartKeyGroup()) {
            CompositeKeySerializationUtils.serializeKeyGroup(
                    currentKeyGroupRange.getStartKeyGroup(), beginKeyGroupBytes);
            CompositeKeySerializationUtils.serializeKeyGroup(
                    targetKeyGroupRange.getStartKeyGroup(), endKeyGroupBytes);
            deleteRange(db, columnFamilyHandles, beginKeyGroupBytes, endKeyGroupBytes);
        }

        if (currentKeyGroupRange.getEndKeyGroup() > targetKeyGroupRange.getEndKeyGroup()) {
            CompositeKeySerializationUtils.serializeKeyGroup(
                    targetKeyGroupRange.getEndKeyGroup() + 1, beginKeyGroupBytes);
            CompositeKeySerializationUtils.serializeKeyGroup(
                    currentKeyGroupRange.getEndKeyGroup() + 1, endKeyGroupBytes);
            deleteRange(db, columnFamilyHandles, beginKeyGroupBytes, endKeyGroupBytes);
        }
    }

    /**
     * Delete the record falls into [beginKeyBytes, endKeyBytes) of the db.
     *
     * @param db the target need to be clipped.
     * @param columnFamilyHandles the column family need to be clipped.
     * @param beginKeyBytes the begin key bytes
     * @param endKeyBytes the end key bytes
     */
    private static void deleteRange(
            RocksDB db,
            List<ColumnFamilyHandle> columnFamilyHandles,
            byte[] beginKeyBytes,
            byte[] endKeyBytes)
            throws RocksDBException {

        for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
            // Using RocksDB's deleteRange will take advantage of delete
            // tombstones, which mark the range as deleted.
            //
            // https://github.com/ververica/frocksdb/blob/FRocksDB-6.20.3/include/rocksdb/db.h#L363-L377
            db.deleteRange(columnFamilyHandle, beginKeyBytes, endKeyBytes);
        }
    }

    /** check whether the bytes is before prefixBytes in the character order. */
    public static boolean beforeThePrefixBytes(@Nonnull byte[] bytes, @Nonnull byte[] prefixBytes) {
        final int prefixLength = prefixBytes.length;
        for (int i = 0; i < prefixLength; ++i) {
            int r = (char) prefixBytes[i] - (char) bytes[i];
            if (r != 0) {
                return r > 0;
            }
        }
        return false;
    }

    /**
     * Choose the best state handle according to the {@link #stateHandleEvaluator(KeyedStateHandle,
     * KeyGroupRange, double)} to init the initial db.
     *
     * @param restoreStateHandles The candidate state handles.
     * @param targetKeyGroupRange The target key group range.
     * @return The best candidate or null if no candidate was a good fit.
     */
    @Nullable
    public static <T extends KeyedStateHandle> T chooseTheBestStateHandleForInitial(
            @Nonnull Collection<T> restoreStateHandles,
            @Nonnull KeyGroupRange targetKeyGroupRange,
            double overlapFractionThreshold) {

        T bestStateHandle = null;
        Score bestScore = Score.MIN;
        for (T rawStateHandle : restoreStateHandles) {
            Score handleScore =
                    stateHandleEvaluator(
                            rawStateHandle, targetKeyGroupRange, overlapFractionThreshold);
            if (bestStateHandle == null || handleScore.compareTo(bestScore) > 0) {
                bestStateHandle = rawStateHandle;
                bestScore = handleScore;
            }
        }

        return bestStateHandle;
    }
}
