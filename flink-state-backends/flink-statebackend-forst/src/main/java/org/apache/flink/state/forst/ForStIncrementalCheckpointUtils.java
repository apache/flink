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

package org.apache.flink.state.forst;

import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava33.com.google.common.primitives.UnsignedBytes;

import org.forstdb.ColumnFamilyHandle;
import org.forstdb.LiveFileMetaData;
import org.forstdb.RocksDB;
import org.forstdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/** Utils for RocksDB Incremental Checkpoint. */
public class ForStIncrementalCheckpointUtils {

    private static final Logger logger =
            LoggerFactory.getLogger(ForStIncrementalCheckpointUtils.class);

    /**
     * Evaluates state handle's "score" regarding the target range when choosing the best state
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
        public int compareTo(@Nullable Score other) {
            return Comparator.nullsFirst(
                            Comparator.comparing(Score::getIntersectGroupRange)
                                    .thenComparing(Score::getIntersectGroupRange)
                                    .thenComparing(Score::getOverlapFraction))
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
     * @param useDeleteFilesInRange whether to call db.deleteFilesInRanges for the deleted ranges.
     */
    public static void clipDBWithKeyGroupRange(
            @Nonnull RocksDB db,
            @Nonnull List<ColumnFamilyHandle> columnFamilyHandles,
            @Nonnull KeyGroupRange targetKeyGroupRange,
            @Nonnull KeyGroupRange currentKeyGroupRange,
            @Nonnegative int keyGroupPrefixBytes,
            boolean useDeleteFilesInRange)
            throws RocksDBException {

        List<byte[]> deleteFilesRanges = new ArrayList<>(4);

        if (currentKeyGroupRange.getStartKeyGroup() < targetKeyGroupRange.getStartKeyGroup()) {
            prepareRangeDeletes(
                    keyGroupPrefixBytes,
                    currentKeyGroupRange.getStartKeyGroup(),
                    targetKeyGroupRange.getStartKeyGroup(),
                    deleteFilesRanges);
        }

        if (currentKeyGroupRange.getEndKeyGroup() > targetKeyGroupRange.getEndKeyGroup()) {
            prepareRangeDeletes(
                    keyGroupPrefixBytes,
                    targetKeyGroupRange.getEndKeyGroup() + 1,
                    currentKeyGroupRange.getEndKeyGroup() + 1,
                    deleteFilesRanges);
        }

        logger.info(
                "Performing range delete for backend with target key-groups range {} with boundaries set {} - deleteFilesInRanges = {}.",
                targetKeyGroupRange.prettyPrintInterval(),
                deleteFilesRanges.stream().map(Arrays::toString).collect(Collectors.toList()),
                useDeleteFilesInRange);

        deleteRangeData(db, columnFamilyHandles, deleteFilesRanges, useDeleteFilesInRange);
    }

    private static void prepareRangeDeletes(
            int keyGroupPrefixBytes,
            int beginKeyGroup,
            int endKeyGroup,
            List<byte[]> deleteFilesRangesOut) {
        byte[] beginKeyGroupBytes = new byte[keyGroupPrefixBytes];
        byte[] endKeyGroupBytes = new byte[keyGroupPrefixBytes];
        CompositeKeySerializationUtils.serializeKeyGroup(beginKeyGroup, beginKeyGroupBytes);
        CompositeKeySerializationUtils.serializeKeyGroup(endKeyGroup, endKeyGroupBytes);
        deleteFilesRangesOut.add(beginKeyGroupBytes);
        deleteFilesRangesOut.add(endKeyGroupBytes);
    }

    /**
     * Delete the record that falls into the given deleteRanges of the db.
     *
     * @param db the target need to be clipped.
     * @param columnFamilyHandles the column family need to be clipped.
     * @param deleteRanges - pairs of deleted ranges (from1, to1, from2, to2, ...). For each pair
     *     [from, to), the startKey ('from') is inclusive, the endKey ('to') is exclusive.
     * @param useDeleteFilesInRange whether to use deleteFilesInRange to clean up redundant files.
     */
    private static void deleteRangeData(
            RocksDB db,
            List<ColumnFamilyHandle> columnFamilyHandles,
            List<byte[]> deleteRanges,
            boolean useDeleteFilesInRange)
            throws RocksDBException {

        if (deleteRanges.isEmpty()) {
            // nothing to do.
            return;
        }

        Preconditions.checkArgument(deleteRanges.size() % 2 == 0);
        for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
            // First delete the files in ranges
            if (useDeleteFilesInRange) {
                db.deleteFilesInRanges(columnFamilyHandle, deleteRanges, false);
            }

            // Then put range limiting tombstones in place.
            for (int i = 0; i < deleteRanges.size() / 2; i++) {
                // Using RocksDB's deleteRange will take advantage of delete
                // tombstones, which mark the range as deleted.
                //
                // https://github.com/ververica/frocksdb/blob/FRocksDB-6.20.3/include/rocksdb/db.h#L363-L377
                db.deleteRange(
                        columnFamilyHandle, deleteRanges.get(i * 2), deleteRanges.get(i * 2 + 1));
            }
        }
    }

    /**
     * Checks data in the SST files of the given DB for keys that exceed either the lower and upper
     * bound of the proclaimed key-groups range of the DB.
     *
     * @param db the DB to check.
     * @param keyGroupPrefixBytes the number of bytes used to serialize the key-group prefix of keys
     *     in the DB.
     * @param dbExpectedKeyGroupRange the expected key-groups range of the DB.
     * @return the check result with detailed info about lower and upper bound violations.
     */
    public static RangeCheckResult checkSstDataAgainstKeyGroupRange(
            RocksDB db, int keyGroupPrefixBytes, KeyGroupRange dbExpectedKeyGroupRange) {
        final byte[] beginKeyGroupBytes = new byte[keyGroupPrefixBytes];
        final byte[] endKeyGroupBytes = new byte[keyGroupPrefixBytes];

        CompositeKeySerializationUtils.serializeKeyGroup(
                dbExpectedKeyGroupRange.getStartKeyGroup(), beginKeyGroupBytes);

        CompositeKeySerializationUtils.serializeKeyGroup(
                dbExpectedKeyGroupRange.getEndKeyGroup() + 1, endKeyGroupBytes);

        KeyRange dbKeyRange = getDBKeyRange(db);
        return RangeCheckResult.of(
                beginKeyGroupBytes,
                endKeyGroupBytes,
                dbKeyRange.minKey,
                dbKeyRange.maxKey,
                keyGroupPrefixBytes);
    }

    /** Returns a pair of minimum and maximum key in the sst files of the given database. */
    private static KeyRange getDBKeyRange(RocksDB db) {
        final Comparator<byte[]> comparator = UnsignedBytes.lexicographicalComparator();
        final List<LiveFileMetaData> liveFilesMetaData = db.getLiveFilesMetaData();

        if (liveFilesMetaData.isEmpty()) {
            return KeyRange.EMPTY;
        }

        Iterator<LiveFileMetaData> liveFileMetaDataIterator = liveFilesMetaData.iterator();
        LiveFileMetaData fileMetaData = liveFileMetaDataIterator.next();
        byte[] smallestKey = fileMetaData.smallestKey();
        byte[] largestKey = fileMetaData.largestKey();
        while (liveFileMetaDataIterator.hasNext()) {
            fileMetaData = liveFileMetaDataIterator.next();
            byte[] sstSmallestKey = fileMetaData.smallestKey();
            byte[] sstLargestKey = fileMetaData.largestKey();
            if (comparator.compare(sstSmallestKey, smallestKey) < 0) {
                smallestKey = sstSmallestKey;
            }
            if (comparator.compare(sstLargestKey, largestKey) > 0) {
                largestKey = sstLargestKey;
            }
        }
        return KeyRange.of(smallestKey, largestKey);
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
     * KeyGroupRange, double)} to init the initial db from the given lists and returns its index.
     *
     * @param restoreStateHandles The candidate state handles.
     * @param targetKeyGroupRange The target key group range.
     * @param overlapFractionThreshold configured threshold for overlap.
     * @return the index of the best candidate handle in the list or -1 if the list was empty.
     * @param <T> the generic parameter type of the state handles.
     */
    public static <T extends KeyedStateHandle> int findTheBestStateHandleForInitial(
            @Nonnull List<T> restoreStateHandles,
            @Nonnull KeyGroupRange targetKeyGroupRange,
            double overlapFractionThreshold) {

        if (restoreStateHandles.isEmpty()) {
            return -1;
        }

        // Shortcut for a common case (scale out)
        if (restoreStateHandles.size() == 1) {
            return 0;
        }

        int currentPos = 0;
        int bestHandlePos = 0;
        Score bestScore = Score.MIN;
        for (T rawStateHandle : restoreStateHandles) {
            Score handleScore =
                    stateHandleEvaluator(
                            rawStateHandle, targetKeyGroupRange, overlapFractionThreshold);
            if (handleScore.compareTo(bestScore) > 0) {
                bestHandlePos = currentPos;
                bestScore = handleScore;
            }
            ++currentPos;
        }
        return bestHandlePos;
    }

    /** Helper class tha defines a key-range in RocksDB as byte arrays for min and max key. */
    private static final class KeyRange {
        static final KeyRange EMPTY = KeyRange.of(new byte[0], new byte[0]);

        final byte[] minKey;
        final byte[] maxKey;

        private KeyRange(byte[] minKey, byte[] maxKey) {
            this.minKey = minKey;
            this.maxKey = maxKey;
        }

        static KeyRange of(byte[] minKey, byte[] maxKey) {
            return new KeyRange(minKey, maxKey);
        }
    }

    /**
     * Helper class that represents the result of a range check of the actual keys in a RocksDB
     * instance against the proclaimed key-group range of the instance. In short, this checks if the
     * instance contains any keys (or tombstones for keys) that don't belong in the instance's
     * key-groups range.
     */
    public static final class RangeCheckResult {
        private final byte[] proclaimedMinKey;
        private final byte[] proclaimedMaxKey;
        private final byte[] actualMinKey;
        private final byte[] actualMaxKey;
        final boolean leftInRange;
        final boolean rightInRange;

        final int keyGroupPrefixBytes;

        private RangeCheckResult(
                byte[] proclaimedMinKey,
                byte[] proclaimedMaxKey,
                byte[] actualMinKey,
                byte[] actualMaxKey,
                int keyGroupPrefixBytes) {
            Comparator<byte[]> comparator = UnsignedBytes.lexicographicalComparator();
            this.proclaimedMinKey = proclaimedMinKey;
            this.proclaimedMaxKey = proclaimedMaxKey;
            this.actualMinKey = actualMinKey;
            this.actualMaxKey = actualMaxKey;
            this.leftInRange = comparator.compare(actualMinKey, proclaimedMinKey) >= 0;
            // TODO: consider using <= here to avoid that range delete tombstones of
            //  (targetMaxKeyGroup + 1) prevent using ingest for no good reason.
            this.rightInRange = comparator.compare(actualMaxKey, proclaimedMaxKey) < 0;
            this.keyGroupPrefixBytes = keyGroupPrefixBytes;
        }

        public boolean allInRange() {
            return leftInRange && rightInRange;
        }

        public byte[] getProclaimedMinKey() {
            return proclaimedMinKey;
        }

        public byte[] getProclaimedMaxKey() {
            return proclaimedMaxKey;
        }

        public byte[] getActualMinKey() {
            return actualMinKey;
        }

        public byte[] getActualMaxKey() {
            return actualMaxKey;
        }

        public int getKeyGroupPrefixBytes() {
            return keyGroupPrefixBytes;
        }

        public boolean isLeftInRange() {
            return leftInRange;
        }

        public boolean isRightInRange() {
            return rightInRange;
        }

        static RangeCheckResult of(
                byte[] proclaimedMinKey,
                byte[] proclaimedMaxKey,
                byte[] actualMinKey,
                byte[] actualMaxKey,
                int keyGroupPrefixBytes) {
            return new RangeCheckResult(
                    proclaimedMinKey,
                    proclaimedMaxKey,
                    actualMinKey,
                    actualMaxKey,
                    keyGroupPrefixBytes);
        }

        @Override
        public String toString() {
            return "RangeCheckResult{"
                    + "leftInRange="
                    + leftInRange
                    + ", rightInRange="
                    + rightInRange
                    + ", actualMinKeyGroup="
                    + CompositeKeySerializationUtils.extractKeyGroup(
                            keyGroupPrefixBytes, getActualMinKey())
                    + ", actualMaxKeyGroup="
                    + CompositeKeySerializationUtils.extractKeyGroup(
                            keyGroupPrefixBytes, getActualMaxKey())
                    + '}';
        }
    }
}
