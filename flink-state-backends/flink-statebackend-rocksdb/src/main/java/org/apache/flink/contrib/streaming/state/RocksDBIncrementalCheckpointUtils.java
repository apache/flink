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
import org.apache.flink.util.function.RunnableWithException;

import org.apache.flink.shaded.guava31.com.google.common.primitives.UnsignedBytes;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.LiveFileMetaData;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/** Utils for RocksDB Incremental Checkpoint. */
public class RocksDBIncrementalCheckpointUtils {

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

    /**
     * Returns true, if all entries in the sst files of the given DB is strictly within the expected
     * key-group range for the DB.
     *
     * @param db the DB to check.
     * @param dbExpectedKeyGroupRange the expected key-groups range of the DB.
     * @param keyGroupPrefixBytes the number of bytes used to serialize the key-group prefix of keys
     *     in the DB.
     */
    public static boolean isSstDataInKeyGroupRange(
            RocksDB db, int keyGroupPrefixBytes, KeyGroupRange dbExpectedKeyGroupRange) {
        return checkSstDataAgainstKeyGroupRange(db, keyGroupPrefixBytes, dbExpectedKeyGroupRange)
                .allInRange();
    }

    /**
     * Returns a range compaction task as runnable if any data in the SST files of the given DB
     * exceeds the proclaimed key-group range.
     *
     * @param db the DB to check and compact if needed.
     * @param columnFamilyHandles list of column families to check.
     * @param keyGroupPrefixBytes the number of bytes used to serialize the key-group prefix of keys
     *     in the DB.
     * @param dbExpectedKeyGroupRange the expected key-groups range of the DB.
     * @return runnable that performs compaction upon execution if the key-groups range is exceeded.
     *     Otherwise, empty optional is returned.
     */
    public static Optional<RunnableWithException> createRangeCompactionTaskIfNeeded(
            RocksDB db,
            Collection<ColumnFamilyHandle> columnFamilyHandles,
            int keyGroupPrefixBytes,
            KeyGroupRange dbExpectedKeyGroupRange) {

        RangeCheckResult rangeCheckResult =
                checkSstDataAgainstKeyGroupRange(db, keyGroupPrefixBytes, dbExpectedKeyGroupRange);

        if (rangeCheckResult.allInRange()) {
            // No keys exceed the proclaimed range of the backend, so we don't need a compaction
            // from this point of view.
            return Optional.empty();
        }

        return Optional.of(
                () -> {
                    /*
                    try (CompactRangeOptions compactionOptions =
                            new CompactRangeOptions()
                                    .setExclusiveManualCompaction(true)
                                    .setBottommostLevelCompaction(
                                            CompactRangeOptions.BottommostLevelCompaction
                                                    .kForceOptimized)) {

                        if (!rangeCheckResult.leftInRange) {
                            // Compact all keys before from the expected key-groups range
                            for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
                                db.compactRange(
                                        columnFamilyHandle,
                                        // TODO: change to null once this API is fixed
                                        new byte[] {},
                                        rangeCheckResult.minKey,
                                        compactionOptions);
                            }
                        }

                        if (!rangeCheckResult.rightInRange) {
                            // Compact all keys after the expected key-groups range
                            for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
                                db.compactRange(
                                        columnFamilyHandle,
                                        rangeCheckResult.maxKey,
                                        // TODO: change to null once this API is fixed
                                        new byte[] {
                                            (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff
                                        },
                                        compactionOptions);
                            }
                        }
                    }
                     */
                });
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
    private static RangeCheckResult checkSstDataAgainstKeyGroupRange(
            RocksDB db, int keyGroupPrefixBytes, KeyGroupRange dbExpectedKeyGroupRange) {
        final byte[] beginKeyGroupBytes = new byte[keyGroupPrefixBytes];
        final byte[] endKeyGroupBytes = new byte[keyGroupPrefixBytes];

        CompositeKeySerializationUtils.serializeKeyGroup(
                dbExpectedKeyGroupRange.getStartKeyGroup(), beginKeyGroupBytes);

        CompositeKeySerializationUtils.serializeKeyGroup(
                dbExpectedKeyGroupRange.getEndKeyGroup() + 1, endKeyGroupBytes);

        KeyRange dbKeyRange = getDBKeyRange(db);
        Comparator<byte[]> comparator = UnsignedBytes.lexicographicalComparator();
        return RangeCheckResult.of(
                comparator.compare(dbKeyRange.minKey, beginKeyGroupBytes) >= 0,
                comparator.compare(dbKeyRange.maxKey, endKeyGroupBytes) < 0,
                beginKeyGroupBytes,
                endKeyGroupBytes);
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

    /**
     * Exports the data of the given column families in the given DB.
     *
     * @param db the DB to export from.
     * @param columnFamilyHandles the column families to export.
     * @param registeredStateMetaInfoBases meta information about the registered states in the DB.
     * @param exportBasePath the path to which the export files go.
     * @param resultOutput output parameter for the metadata of the export.
     * @throws RocksDBException on problems inside RocksDB.
     */
    /*
    public static void exportColumnFamilies(
            RocksDB db,
            List<ColumnFamilyHandle> columnFamilyHandles,
            List<RegisteredStateMetaInfoBase> registeredStateMetaInfoBases,
            Path exportBasePath,
            Map<RegisteredStateMetaInfoBase, List<ExportImportFilesMetaData>> resultOutput)
            throws RocksDBException {

        Preconditions.checkArgument(
                columnFamilyHandles.size() == registeredStateMetaInfoBases.size(),
                "Lists are aligned by index and must be of the same size!");

        try (final Checkpoint checkpoint = Checkpoint.create(db)) {
            for (int i = 0; i < columnFamilyHandles.size(); i++) {
                RegisteredStateMetaInfoBase stateMetaInfo = registeredStateMetaInfoBases.get(i);

                Path subPath = exportBasePath.resolve(UUID.randomUUID().toString());
                ExportImportFilesMetaData exportedColumnFamilyMetaData =
                        checkpoint.exportColumnFamily(
                                columnFamilyHandles.get(i), subPath.toString());

                File[] exportedSstFiles =
                        subPath.toFile()
                                .listFiles((file, name) -> name.toLowerCase().endsWith(".sst"));

                if (exportedSstFiles != null && exportedSstFiles.length > 0) {
                    resultOutput
                            .computeIfAbsent(stateMetaInfo, (key) -> new ArrayList<>())
                            .add(exportedColumnFamilyMetaData);
                } else {
                    // Close unused empty export result
                    IOUtils.closeQuietly(exportedColumnFamilyMetaData);
                }
            }
        }
    }
    */

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
     * @param overlapFractionThreshold configured threshold for overlap.
     * @return The best candidate or null if no candidate was a good fit.
     * @param <T> the generic parameter type of the state handles.
     */
    @Nullable
    public static <T extends KeyedStateHandle> T chooseTheBestStateHandleForInitial(
            @Nonnull List<T> restoreStateHandles,
            @Nonnull KeyGroupRange targetKeyGroupRange,
            double overlapFractionThreshold) {

        int pos =
                findTheBestStateHandleForInitial(
                        restoreStateHandles, targetKeyGroupRange, overlapFractionThreshold);
        return pos >= 0 ? restoreStateHandles.get(pos) : null;
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
    private static final class RangeCheckResult {
        private final byte[] minKey;

        private final byte[] maxKey;
        final boolean leftInRange;
        final boolean rightInRange;

        private RangeCheckResult(
                boolean leftInRange, boolean rightInRange, byte[] minKey, byte[] maxKey) {
            this.leftInRange = leftInRange;
            this.rightInRange = rightInRange;
            this.minKey = minKey;
            this.maxKey = maxKey;
        }

        boolean allInRange() {
            return leftInRange && rightInRange;
        }

        static RangeCheckResult of(
                boolean leftInRange, boolean rightInRange, byte[] minKey, byte[] maxKey) {
            return new RangeCheckResult(leftInRange, rightInRange, minKey, maxKey);
        }
    }
}
