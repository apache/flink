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

package org.apache.flink.state.rocksdb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.Snapshot;
import org.rocksdb.Statistics;
import org.rocksdb.TickerType;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for prefix bounds and resource ownership of RocksDB iterators. */
class RocksDBPrefixIteratorTest {

    @RegisterExtension final RocksDBExtension rocksDBExtension = new RocksDBExtension(true);

    @ParameterizedTest
    @MethodSource("prefixEnds")
    void testPrefixEnd(byte[] prefix, byte[] expectedEnd) {
        assertThat(RocksDBOperationUtils.getPrefixEnd(prefix)).isEqualTo(expectedEnd);
    }

    private static Stream<Arguments> prefixEnds() {
        return Stream.of(
                Arguments.of(bytes(0), bytes(1)),
                Arguments.of(bytes(1), bytes(2)),
                // The successor of 0x7F is 0x80 under unsigned ordering.
                Arguments.of(bytes(127), bytes(128)),
                // The incremented byte may itself become 0xFF.
                Arguments.of(bytes(254), bytes(255)),
                Arguments.of(bytes(0x12, 0xFE, 0xFF), bytes(0x12, 0xFF)),
                // Trailing 0xFF bytes are dropped before the increment.
                Arguments.of(bytes(1, 255, 255), bytes(2)),
                Arguments.of(bytes(1, 255, 0), bytes(1, 255, 1)),
                // No finite bound exists for all-0xFF or empty prefixes.
                Arguments.of(bytes(255), null),
                Arguments.of(bytes(255, 255), null),
                Arguments.of(bytes(), null));
    }

    @ParameterizedTest
    @MethodSource("prefixRanges")
    void testPrefixRange(byte[] prefix, byte[][] keys, byte[][] expectedKeys)
            throws RocksDBException {
        putKeys(keys);

        try (RocksIteratorWrapper iterator =
                createIterator(rocksDBExtension.getReadOptions(), prefix)) {
            assertKeys(iterator, prefix, expectedKeys);
        }
    }

    private static Stream<Arguments> prefixRanges() {
        return Stream.of(
                Arguments.of(
                        bytes(1),
                        new byte[][] {bytes(0), bytes(1), bytes(1, 0), bytes(1, 255), bytes(2)},
                        new byte[][] {bytes(1), bytes(1, 0), bytes(1, 255)}),
                Arguments.of(
                        bytes(1, 255, 255),
                        new byte[][] {
                            bytes(1, 255, 254), bytes(1, 255, 255), bytes(1, 255, 255, 0), bytes(2)
                        },
                        new byte[][] {bytes(1, 255, 255), bytes(1, 255, 255, 0)}),
                Arguments.of(
                        bytes(127),
                        new byte[][] {bytes(126), bytes(127), bytes(127, 255), bytes(128)},
                        new byte[][] {bytes(127), bytes(127, 255)}),
                Arguments.of(
                        bytes(255, 255),
                        new byte[][] {
                            bytes(255, 254),
                            bytes(255, 255),
                            bytes(255, 255, 0),
                            bytes(255, 255, 255)
                        },
                        new byte[][] {bytes(255, 255), bytes(255, 255, 0), bytes(255, 255, 255)}),
                Arguments.of(
                        bytes(),
                        new byte[][] {bytes(), bytes(0), bytes(127), bytes(128), bytes(255)},
                        new byte[][] {bytes(), bytes(0), bytes(127), bytes(128), bytes(255)}));
    }

    @Test
    void testConcurrentIteratorsDoNotModifyOrCloseSharedOptions() throws RocksDBException {
        putKeys(bytes(1, 0), bytes(2, 0), bytes(3, 0));
        final ReadOptions sharedOptions = rocksDBExtension.getReadOptions();
        sharedOptions.setFillCache(false);

        try (RocksIteratorWrapper second = createIterator(sharedOptions, bytes(2))) {
            try (RocksIteratorWrapper first = createIterator(sharedOptions, bytes(1))) {
                assertKeys(first, bytes(1), bytes(1, 0));
                assertThat(sharedOptions.iterateUpperBound()).isNull();
                assertThat(sharedOptions.fillCache()).isFalse();
            }

            assertKeys(second, bytes(2), bytes(2, 0));
        }

        assertThat(sharedOptions.isOwningHandle()).isTrue();
        try (RocksIteratorWrapper iterator =
                RocksDBOperationUtils.getRocksIterator(
                        rocksDBExtension.getRocksDB(),
                        rocksDBExtension.getDefaultColumnFamily(),
                        sharedOptions)) {
            assertKeys(iterator, bytes(), bytes(1, 0), bytes(2, 0), bytes(3, 0));
        }
    }

    @Test
    void testWrapperClosesOwnedResources() {
        final ReadOptions sharedOptions = rocksDBExtension.getReadOptions();
        try (TrackingSlice upperBound = new TrackingSlice(bytes(2));
                ReadOptions ownedOptions =
                        new ReadOptions(sharedOptions).setIterateUpperBound(upperBound);
                RocksIterator nativeIterator =
                        rocksDBExtension
                                .getRocksDB()
                                .newIterator(
                                        rocksDBExtension.getDefaultColumnFamily(), ownedOptions);
                RocksIteratorWrapper iterator =
                        new RocksIteratorWrapper(nativeIterator, ownedOptions, upperBound)) {
            iterator.close();
            iterator.close();

            assertThat(nativeIterator.isOwningHandle()).isFalse();
            assertThat(ownedOptions.isOwningHandle()).isFalse();
            assertThat(upperBound.isClosed()).isTrue();
            assertThat(sharedOptions.isOwningHandle()).isTrue();
            assertThat(sharedOptions.iterateUpperBound()).isNull();
        }
    }

    @Test
    void testConfiguredSnapshotIsPreserved() throws RocksDBException {
        final RocksDB db = rocksDBExtension.getRocksDB();
        db.put(rocksDBExtension.getDefaultColumnFamily(), bytes(1, 0), bytes(10));
        final Snapshot snapshot = db.getSnapshot();

        try (ReadOptions readOptions = new ReadOptions().setSnapshot(snapshot)) {
            db.put(rocksDBExtension.getDefaultColumnFamily(), bytes(1, 0), bytes(20));
            db.put(rocksDBExtension.getDefaultColumnFamily(), bytes(1, 1), bytes(30));

            try (RocksIteratorWrapper iterator = createIterator(readOptions, bytes(1))) {
                iterator.seek(bytes(1));
                assertThat(iterator.isValid()).isTrue();
                assertThat(iterator.key()).isEqualTo(bytes(1, 0));
                assertThat(iterator.value()).isEqualTo(bytes(10));
                iterator.next();
                assertThat(iterator.isValid()).isFalse();
            }

            assertThat(readOptions.isOwningHandle()).isTrue();
        } finally {
            db.releaseSnapshot(snapshot);
        }
    }

    @Test
    void testUpperBoundWithPrefixExtractor() throws RocksDBException {
        final RocksDB db = rocksDBExtension.getRocksDB();
        try (BloomFilter filter = new BloomFilter(10);
                ColumnFamilyOptions options =
                        new ColumnFamilyOptions()
                                .useFixedLengthPrefixExtractor(1)
                                .setTableFormatConfig(prefixBloomTableConfig(filter));
                ColumnFamilyHandle columnFamily =
                        db.createColumnFamily(new ColumnFamilyDescriptor(bytes(42), options));
                FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
            db.put(columnFamily, bytes(0, 0), bytes());
            db.put(columnFamily, bytes(1, 0), bytes());
            db.put(columnFamily, bytes(1, 255), bytes());
            db.put(columnFamily, bytes(2, 0), bytes());
            db.flush(flushOptions, columnFamily);

            try (RocksIteratorWrapper iterator =
                    RocksDBOperationUtils.getRocksIteratorBoundedByPrefix(
                            db, columnFamily, rocksDBExtension.getReadOptions(), bytes(1))) {
                assertKeys(iterator, bytes(1), bytes(1, 0), bytes(1, 255));
            }
        }
    }

    @Test
    void testUpperBoundShorterThanPrefixExtractorDomain() throws RocksDBException {
        final RocksDB db = rocksDBExtension.getRocksDB();
        try (BloomFilter filter = new BloomFilter(10);
                ColumnFamilyOptions options =
                        new ColumnFamilyOptions()
                                .useFixedLengthPrefixExtractor(2)
                                .setTableFormatConfig(prefixBloomTableConfig(filter));
                ColumnFamilyHandle columnFamily =
                        db.createColumnFamily(new ColumnFamilyDescriptor(bytes(42), options));
                FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
            db.put(columnFamily, bytes(0), bytes());
            db.put(columnFamily, bytes(0, 254, 0), bytes());
            db.put(columnFamily, bytes(0, 255, 0), bytes());
            db.put(columnFamily, bytes(0, 255, 255), bytes());
            db.put(columnFamily, bytes(1), bytes());
            db.put(columnFamily, bytes(1, 0, 0), bytes());
            db.flush(flushOptions, columnFamily);

            try (RocksIteratorWrapper iterator =
                    RocksDBOperationUtils.getRocksIteratorBoundedByPrefix(
                            db, columnFamily, rocksDBExtension.getReadOptions(), bytes(0, 255))) {
                assertKeys(iterator, bytes(0, 255), bytes(0, 255, 0), bytes(0, 255, 255));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testResumedSeekDoesNotStopAtExtractedPrefix(boolean totalOrderSeek)
            throws RocksDBException {
        final RocksDB db = rocksDBExtension.getRocksDB();
        try (ColumnFamilyOptions options =
                        new ColumnFamilyOptions().useFixedLengthPrefixExtractor(2);
                ColumnFamilyHandle columnFamily =
                        db.createColumnFamily(new ColumnFamilyDescriptor(bytes(42), options));
                ReadOptions readOptions =
                        new ReadOptions()
                                .setPrefixSameAsStart(true)
                                .setTotalOrderSeek(totalOrderSeek)) {
            db.put(columnFamily, bytes(1, 0), bytes());
            db.put(columnFamily, bytes(1, 1), bytes());
            db.put(columnFamily, bytes(2, 0), bytes());

            try (RocksIteratorWrapper iterator =
                    RocksDBOperationUtils.getRocksIteratorBoundedByPrefix(
                            db, columnFamily, readOptions, bytes(1))) {
                // The map prefix is [1], but the resume key's extracted prefix is [1, 0].
                assertKeys(iterator, bytes(1, 0), bytes(1, 0), bytes(1, 1));
            }

            assertThat(readOptions.prefixSameAsStart()).isTrue();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testInitialSeekDoesNotStopAtExtractedPrefix(boolean totalOrderSeek)
            throws RocksDBException {
        final RocksDB db = rocksDBExtension.getRocksDB();
        // RocksDB enforces prefixSameAsStart regardless of totalOrderSeek, so both are covered.
        try (ColumnFamilyOptions options = new ColumnFamilyOptions().useCappedPrefixExtractor(2);
                ColumnFamilyHandle columnFamily =
                        db.createColumnFamily(new ColumnFamilyDescriptor(bytes(42), options));
                ReadOptions readOptions =
                        new ReadOptions()
                                .setPrefixSameAsStart(true)
                                .setTotalOrderSeek(totalOrderSeek)) {
            db.put(columnFamily, bytes(1, 0), bytes());
            db.put(columnFamily, bytes(1, 1), bytes());
            db.put(columnFamily, bytes(2, 0), bytes());

            try (RocksIteratorWrapper iterator =
                    RocksDBOperationUtils.getRocksIteratorBoundedByPrefix(
                            db, columnFamily, readOptions, bytes(1))) {
                // The bare prefix [1] is its own extracted prefix, which no stored key shares.
                assertKeys(iterator, bytes(1), bytes(1, 0), bytes(1, 1));
            }

            assertThat(readOptions.prefixSameAsStart()).isTrue();
        }
    }

    @Test
    void testConfiguredLowerBoundDoesNotClampSeek() throws RocksDBException {
        final RocksDB db = rocksDBExtension.getRocksDB();
        try (ColumnFamilyOptions options = new ColumnFamilyOptions();
                ColumnFamilyHandle columnFamily =
                        db.createColumnFamily(new ColumnFamilyDescriptor(bytes(42), options));
                Slice lowerBound = new Slice(bytes(1, 1));
                ReadOptions readOptions = new ReadOptions().setIterateLowerBound(lowerBound)) {
            db.put(columnFamily, bytes(1, 0), bytes());
            db.put(columnFamily, bytes(1, 1), bytes());
            db.put(columnFamily, bytes(2, 0), bytes());

            try (RocksIteratorWrapper iterator =
                    RocksDBOperationUtils.getRocksIteratorBoundedByPrefix(
                            db, columnFamily, readOptions, bytes(1))) {
                // An inherited lower bound would clamp the seek to [1, 1].
                assertKeys(iterator, bytes(1, 0), bytes(1, 0), bytes(1, 1));
            }

            assertThat(readOptions.iterateLowerBound().data()).isEqualTo(bytes(1, 1));
        }
    }

    @ParameterizedTest
    @MethodSource("prefixBloomRanges")
    void testPrefixBloomFiltersRespectBoundsAndConfiguredTotalOrderSeek(
            byte[] prefix, int extractorLength, boolean filterCompatible, boolean totalOrderSeek)
            throws RocksDBException {
        final RocksDB db = rocksDBExtension.getRocksDB();
        try (BloomFilter filter = new BloomFilter(10);
                ColumnFamilyOptions options =
                        new ColumnFamilyOptions()
                                .useFixedLengthPrefixExtractor(extractorLength)
                                .setTableFormatConfig(prefixBloomTableConfig(filter));
                ColumnFamilyHandle columnFamily =
                        db.createColumnFamily(new ColumnFamilyDescriptor(bytes(42), options));
                FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true);
                ReadOptions readOptions = new ReadOptions().setTotalOrderSeek(totalOrderSeek);
                // Each call owns a separate native shared_ptr wrapper that must be closed.
                Statistics statistics = rocksDBExtension.getDbOptions().statistics()) {
            db.put(columnFamily, bytes(0, 0, 0), bytes());
            db.put(columnFamily, bytes(2, 0, 0), bytes());
            db.flush(flushOptions, columnFamily);

            final long filterLookupsBefore = filterLookups(statistics);
            try (RocksIteratorWrapper iterator =
                    RocksDBOperationUtils.getRocksIteratorBoundedByPrefix(
                            db, columnFamily, readOptions, prefix)) {
                assertKeys(iterator, prefix);
            }

            // Filter block accesses prove that the SST prefix Bloom filter was consulted.
            assertThat(filterLookups(statistics) - filterLookupsBefore)
                    .isEqualTo(filterCompatible && !totalOrderSeek ? 1 : 0);
            assertThat(readOptions.totalOrderSeek()).isEqualTo(totalOrderSeek);
            assertThat(readOptions.autoPrefixMode()).isFalse();
            assertThat(readOptions.iterateUpperBound()).isNull();
        }
    }

    private static Stream<Arguments> prefixBloomRanges() {
        return Stream.of(
                // The bound shares the seek key's extracted prefix.
                Arguments.of(bytes(1, 0), 1, true, false),
                Arguments.of(bytes(1, 0), 1, true, true),
                // The bound is the immediate successor of the extracted prefix.
                Arguments.of(bytes(1), 1, true, false),
                Arguments.of(bytes(1), 1, true, true),
                // The bound is shorter than the extractor, so no Bloom filter can be consulted.
                Arguments.of(bytes(0, 255), 2, false, false));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testPrefixRangeSpansMultipleExtractedPrefixes(boolean capped) throws RocksDBException {
        final RocksDB db = rocksDBExtension.getRocksDB();
        try (BloomFilter filter = new BloomFilter(10);
                ColumnFamilyOptions options =
                        new ColumnFamilyOptions()
                                .setTableFormatConfig(prefixBloomTableConfig(filter))) {
            if (capped) {
                options.useCappedPrefixExtractor(2);
            } else {
                options.useFixedLengthPrefixExtractor(2);
            }
            try (ColumnFamilyHandle columnFamily =
                            db.createColumnFamily(new ColumnFamilyDescriptor(bytes(42), options));
                    FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
                final byte[][] expectedKeys =
                        new byte[][] {bytes(1), bytes(1, 0), bytes(1, 1, 0), bytes(1, 255)};
                db.put(columnFamily, bytes(0), bytes());
                for (byte[] key : expectedKeys) {
                    db.put(columnFamily, key, bytes());
                }
                db.put(columnFamily, bytes(2), bytes());
                db.flush(flushOptions, columnFamily);

                try (RocksIteratorWrapper iterator =
                        RocksDBOperationUtils.getRocksIteratorBoundedByPrefix(
                                db, columnFamily, rocksDBExtension.getReadOptions(), bytes(1))) {
                    assertKeys(iterator, bytes(1), expectedKeys);
                }
            }
        }
    }

    private static BlockBasedTableConfig prefixBloomTableConfig(BloomFilter filter) {
        return new BlockBasedTableConfig()
                .setFilterPolicy(filter)
                .setWholeKeyFiltering(false)
                .setCacheIndexAndFilterBlocks(true);
    }

    private static long filterLookups(Statistics statistics) {
        return statistics.getTickerCount(TickerType.BLOCK_CACHE_FILTER_HIT)
                + statistics.getTickerCount(TickerType.BLOCK_CACHE_FILTER_MISS);
    }

    private RocksIteratorWrapper createIterator(ReadOptions readOptions, byte[] prefix) {
        return RocksDBOperationUtils.getRocksIteratorBoundedByPrefix(
                rocksDBExtension.getRocksDB(),
                rocksDBExtension.getDefaultColumnFamily(),
                readOptions,
                prefix);
    }

    private void putKeys(byte[]... keys) throws RocksDBException {
        for (byte[] key : keys) {
            rocksDBExtension
                    .getRocksDB()
                    .put(rocksDBExtension.getDefaultColumnFamily(), key, bytes());
        }
    }

    private static void assertKeys(
            RocksIteratorWrapper iterator, byte[] seekKey, byte[]... expectedKeys) {
        iterator.seek(seekKey);
        for (byte[] expectedKey : expectedKeys) {
            assertThat(iterator.isValid()).isTrue();
            assertThat(iterator.key()).isEqualTo(expectedKey);
            iterator.next();
        }
        assertThat(iterator.isValid()).isFalse();
    }

    private static byte[] bytes(int... values) {
        final byte[] bytes = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            bytes[i] = (byte) values[i];
        }
        return bytes;
    }

    private static final class TrackingSlice extends Slice {

        private TrackingSlice(byte[] data) {
            super(data);
        }

        private boolean isClosed() {
            return !isOwningHandle();
        }
    }
}
