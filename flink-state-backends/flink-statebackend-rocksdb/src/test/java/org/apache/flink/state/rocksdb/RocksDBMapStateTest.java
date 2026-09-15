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

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.util.IOUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.FlushOptions;
import org.rocksdb.MutableColumnFamilyOptions;
import org.rocksdb.TableProperties;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.state.rocksdb.RocksDBMapState.ITERATOR_CACHE_SIZE;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests iteration and prefix isolation of RocksDB map state operations. */
class RocksDBMapStateTest {

    // Two cache loads plus one entry: a fully refilled cache followed by a partial refill.
    private static final int MAP_ENTRIES = 2 * ITERATOR_CACHE_SIZE + 1;

    // A deleted entry can leave both its value and a tombstone.
    // Allow both records per entry, plus one extra skip.
    private static final int SKIPPABLE_INTERNAL_KEY_LIMIT = 2 * MAP_ENTRIES + 1;

    // An unbounded seek into the neighboring map must exceed the skip limit.
    private static final int NEIGHBOR_TOMBSTONES = SKIPPABLE_INTERNAL_KEY_LIMIT + 1;
    private static final MapStateDescriptor<Integer, Integer> STATE_DESCRIPTOR =
            new MapStateDescriptor<>("map", IntSerializer.INSTANCE, IntSerializer.INSTANCE);

    @TempDir private Path temporaryDirectory;

    private RocksDBKeyedStateBackend<Integer> backend;
    private ColumnFamilyHandle columnFamily;

    @BeforeEach
    void setUp() throws Exception {
        backend =
                RocksDBTestUtils.builderForTestDefaults(
                                temporaryDirectory.toFile(),
                                IntSerializer.INSTANCE,
                                1,
                                new KeyGroupRange(0, 0),
                                Collections.emptyList())
                        .build();
        mapState(0, 0);
        columnFamily = backend.getColumnFamilyHandle(STATE_DESCRIPTOR.getName());
        backend.db.setOptions(
                columnFamily,
                MutableColumnFamilyOptions.builder().setDisableAutoCompactions(true).build());
        // Allow the entire map to be deleted, but fail if a seek scans the neighbor's tombstones.
        backend.getReadOptions().setMaxSkippableInternalKeys(SKIPPABLE_INTERNAL_KEY_LIMIT);
    }

    @AfterEach
    void tearDown() {
        if (backend != null) {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    @ParameterizedTest
    @EnumSource(EmptyMapOperation.class)
    void testEmptyMapDoesNotScanDeletedEntriesOfNextKey(EmptyMapOperation operation)
            throws Exception {
        createNeighborTombstones();
        final MapState<Integer, Integer> state = mapState(0, 0);

        switch (operation) {
            case ENTRIES:
                assertThat(state.entries()).isEmpty();
                break;
            case IS_EMPTY:
                assertThat(state.isEmpty()).isTrue();
                break;
            case CLEAR:
                state.clear();
                break;
            default:
                throw new IllegalArgumentException("Unknown map operation: " + operation);
        }

        assertNeighborPreserved();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testIteratorCacheReloadDoesNotScanDeletedEntriesOfNextKey(boolean removeEntries)
            throws Exception {
        createNeighborTombstones();
        final MapState<Integer, Integer> state = mapState(0, 0);
        populateMap(state);

        final List<Integer> keys = new ArrayList<>();
        final Iterator<Map.Entry<Integer, Integer>> iterator = state.iterator();
        while (iterator.hasNext()) {
            final Map.Entry<Integer, Integer> entry = iterator.next();
            keys.add(entry.getKey());
            assertThat(entry.getValue()).isEqualTo(entry.getKey());
            if (removeEntries) {
                iterator.remove();
            }
        }

        assertThat(keys)
                .containsExactlyElementsOf(
                        IntStream.range(0, MAP_ENTRIES).boxed().collect(Collectors.toList()));
        assertThat(state.isEmpty()).isEqualTo(removeEntries);
        assertNeighborPreserved();
    }

    @Test
    void testClearDoesNotScanDeletedEntriesOfNextKey() throws Exception {
        createNeighborTombstones();
        final MapState<Integer, Integer> state = mapState(0, 0);
        populateMap(state);

        state.clear();

        assertThat(state.isEmpty()).isTrue();
        assertNeighborPreserved();
    }

    /** Isolates the validity guard without requiring prefix bounds or neighboring tombstones. */
    @Test
    void testIteratorCacheReloadDoesNotAdvanceAfterExhaustedSeek() throws Exception {
        assertRocksIteratorAssertionsEnabled();
        final MapState<Integer, Integer> state = mapState(0, 0);

        // One entry beyond the cache ensures that exhaustion requires another seek.
        for (int i = 0; i <= ITERATOR_CACHE_SIZE; i++) {
            state.put(i, i);
        }
        final Iterator<Map.Entry<Integer, Integer>> iterator = state.iterator();
        for (int i = 0; i < ITERATOR_CACHE_SIZE; i++) {
            assertThat(iterator.next().getKey()).isEqualTo(i);
        }

        // MapState.remove() leaves the cached entry's 'deleted' flag false. Remove both the
        // resume entry and the only later entry so the refill seek is exhausted.
        state.remove(ITERATOR_CACHE_SIZE - 1);
        state.remove(ITERATOR_CACHE_SIZE);

        assertThat(iterator).isExhausted();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testIteratorCacheReloadAfterEntriesWereRemoved(boolean removeLastReturnedEntry)
            throws Exception {
        assertRocksIteratorAssertionsEnabled();
        createNeighborTombstones();
        final MapState<Integer, Integer> state = mapState(0, 0);
        populateMap(state);
        final Iterator<Map.Entry<Integer, Integer>> iterator = state.iterator();
        for (int i = 0; i < ITERATOR_CACHE_SIZE; i++) {
            assertThat(iterator.next().getKey()).isEqualTo(i);
        }

        // Removing the last returned entry exhausts the resumed seek at the prefix bound.
        // Keeping it exercises the valid seek followed by an advance to the bound.
        final int firstRemovedKey =
                removeLastReturnedEntry ? ITERATOR_CACHE_SIZE - 1 : ITERATOR_CACHE_SIZE;
        for (int i = firstRemovedKey; i < MAP_ENTRIES; i++) {
            state.remove(i);
        }

        assertThat(iterator).isExhausted();
        assertNeighborPreserved();
    }

    private static void assertRocksIteratorAssertionsEnabled() {
        assertThat(RocksIteratorWrapper.class.desiredAssertionStatus())
                .as("This test requires Java assertions for RocksIteratorWrapper")
                .isTrue();
    }

    private void createNeighborTombstones() throws Exception {
        final MapState<Integer, Integer> neighbor = mapState(1, 0);
        for (int i = 0; i < NEIGHBOR_TOMBSTONES; i++) {
            neighbor.put(i, i);
        }
        flush();
        for (int i = 0; i < NEIGHBOR_TOMBSTONES; i++) {
            neighbor.remove(i);
        }
        flush();

        assertThat(
                        backend.db.getPropertiesOfAllTables(columnFamily).values().stream()
                                .mapToLong(TableProperties::getNumDeletions)
                                .sum())
                .isEqualTo(NEIGHBOR_TOMBSTONES);
        mapState(2, 0).put(NEIGHBOR_TOMBSTONES, NEIGHBOR_TOMBSTONES);
    }

    private void populateMap(MapState<Integer, Integer> state) throws Exception {
        for (int i = 0; i < MAP_ENTRIES; i++) {
            state.put(i, i);
        }
    }

    private void assertNeighborPreserved() throws Exception {
        assertThat(mapState(2, 0).get(NEIGHBOR_TOMBSTONES)).isEqualTo(NEIGHBOR_TOMBSTONES);
    }

    private MapState<Integer, Integer> mapState(int key, int namespace) throws Exception {
        backend.setCurrentKey(key);
        return backend.getPartitionedState(namespace, IntSerializer.INSTANCE, STATE_DESCRIPTOR);
    }

    private void flush() throws Exception {
        try (FlushOptions options = new FlushOptions().setWaitForFlush(true)) {
            backend.db.flush(options, columnFamily);
        }
    }

    private enum EmptyMapOperation {
        ENTRIES,
        IS_EMPTY,
        CLEAR
    }
}
