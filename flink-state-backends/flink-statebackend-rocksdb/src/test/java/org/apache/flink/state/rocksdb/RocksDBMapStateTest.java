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

import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import static org.apache.flink.state.rocksdb.RocksDBMapState.ITERATOR_CACHE_SIZE;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests iteration of RocksDB map state. */
class RocksDBMapStateTest {

    private static final MapStateDescriptor<Integer, Integer> STATE_DESCRIPTOR =
            new MapStateDescriptor<>("map", IntSerializer.INSTANCE, IntSerializer.INSTANCE);

    @TempDir private Path temporaryDirectory;

    private RocksDBKeyedStateBackend<Integer> backend;

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
    }

    @AfterEach
    void tearDown() throws Exception {
        if (backend != null) {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
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

    private static void assertRocksIteratorAssertionsEnabled() {
        assertThat(RocksIteratorWrapper.class.desiredAssertionStatus())
                .as("This test requires Java assertions for RocksIteratorWrapper")
                .isTrue();
    }

    private MapState<Integer, Integer> mapState(int key, int namespace) throws Exception {
        backend.setCurrentKey(key);
        return backend.getPartitionedState(namespace, IntSerializer.INSTANCE, STATE_DESCRIPTOR);
    }
}
