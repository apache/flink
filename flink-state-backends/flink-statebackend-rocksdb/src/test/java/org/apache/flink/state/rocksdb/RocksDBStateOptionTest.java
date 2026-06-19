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

package org.apache.flink.state.rocksdb;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend.PriorityQueueStateType;
import org.apache.flink.streaming.api.operators.TimerHeapInternalTimer;
import org.apache.flink.streaming.api.operators.TimerSerializer;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.state.rocksdb.RocksDBTestUtils.createKeyedStateBackend;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests to cover cases that if user choose options previously prone to misuse, embedded RocksDB
 * state-backend could still work as expected or give explicit feedback.
 *
 * <p>RocksDB state-backend has some internal operations based on RocksDB's APIs which is
 * transparent for users. However, user could still configure options via {@link
 * RocksDBOptionsFactory}, and might lead some operations could not get expected result, e.g.
 * FLINK-17800
 */
public class RocksDBStateOptionTest {

    @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

    /**
     * Tests to cover case when user choose optimizeForPointLookup with iterator interfaces on map
     * state.
     */
    @Test
    public void testUseOptimizePointLookupWithMapState() throws Exception {
        EmbeddedRocksDBStateBackend rocksDBStateBackend =
                createStateBackendWithOptimizePointLookup();
        RocksDBKeyedStateBackend<Integer> keyedStateBackend =
                createKeyedStateBackend(
                        rocksDBStateBackend,
                        new MockEnvironmentBuilder().build(),
                        IntSerializer.INSTANCE);
        try {
            MapStateDescriptor<Integer, Long> stateDescriptor =
                    new MapStateDescriptor<>(
                            "map", IntSerializer.INSTANCE, LongSerializer.INSTANCE);
            MapState<Integer, Long> mapState =
                    keyedStateBackend.getPartitionedState(
                            VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            stateDescriptor);

            keyedStateBackend.setCurrentKey(1);
            Map<Integer, Long> expectedResult = new HashMap<>();
            for (int i = 0; i < 100; i++) {
                long uv = ThreadLocalRandom.current().nextLong();
                mapState.put(i, uv);
                expectedResult.put(i, uv);
            }

            Iterator<Map.Entry<Integer, Long>> iterator = mapState.entries().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Integer, Long> entry = iterator.next();
                assertEquals(entry.getValue(), expectedResult.remove(entry.getKey()));
                iterator.remove();
            }
            assertTrue(expectedResult.isEmpty());
            assertTrue(mapState.isEmpty());
        } finally {
            keyedStateBackend.dispose();
        }
    }

    /**
     * Tests to cover case when user choose optimizeForPointLookup with peek operations on priority
     * queue.
     */
    @Test
    public void testUseOptimizePointLookupWithPriorityQueue() throws IOException {
        EmbeddedRocksDBStateBackend rocksDBStateBackend =
                createStateBackendWithOptimizePointLookup();
        RocksDBKeyedStateBackend<Integer> keyedStateBackend =
                createKeyedStateBackend(
                        rocksDBStateBackend,
                        new MockEnvironmentBuilder().build(),
                        IntSerializer.INSTANCE);
        try {
            KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<Integer, VoidNamespace>>
                    priorityQueue =
                            keyedStateBackend.create(
                                    "timer",
                                    new TimerSerializer<>(
                                            keyedStateBackend.getKeySerializer(),
                                            VoidNamespaceSerializer.INSTANCE));

            PriorityQueue<TimerHeapInternalTimer<Integer, VoidNamespace>> expectedPriorityQueue =
                    new PriorityQueue<>((o1, o2) -> (int) (o1.getTimestamp() - o2.getTimestamp()));
            // ensure we insert timers more than cache capacity.
            assertTrue(
                    keyedStateBackend.getPriorityQueueFactory()
                            instanceof RocksDBPriorityQueueSetFactory);
            int queueSize =
                    ((RocksDBPriorityQueueSetFactory) keyedStateBackend.getPriorityQueueFactory())
                                    .getCacheSize()
                            + 42;
            List<Integer> timeStamps =
                    IntStream.range(0, queueSize).boxed().collect(Collectors.toList());
            Collections.shuffle(timeStamps);
            for (Integer timeStamp : timeStamps) {
                TimerHeapInternalTimer<Integer, VoidNamespace> timer =
                        new TimerHeapInternalTimer<>(timeStamp, timeStamp, VoidNamespace.INSTANCE);
                priorityQueue.add(timer);
                expectedPriorityQueue.add(timer);
            }
            assertEquals(queueSize, priorityQueue.size());
            TimerHeapInternalTimer<Integer, VoidNamespace> timer;
            while ((timer = priorityQueue.poll()) != null) {
                assertEquals(expectedPriorityQueue.poll(), timer);
            }
            assertTrue(expectedPriorityQueue.isEmpty());
            assertTrue(priorityQueue.isEmpty());
        } finally {
            keyedStateBackend.dispose();
        }
    }

    private EmbeddedRocksDBStateBackend createStateBackendWithOptimizePointLookup()
            throws IOException {
        EmbeddedRocksDBStateBackend rocksDBStateBackend = new EmbeddedRocksDBStateBackend(true);
        rocksDBStateBackend.setPriorityQueueStateType(PriorityQueueStateType.ROCKSDB);
        rocksDBStateBackend.setRocksDBOptions(
                new RocksDBOptionsFactory() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public DBOptions createDBOptions(
                            DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
                        return currentOptions;
                    }

                    @Override
                    public ColumnFamilyOptions createColumnOptions(
                            ColumnFamilyOptions currentOptions,
                            Collection<AutoCloseable> handlesToClose) {
                        return currentOptions.optimizeForPointLookup(64);
                    }
                });
        return rocksDBStateBackend;
    }
}
