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

package org.apache.flink.state.rocksdb;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.state.rocksdb.iterator.RocksStateKeysAndNamespaceIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ColumnFamilyHandle;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the RocksDBRocksStateKeysAndNamespacesIterator. */
class RocksDBRocksStateKeysAndNamespacesIteratorTest {

    @TempDir private Path tmp;

    @Test
    void testIterator() throws Exception {

        // test for keyGroupPrefixBytes == 1 && ambiguousKeyPossible == false
        testIteratorHelper(IntSerializer.INSTANCE, 128, i -> i);

        // test for keyGroupPrefixBytes == 1 && ambiguousKeyPossible == true
        testIteratorHelper(StringSerializer.INSTANCE, 128, String::valueOf);

        // test for keyGroupPrefixBytes == 2 && ambiguousKeyPossible == false
        testIteratorHelper(IntSerializer.INSTANCE, 256, i -> i);

        // test for keyGroupPrefixBytes == 2 && ambiguousKeyPossible == true
        testIteratorHelper(StringSerializer.INSTANCE, 256, String::valueOf);
    }

    @SuppressWarnings("unchecked")
    <K> void testIteratorHelper(
            TypeSerializer<K> keySerializer, int maxKeyGroupNumber, Function<Integer, K> getKeyFunc)
            throws Exception {

        String testStateName = "aha";
        String namespace = "ns";

        try (RocksDBKeyedStateBackendTestFactory factory =
                new RocksDBKeyedStateBackendTestFactory()) {
            RocksDBKeyedStateBackend<K> keyedStateBackend =
                    factory.create(tmp, keySerializer, maxKeyGroupNumber);

            ValueState<String> testState =
                    keyedStateBackend.getPartitionedState(
                            namespace,
                            StringSerializer.INSTANCE,
                            new ValueStateDescriptor<>(testStateName, String.class));

            // insert record
            for (int i = 0; i < 1000; ++i) {
                keyedStateBackend.setCurrentKey(getKeyFunc.apply(i));
                testState.update(String.valueOf(i));
            }

            DataOutputSerializer outputStream = new DataOutputSerializer(8);
            boolean ambiguousKeyPossible =
                    CompositeKeySerializationUtils.isAmbiguousKeyPossible(
                            keySerializer, StringSerializer.INSTANCE);
            CompositeKeySerializationUtils.writeNameSpace(
                    namespace, StringSerializer.INSTANCE, outputStream, ambiguousKeyPossible);

            // already created with the state, should be closed with the backend
            ColumnFamilyHandle handle = keyedStateBackend.getColumnFamilyHandle(testStateName);

            try (RocksIteratorWrapper iterator =
                            RocksDBOperationUtils.getRocksIterator(
                                    keyedStateBackend.db,
                                    handle,
                                    keyedStateBackend.getReadOptions());
                    RocksStateKeysAndNamespaceIterator<K, String> iteratorWrapper =
                            new RocksStateKeysAndNamespaceIterator<>(
                                    iterator,
                                    testStateName,
                                    keySerializer,
                                    StringSerializer.INSTANCE,
                                    keyedStateBackend.getKeyGroupPrefixBytes(),
                                    ambiguousKeyPossible)) {

                iterator.seekToFirst();

                // valid record
                List<Tuple2<Integer, String>> fetchedKeys = new ArrayList<>(1000);
                while (iteratorWrapper.hasNext()) {
                    Tuple2 entry = iteratorWrapper.next();
                    entry.f0 = Integer.parseInt(entry.f0.toString());

                    fetchedKeys.add((Tuple2<Integer, String>) entry);
                }

                assertThat(fetchedKeys)
                        .containsExactlyInAnyOrderElementsOf(
                                IntStream.range(0, 1000)
                                        .mapToObj(i -> Tuple2.of(i, namespace))
                                        .collect(Collectors.toList()));
            }
        }
    }
}
