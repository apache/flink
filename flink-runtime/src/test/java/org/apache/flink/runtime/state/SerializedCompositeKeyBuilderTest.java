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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

/** Test for @{@link SerializedCompositeKeyBuilder}. */
public class SerializedCompositeKeyBuilderTest {

    private final DataOutputSerializer dataOutputSerializer = new DataOutputSerializer(128);

    private static final int[] TEST_PARALLELISMS = new int[] {64, 4096};
    private static final Collection<Integer> TEST_INTS = Arrays.asList(42, 4711);
    private static final Collection<String> TEST_STRINGS = Arrays.asList("test123", "abc");

    @Before
    public void before() {
        dataOutputSerializer.clear();
    }

    @Test
    public void testSetKey() throws IOException {
        for (int parallelism : TEST_PARALLELISMS) {
            testSetKeyInternal(IntSerializer.INSTANCE, TEST_INTS, parallelism);
            testSetKeyInternal(StringSerializer.INSTANCE, TEST_STRINGS, parallelism);
        }
    }

    @Test
    public void testSetKeyNamespace() throws IOException {
        testSetKeyNamespaceInternal(BuildKeyAndNamespaceType.BUILD);
    }

    @Test
    public void testSetKeyNamespaceWithSet() throws IOException {
        testSetKeyNamespaceInternal(BuildKeyAndNamespaceType.SET_AND_BUILD);
    }

    private void testSetKeyNamespaceInternal(BuildKeyAndNamespaceType buildKeyAndNamespaceType)
            throws IOException {
        for (int parallelism : TEST_PARALLELISMS) {
            testSetKeyNamespaceInternal(
                    IntSerializer.INSTANCE,
                    IntSerializer.INSTANCE,
                    TEST_INTS,
                    TEST_INTS,
                    parallelism,
                    buildKeyAndNamespaceType);
            testSetKeyNamespaceInternal(
                    IntSerializer.INSTANCE,
                    StringSerializer.INSTANCE,
                    TEST_INTS,
                    TEST_STRINGS,
                    parallelism,
                    buildKeyAndNamespaceType);
            testSetKeyNamespaceInternal(
                    StringSerializer.INSTANCE,
                    IntSerializer.INSTANCE,
                    TEST_STRINGS,
                    TEST_INTS,
                    parallelism,
                    buildKeyAndNamespaceType);
            testSetKeyNamespaceInternal(
                    StringSerializer.INSTANCE,
                    StringSerializer.INSTANCE,
                    TEST_STRINGS,
                    TEST_STRINGS,
                    parallelism,
                    buildKeyAndNamespaceType);
        }
    }

    @Test
    public void testSetKeyNamespaceUserKey() throws IOException {
        testSetKeyNamespaceUserKeyInternal(BuildKeyAndNamespaceType.BUILD);
    }

    @Test
    public void testSetKeyNamespaceUserKeyWithSet() throws IOException {
        testSetKeyNamespaceUserKeyInternal(BuildKeyAndNamespaceType.SET_AND_BUILD);
    }

    private void testSetKeyNamespaceUserKeyInternal(
            BuildKeyAndNamespaceType buildKeyAndNamespaceType) throws IOException {
        for (int parallelism : TEST_PARALLELISMS) {
            testSetKeyNamespaceUserKeyInternal(
                    IntSerializer.INSTANCE,
                    IntSerializer.INSTANCE,
                    IntSerializer.INSTANCE,
                    TEST_INTS,
                    TEST_INTS,
                    TEST_INTS,
                    parallelism,
                    buildKeyAndNamespaceType);
            testSetKeyNamespaceUserKeyInternal(
                    IntSerializer.INSTANCE,
                    StringSerializer.INSTANCE,
                    IntSerializer.INSTANCE,
                    TEST_INTS,
                    TEST_STRINGS,
                    TEST_INTS,
                    parallelism,
                    buildKeyAndNamespaceType);
            testSetKeyNamespaceUserKeyInternal(
                    StringSerializer.INSTANCE,
                    IntSerializer.INSTANCE,
                    IntSerializer.INSTANCE,
                    TEST_STRINGS,
                    TEST_INTS,
                    TEST_INTS,
                    parallelism,
                    buildKeyAndNamespaceType);
            testSetKeyNamespaceUserKeyInternal(
                    StringSerializer.INSTANCE,
                    StringSerializer.INSTANCE,
                    IntSerializer.INSTANCE,
                    TEST_STRINGS,
                    TEST_STRINGS,
                    TEST_INTS,
                    parallelism,
                    buildKeyAndNamespaceType);
            testSetKeyNamespaceUserKeyInternal(
                    IntSerializer.INSTANCE,
                    IntSerializer.INSTANCE,
                    StringSerializer.INSTANCE,
                    TEST_INTS,
                    TEST_INTS,
                    TEST_STRINGS,
                    parallelism,
                    buildKeyAndNamespaceType);
            testSetKeyNamespaceUserKeyInternal(
                    IntSerializer.INSTANCE,
                    StringSerializer.INSTANCE,
                    StringSerializer.INSTANCE,
                    TEST_INTS,
                    TEST_STRINGS,
                    TEST_STRINGS,
                    parallelism,
                    buildKeyAndNamespaceType);
            testSetKeyNamespaceUserKeyInternal(
                    StringSerializer.INSTANCE,
                    IntSerializer.INSTANCE,
                    StringSerializer.INSTANCE,
                    TEST_STRINGS,
                    TEST_INTS,
                    TEST_STRINGS,
                    parallelism,
                    buildKeyAndNamespaceType);
            testSetKeyNamespaceUserKeyInternal(
                    StringSerializer.INSTANCE,
                    StringSerializer.INSTANCE,
                    StringSerializer.INSTANCE,
                    TEST_STRINGS,
                    TEST_STRINGS,
                    TEST_STRINGS,
                    parallelism,
                    buildKeyAndNamespaceType);
        }
    }

    private <K> void testSetKeyInternal(
            TypeSerializer<K> serializer, Collection<K> testKeys, int maxParallelism)
            throws IOException {
        final int prefixBytes = maxParallelism > Byte.MAX_VALUE ? 2 : 1;
        SerializedCompositeKeyBuilder<K> keyBuilder =
                createRocksDBSerializedCompositeKeyBuilder(serializer, prefixBytes);

        final DataInputDeserializer deserializer = new DataInputDeserializer();
        for (K testKey : testKeys) {
            int keyGroup = setKeyAndReturnKeyGroup(keyBuilder, testKey, maxParallelism);
            byte[] result = dataOutputSerializer.getCopyOfBuffer();
            deserializer.setBuffer(result);
            assertKeyKeyGroupBytes(testKey, keyGroup, prefixBytes, serializer, deserializer, false);
            Assert.assertEquals(0, deserializer.available());
        }
    }

    enum BuildKeyAndNamespaceType {
        BUILD,
        SET_AND_BUILD
    }

    private <K, N> void testSetKeyNamespaceInternal(
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            Collection<K> testKeys,
            Collection<N> testNamespaces,
            int maxParallelism,
            BuildKeyAndNamespaceType buildKeyAndNamespaceType)
            throws IOException {
        final int prefixBytes = maxParallelism > Byte.MAX_VALUE ? 2 : 1;

        SerializedCompositeKeyBuilder<K> keyBuilder =
                createRocksDBSerializedCompositeKeyBuilder(keySerializer, prefixBytes);

        final DataInputDeserializer deserializer = new DataInputDeserializer();

        final boolean ambiguousPossible =
                keyBuilder.isAmbiguousCompositeKeyPossible(namespaceSerializer);

        for (K testKey : testKeys) {
            int keyGroup = setKeyAndReturnKeyGroup(keyBuilder, testKey, maxParallelism);
            for (N testNamespace : testNamespaces) {
                final byte[] compositeBytes;
                if (buildKeyAndNamespaceType == BuildKeyAndNamespaceType.BUILD) {
                    compositeBytes =
                            keyBuilder.buildCompositeKeyNamespace(
                                    testNamespace, namespaceSerializer);
                } else {
                    keyBuilder.setNamespace(testNamespace, namespaceSerializer);
                    compositeBytes = keyBuilder.build();
                }
                deserializer.setBuffer(compositeBytes);
                assertKeyGroupKeyNamespaceBytes(
                        testKey,
                        keyGroup,
                        prefixBytes,
                        keySerializer,
                        testNamespace,
                        namespaceSerializer,
                        deserializer,
                        ambiguousPossible);
                Assert.assertEquals(0, deserializer.available());
            }
        }
    }

    private <K, N, U> void testSetKeyNamespaceUserKeyInternal(
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<U> userKeySerializer,
            Collection<K> testKeys,
            Collection<N> testNamespaces,
            Collection<U> testUserKeys,
            int maxParallelism,
            BuildKeyAndNamespaceType buildKeyAndNamespaceType)
            throws IOException {
        final int prefixBytes = maxParallelism > Byte.MAX_VALUE ? 2 : 1;

        SerializedCompositeKeyBuilder<K> keyBuilder =
                createRocksDBSerializedCompositeKeyBuilder(keySerializer, prefixBytes);

        final DataInputDeserializer deserializer = new DataInputDeserializer();

        final boolean ambiguousPossible =
                keyBuilder.isAmbiguousCompositeKeyPossible(namespaceSerializer);

        for (K testKey : testKeys) {
            int keyGroup = setKeyAndReturnKeyGroup(keyBuilder, testKey, maxParallelism);
            for (N testNamespace : testNamespaces) {
                if (buildKeyAndNamespaceType == BuildKeyAndNamespaceType.SET_AND_BUILD) {
                    keyBuilder.setNamespace(testNamespace, namespaceSerializer);
                }
                for (U testUserKey : testUserKeys) {
                    final byte[] compositeBytes;
                    if (buildKeyAndNamespaceType == BuildKeyAndNamespaceType.BUILD) {
                        compositeBytes =
                                keyBuilder.buildCompositeKeyNamesSpaceUserKey(
                                        testNamespace,
                                        namespaceSerializer,
                                        testUserKey,
                                        userKeySerializer);
                    } else {
                        compositeBytes =
                                keyBuilder.buildCompositeKeyUserKey(testUserKey, userKeySerializer);
                    }

                    deserializer.setBuffer(compositeBytes);
                    assertKeyGroupKeyNamespaceUserKeyBytes(
                            testKey,
                            keyGroup,
                            prefixBytes,
                            keySerializer,
                            testNamespace,
                            namespaceSerializer,
                            testUserKey,
                            userKeySerializer,
                            deserializer,
                            ambiguousPossible);

                    Assert.assertEquals(0, deserializer.available());
                }
            }
        }
    }

    private <K> SerializedCompositeKeyBuilder<K> createRocksDBSerializedCompositeKeyBuilder(
            TypeSerializer<K> serializer, int prefixBytes) {
        final boolean variableSize =
                CompositeKeySerializationUtils.isSerializerTypeVariableSized(serializer);
        return new SerializedCompositeKeyBuilder<>(
                serializer, dataOutputSerializer, prefixBytes, variableSize, 0);
    }

    private <K> int setKeyAndReturnKeyGroup(
            SerializedCompositeKeyBuilder<K> compositeKeyBuilder, K key, int maxParallelism) {

        int keyGroup =
                KeyGroupRangeAssignment.assignKeyToParallelOperator(
                        key, maxParallelism, maxParallelism);
        compositeKeyBuilder.setKeyAndKeyGroup(key, keyGroup);
        return keyGroup;
    }

    private <K> void assertKeyKeyGroupBytes(
            K key,
            int keyGroup,
            int prefixBytes,
            TypeSerializer<K> typeSerializer,
            DataInputDeserializer deserializer,
            boolean ambiguousCompositeKeyPossible)
            throws IOException {

        Assert.assertEquals(
                keyGroup, CompositeKeySerializationUtils.readKeyGroup(prefixBytes, deserializer));
        Assert.assertEquals(
                key,
                CompositeKeySerializationUtils.readKey(
                        typeSerializer, deserializer, ambiguousCompositeKeyPossible));
    }

    private <K, N> void assertKeyGroupKeyNamespaceBytes(
            K key,
            int keyGroup,
            int prefixBytes,
            TypeSerializer<K> keySerializer,
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            DataInputDeserializer deserializer,
            boolean ambiguousCompositeKeyPossible)
            throws IOException {
        assertKeyKeyGroupBytes(
                key,
                keyGroup,
                prefixBytes,
                keySerializer,
                deserializer,
                ambiguousCompositeKeyPossible);
        N readNamespace =
                CompositeKeySerializationUtils.readNamespace(
                        namespaceSerializer, deserializer, ambiguousCompositeKeyPossible);
        Assert.assertEquals(namespace, readNamespace);
    }

    private <K, N, U> void assertKeyGroupKeyNamespaceUserKeyBytes(
            K key,
            int keyGroup,
            int prefixBytes,
            TypeSerializer<K> keySerializer,
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            U userKey,
            TypeSerializer<U> userKeySerializer,
            DataInputDeserializer deserializer,
            boolean ambiguousCompositeKeyPossible)
            throws IOException {
        assertKeyGroupKeyNamespaceBytes(
                key,
                keyGroup,
                prefixBytes,
                keySerializer,
                namespace,
                namespaceSerializer,
                deserializer,
                ambiguousCompositeKeyPossible);
        Assert.assertEquals(userKey, userKeySerializer.deserialize(deserializer));
    }
}
