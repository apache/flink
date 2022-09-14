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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.core.testutils.CommonTestUtils;

import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for the common/shared functionality of {@link StateDescriptor}. */
public class StateDescriptorTest {

    // ------------------------------------------------------------------------
    //  Tests for serializer initialization
    // ------------------------------------------------------------------------

    @Test
    public void testInitializeWithSerializer() throws Exception {
        final TypeSerializer<String> serializer = StringSerializer.INSTANCE;
        final TestStateDescriptor<String> descr = new TestStateDescriptor<>("test", serializer);

        assertTrue(descr.isSerializerInitialized());
        assertNotNull(descr.getSerializer());
        assertTrue(descr.getSerializer() instanceof StringSerializer);

        // this should not have any effect
        descr.initializeSerializerUnlessSet(new ExecutionConfig());
        assertTrue(descr.isSerializerInitialized());
        assertNotNull(descr.getSerializer());
        assertTrue(descr.getSerializer() instanceof StringSerializer);

        TestStateDescriptor<String> clone = CommonTestUtils.createCopySerializable(descr);
        assertTrue(clone.isSerializerInitialized());
        assertNotNull(clone.getSerializer());
        assertTrue(clone.getSerializer() instanceof StringSerializer);
    }

    @Test
    public void testInitializeSerializerBeforeSerialization() throws Exception {
        final TestStateDescriptor<String> descr = new TestStateDescriptor<>("test", String.class);

        assertFalse(descr.isSerializerInitialized());
        try {
            descr.getSerializer();
            fail("should fail with an exception");
        } catch (IllegalStateException ignored) {
        }

        descr.initializeSerializerUnlessSet(new ExecutionConfig());

        assertTrue(descr.isSerializerInitialized());
        assertNotNull(descr.getSerializer());
        assertTrue(descr.getSerializer() instanceof StringSerializer);

        TestStateDescriptor<String> clone = CommonTestUtils.createCopySerializable(descr);

        assertTrue(clone.isSerializerInitialized());
        assertNotNull(clone.getSerializer());
        assertTrue(clone.getSerializer() instanceof StringSerializer);
    }

    @Test
    public void testInitializeSerializerAfterSerialization() throws Exception {
        final TestStateDescriptor<String> descr = new TestStateDescriptor<>("test", String.class);

        assertFalse(descr.isSerializerInitialized());
        try {
            descr.getSerializer();
            fail("should fail with an exception");
        } catch (IllegalStateException ignored) {
        }

        TestStateDescriptor<String> clone = CommonTestUtils.createCopySerializable(descr);

        assertFalse(clone.isSerializerInitialized());
        try {
            clone.getSerializer();
            fail("should fail with an exception");
        } catch (IllegalStateException ignored) {
        }

        clone.initializeSerializerUnlessSet(new ExecutionConfig());

        assertTrue(clone.isSerializerInitialized());
        assertNotNull(clone.getSerializer());
        assertTrue(clone.getSerializer() instanceof StringSerializer);
    }

    @Test
    public void testInitializeSerializerAfterSerializationWithCustomConfig() throws Exception {
        // guard our test assumptions.
        assertEquals(
                "broken test assumption",
                -1,
                new KryoSerializer<>(String.class, new ExecutionConfig())
                        .getKryo()
                        .getRegistration(File.class)
                        .getId());

        final ExecutionConfig config = new ExecutionConfig();
        config.registerKryoType(File.class);

        final TestStateDescriptor<Path> original = new TestStateDescriptor<>("test", Path.class);
        TestStateDescriptor<Path> clone = CommonTestUtils.createCopySerializable(original);

        clone.initializeSerializerUnlessSet(config);

        // serialized one (later initialized) carries the registration
        assertTrue(
                ((KryoSerializer<?>) clone.getSerializer())
                                .getKryo()
                                .getRegistration(File.class)
                                .getId()
                        > 0);
    }

    // ------------------------------------------------------------------------
    //  Tests for serializer initialization
    // ------------------------------------------------------------------------

    /**
     * FLINK-6775, tests that the returned serializer is duplicated. This allows to share the state
     * descriptor across threads.
     */
    @Test
    public void testSerializerDuplication() throws Exception {
        // we need a serializer that actually duplicates for testing (a stateful one)
        // we use Kryo here, because it meets these conditions
        TypeSerializer<String> statefulSerializer =
                new KryoSerializer<>(String.class, new ExecutionConfig());

        TestStateDescriptor<String> descr = new TestStateDescriptor<>("foobar", statefulSerializer);

        TypeSerializer<String> serializerA = descr.getSerializer();
        TypeSerializer<String> serializerB = descr.getSerializer();

        // check that the retrieved serializers are not the same
        assertNotSame(serializerA, serializerB);
    }

    // ------------------------------------------------------------------------
    //  Test hashCode() and equals()
    // ------------------------------------------------------------------------

    @Test
    public void testHashCodeAndEquals() throws Exception {
        final String name = "testName";

        TestStateDescriptor<String> original = new TestStateDescriptor<>(name, String.class);
        TestStateDescriptor<String> same = new TestStateDescriptor<>(name, String.class);
        TestStateDescriptor<String> sameBySerializer =
                new TestStateDescriptor<>(name, StringSerializer.INSTANCE);

        // test that hashCode() works on state descriptors with initialized and uninitialized
        // serializers
        assertEquals(original.hashCode(), same.hashCode());
        assertEquals(original.hashCode(), sameBySerializer.hashCode());

        assertEquals(original, same);
        assertEquals(original, sameBySerializer);

        // equality with a clone
        TestStateDescriptor<String> clone = CommonTestUtils.createCopySerializable(original);
        assertEquals(original, clone);

        // equality with an initialized
        clone.initializeSerializerUnlessSet(new ExecutionConfig());
        assertEquals(original, clone);

        original.initializeSerializerUnlessSet(new ExecutionConfig());
        assertEquals(original, same);
    }

    @Test
    public void testEqualsSameNameAndTypeDifferentClass() throws Exception {
        final String name = "test name";

        final TestStateDescriptor<String> descr1 = new TestStateDescriptor<>(name, String.class);
        final OtherTestStateDescriptor<String> descr2 =
                new OtherTestStateDescriptor<>(name, String.class);

        assertNotEquals(descr1, descr2);
    }

    @Test
    public void testSerializerLazyInitializeInParallel() throws Exception {
        final String name = "testSerializerLazyInitializeInParallel";
        // use PojoTypeInfo which will create a new serializer when createSerializer is invoked.
        final TestStateDescriptor<String> desc =
                new TestStateDescriptor<>(
                        name, new PojoTypeInfo<>(String.class, new ArrayList<>()));
        final int threadNumber = 20;
        final ArrayList<CheckedThread> threads = new ArrayList<>(threadNumber);
        final ExecutionConfig executionConfig = new ExecutionConfig();
        final ConcurrentHashMap<Integer, TypeSerializer<String>> serializers =
                new ConcurrentHashMap<>();
        for (int i = 0; i < threadNumber; i++) {
            threads.add(
                    new CheckedThread() {
                        @Override
                        public void go() {
                            desc.initializeSerializerUnlessSet(executionConfig);
                            TypeSerializer<String> serializer = desc.getOriginalSerializer();
                            serializers.put(System.identityHashCode(serializer), serializer);
                        }
                    });
        }
        threads.forEach(Thread::start);
        for (CheckedThread t : threads) {
            t.sync();
        }
        assertEquals(
                "Should use only one serializer but actually: " + serializers,
                1,
                serializers.size());
        threads.clear();
    }

    @Test
    public void testStateTTlConfig() {
        ValueStateDescriptor<Integer> stateDescriptor =
                new ValueStateDescriptor<>("test-state", IntSerializer.INSTANCE);
        stateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.minutes(60)).build());
        assertTrue(stateDescriptor.getTtlConfig().isEnabled());

        stateDescriptor.enableTimeToLive(StateTtlConfig.DISABLED);
        assertFalse(stateDescriptor.getTtlConfig().isEnabled());
    }

    // ------------------------------------------------------------------------
    //  Mock implementations and test types
    // ------------------------------------------------------------------------

    private static class TestStateDescriptor<T> extends StateDescriptor<State, T> {

        private static final long serialVersionUID = 1L;

        TestStateDescriptor(String name, TypeSerializer<T> serializer) {
            super(name, serializer, null);
        }

        TestStateDescriptor(String name, TypeInformation<T> typeInfo) {
            super(name, typeInfo, null);
        }

        TestStateDescriptor(String name, Class<T> type) {
            super(name, type, null);
        }

        @Override
        public Type getType() {
            return Type.VALUE;
        }
    }

    private static class OtherTestStateDescriptor<T> extends StateDescriptor<State, T> {

        private static final long serialVersionUID = 1L;

        OtherTestStateDescriptor(String name, TypeSerializer<T> serializer) {
            super(name, serializer, null);
        }

        OtherTestStateDescriptor(String name, TypeInformation<T> typeInfo) {
            super(name, typeInfo, null);
        }

        OtherTestStateDescriptor(String name, Class<T> type) {
            super(name, type, null);
        }

        @Override
        public Type getType() {
            return Type.VALUE;
        }
    }
}
