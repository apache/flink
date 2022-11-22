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

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.operators.Keys.ExpressionKeys;
import org.apache.flink.api.common.operators.Keys.IncompatibleKeysException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType.FlatFieldDescriptor;
import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotSerializationUtil;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/** A test for the {@link PojoSerializer}. */
class PojoSerializerTest extends SerializerTestBase<PojoSerializerTest.TestUserClass> {
    private TypeInformation<TestUserClass> type = TypeExtractor.getForClass(TestUserClass.class);

    @Override
    protected TypeSerializer<TestUserClass> createSerializer() {
        TypeSerializer<TestUserClass> serializer = type.createSerializer(new ExecutionConfig());
        assert (serializer instanceof PojoSerializer);
        return serializer;
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected Class<TestUserClass> getTypeClass() {
        return TestUserClass.class;
    }

    @Override
    protected TestUserClass[] getTestData() {
        Random rnd = new Random(874597969123412341L);

        return new TestUserClass[] {
            new TestUserClass(
                    rnd.nextInt(),
                    "foo",
                    rnd.nextDouble(),
                    new int[] {1, 2, 3},
                    new Date(),
                    new NestedTestUserClass(
                            rnd.nextInt(), "foo@boo", rnd.nextDouble(), new int[] {10, 11, 12})),
            new TestUserClass(
                    rnd.nextInt(),
                    "bar",
                    rnd.nextDouble(),
                    new int[] {4, 5, 6},
                    null,
                    new NestedTestUserClass(
                            rnd.nextInt(), "bar@bas", rnd.nextDouble(), new int[] {20, 21, 22})),
            new TestUserClass(rnd.nextInt(), null, rnd.nextDouble(), null, null, null),
            new TestUserClass(
                    rnd.nextInt(),
                    "bar",
                    rnd.nextDouble(),
                    new int[] {4, 5, 6},
                    new Date(),
                    new NestedTestUserClass(
                            rnd.nextInt(), "bar@bas", rnd.nextDouble(), new int[] {20, 21, 22}))
        };
    }

    // User code class for testing the serializer
    public static class TestUserClass {
        public int dumm1;
        public String dumm2;
        public double dumm3;
        public int[] dumm4;
        public Date dumm5;

        public NestedTestUserClass nestedClass;

        public TestUserClass() {}

        public TestUserClass(
                int dumm1,
                String dumm2,
                double dumm3,
                int[] dumm4,
                Date dumm5,
                NestedTestUserClass nestedClass) {
            this.dumm1 = dumm1;
            this.dumm2 = dumm2;
            this.dumm3 = dumm3;
            this.dumm4 = dumm4;
            this.dumm5 = dumm5;
            this.nestedClass = nestedClass;
        }

        @Override
        public int hashCode() {
            return Objects.hash(dumm1, dumm2, dumm3, dumm4, dumm5, nestedClass);
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof TestUserClass)) {
                return false;
            }
            TestUserClass otherTUC = (TestUserClass) other;
            if (dumm1 != otherTUC.dumm1) {
                return false;
            }
            if ((dumm2 == null && otherTUC.dumm2 != null)
                    || (dumm2 != null && !dumm2.equals(otherTUC.dumm2))) {
                return false;
            }
            if (dumm3 != otherTUC.dumm3) {
                return false;
            }
            if ((dumm4 != null && otherTUC.dumm4 == null)
                    || (dumm4 == null && otherTUC.dumm4 != null)
                    || (dumm4 != null
                            && otherTUC.dumm4 != null
                            && dumm4.length != otherTUC.dumm4.length)) {
                return false;
            }
            if (dumm4 != null && otherTUC.dumm4 != null) {
                for (int i = 0; i < dumm4.length; i++) {
                    if (dumm4[i] != otherTUC.dumm4[i]) {
                        return false;
                    }
                }
            }
            if ((dumm5 == null && otherTUC.dumm5 != null)
                    || (dumm5 != null && !dumm5.equals(otherTUC.dumm5))) {
                return false;
            }

            if ((nestedClass == null && otherTUC.nestedClass != null)
                    || (nestedClass != null && !nestedClass.equals(otherTUC.nestedClass))) {
                return false;
            }
            return true;
        }
    }

    public static class NestedTestUserClass {
        public int dumm1;
        public String dumm2;
        public double dumm3;
        public int[] dumm4;

        public NestedTestUserClass() {}

        public NestedTestUserClass(int dumm1, String dumm2, double dumm3, int[] dumm4) {
            this.dumm1 = dumm1;
            this.dumm2 = dumm2;
            this.dumm3 = dumm3;
            this.dumm4 = dumm4;
        }

        @Override
        public int hashCode() {
            return Objects.hash(dumm1, dumm2, dumm3, dumm4);
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof NestedTestUserClass)) {
                return false;
            }
            NestedTestUserClass otherTUC = (NestedTestUserClass) other;
            if (dumm1 != otherTUC.dumm1) {
                return false;
            }
            if (!dumm2.equals(otherTUC.dumm2)) {
                return false;
            }
            if (dumm3 != otherTUC.dumm3) {
                return false;
            }
            if (dumm4.length != otherTUC.dumm4.length) {
                return false;
            }
            for (int i = 0; i < dumm4.length; i++) {
                if (dumm4[i] != otherTUC.dumm4[i]) {
                    return false;
                }
            }
            return true;
        }
    }

    public static class SubTestUserClassA extends TestUserClass {
        public int subDumm1;
        public String subDumm2;

        public SubTestUserClassA() {}
    }

    public static class SubTestUserClassB extends TestUserClass {
        public Double subDumm1;
        public float subDumm2;

        public SubTestUserClassB() {}
    }

    /** This tests if the hashes returned by the pojo and tuple comparators are the same */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    void testTuplePojoTestEquality() throws IncompatibleKeysException {

        // test with a simple, string-key first.
        PojoTypeInfo<TestUserClass> pType = (PojoTypeInfo<TestUserClass>) type;
        List<FlatFieldDescriptor> result = new ArrayList<FlatFieldDescriptor>();
        pType.getFlatFields("nestedClass.dumm2", 0, result);
        int[] fields = new int[1]; // see below
        fields[0] = result.get(0).getPosition();
        TypeComparator<TestUserClass> pojoComp =
                pType.createComparator(fields, new boolean[] {true}, 0, new ExecutionConfig());

        TestUserClass pojoTestRecord =
                new TestUserClass(
                        0,
                        "abc",
                        3d,
                        new int[] {1, 2, 3},
                        new Date(),
                        new NestedTestUserClass(1, "haha", 4d, new int[] {5, 4, 3}));
        int pHash = pojoComp.hash(pojoTestRecord);

        Tuple1<String> tupleTest = new Tuple1<String>("haha");
        TupleTypeInfo<Tuple1<String>> tType =
                (TupleTypeInfo<Tuple1<String>>) TypeExtractor.getForObject(tupleTest);
        TypeComparator<Tuple1<String>> tupleComp =
                tType.createComparator(
                        new int[] {0}, new boolean[] {true}, 0, new ExecutionConfig());

        int tHash = tupleComp.hash(tupleTest);

        assertThat(tHash)
                .isEqualTo(pHash)
                .withFailMessage(
                        "The hashing for tuples and pojos must be the same, so that they are mixable");

        Tuple3<Integer, String, Double> multiTupleTest =
                new Tuple3<Integer, String, Double>(
                        1, "haha", 4d); // its important here to use the same values.
        TupleTypeInfo<Tuple3<Integer, String, Double>> multiTupleType =
                (TupleTypeInfo<Tuple3<Integer, String, Double>>)
                        TypeExtractor.getForObject(multiTupleTest);

        ExpressionKeys fieldKey = new ExpressionKeys(new int[] {1, 0, 2}, multiTupleType);
        ExpressionKeys expressKey =
                new ExpressionKeys(
                        new String[] {
                            "nestedClass.dumm2", "nestedClass.dumm1", "nestedClass.dumm3"
                        },
                        pType);

        assertThat(fieldKey.areCompatible(expressKey))
                .isTrue()
                .withFailMessage("Expecting the keys to be compatible");
        TypeComparator<TestUserClass> multiPojoComp =
                pType.createComparator(
                        expressKey.computeLogicalKeyPositions(),
                        new boolean[] {true, true, true},
                        0,
                        new ExecutionConfig());
        int multiPojoHash = multiPojoComp.hash(pojoTestRecord);

        // pojo order is: dumm2 (str), dumm1 (int), dumm3 (double).
        TypeComparator<Tuple3<Integer, String, Double>> multiTupleComp =
                multiTupleType.createComparator(
                        fieldKey.computeLogicalKeyPositions(),
                        new boolean[] {true, true, true},
                        0,
                        new ExecutionConfig());
        int multiTupleHash = multiTupleComp.hash(multiTupleTest);

        assertThat(multiPojoHash)
                .isEqualTo(multiTupleHash)
                .withFailMessage(
                        "The hashing for tuples and pojos must be the same, so that they are mixable. Also for those with multiple key fields");
    }

    // --------------------------------------------------------------------------------------------
    // Configuration snapshotting & reconfiguring tests
    // --------------------------------------------------------------------------------------------

    /**
     * Verifies that reconfiguring with a config snapshot of a preceding POJO serializer with
     * different POJO type will result in INCOMPATIBLE.
     */
    @Test
    void testReconfigureWithDifferentPojoType() throws Exception {
        PojoSerializer<SubTestUserClassB> pojoSerializer1 =
                (PojoSerializer<SubTestUserClassB>)
                        TypeExtractor.getForClass(SubTestUserClassB.class)
                                .createSerializer(new ExecutionConfig());

        // snapshot configuration and serialize to bytes
        TypeSerializerSnapshot pojoSerializerConfigSnapshot =
                pojoSerializer1.snapshotConfiguration();
        byte[] serializedConfig;
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(
                    new DataOutputViewStreamWrapper(out), pojoSerializerConfigSnapshot);
            serializedConfig = out.toByteArray();
        }

        PojoSerializer<SubTestUserClassA> pojoSerializer2 =
                (PojoSerializer<SubTestUserClassA>)
                        TypeExtractor.getForClass(SubTestUserClassA.class)
                                .createSerializer(new ExecutionConfig());

        // read configuration again from bytes
        try (ByteArrayInputStream in = new ByteArrayInputStream(serializedConfig)) {
            pojoSerializerConfigSnapshot =
                    TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(
                            new DataInputViewStreamWrapper(in),
                            Thread.currentThread().getContextClassLoader());
        }

        @SuppressWarnings("unchecked")
        TypeSerializerSchemaCompatibility<SubTestUserClassA> compatResult =
                pojoSerializerConfigSnapshot.resolveSchemaCompatibility(pojoSerializer2);
        assertThat(compatResult.isIncompatible()).isTrue();
    }

    /**
     * Tests that reconfiguration correctly reorders subclass registrations to their previous order.
     */
    @Test
    void testReconfigureDifferentSubclassRegistrationOrder() throws Exception {
        ExecutionConfig executionConfig = new ExecutionConfig();
        executionConfig.registerPojoType(SubTestUserClassA.class);
        executionConfig.registerPojoType(SubTestUserClassB.class);

        PojoSerializer<TestUserClass> pojoSerializer =
                (PojoSerializer<TestUserClass>) type.createSerializer(executionConfig);

        // get original registration ids
        int subClassATag = pojoSerializer.getRegisteredClasses().get(SubTestUserClassA.class);
        int subClassBTag = pojoSerializer.getRegisteredClasses().get(SubTestUserClassB.class);

        // snapshot configuration and serialize to bytes
        TypeSerializerSnapshot pojoSerializerConfigSnapshot =
                pojoSerializer.snapshotConfiguration();
        byte[] serializedConfig;
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(
                    new DataOutputViewStreamWrapper(out), pojoSerializerConfigSnapshot);
            serializedConfig = out.toByteArray();
        }

        // use new config and instantiate new PojoSerializer
        executionConfig = new ExecutionConfig();
        executionConfig.registerPojoType(
                SubTestUserClassB.class); // test with B registered before A
        executionConfig.registerPojoType(SubTestUserClassA.class);

        pojoSerializer = (PojoSerializer<TestUserClass>) type.createSerializer(executionConfig);

        // read configuration from bytes
        try (ByteArrayInputStream in = new ByteArrayInputStream(serializedConfig)) {
            pojoSerializerConfigSnapshot =
                    TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(
                            new DataInputViewStreamWrapper(in),
                            Thread.currentThread().getContextClassLoader());
        }

        @SuppressWarnings("unchecked")
        TypeSerializerSchemaCompatibility<TestUserClass> compatResult =
                pojoSerializerConfigSnapshot.resolveSchemaCompatibility(pojoSerializer);
        assertThat(compatResult.isCompatibleWithReconfiguredSerializer()).isTrue();
        assertThat(compatResult.getReconfiguredSerializer()).isInstanceOf(PojoSerializer.class);

        // reconfigure - check reconfiguration result and that registration ids remains the same
        // assertEquals(ReconfigureResult.COMPATIBLE,
        // pojoSerializer.reconfigure(pojoSerializerConfigSnapshot));
        PojoSerializer<TestUserClass> reconfiguredPojoSerializer =
                (PojoSerializer<TestUserClass>) compatResult.getReconfiguredSerializer();
        assertThat(subClassATag)
                .isEqualTo(
                        reconfiguredPojoSerializer
                                .getRegisteredClasses()
                                .get(SubTestUserClassA.class)
                                .intValue());
        assertThat(subClassBTag)
                .isEqualTo(
                        reconfiguredPojoSerializer
                                .getRegisteredClasses()
                                .get(SubTestUserClassB.class)
                                .intValue());
    }

    /** Tests that reconfiguration repopulates previously cached subclass serializers. */
    @Test
    void testReconfigureRepopulateNonregisteredSubclassSerializerCache() throws Exception {
        // don't register any subclasses
        PojoSerializer<TestUserClass> pojoSerializer =
                (PojoSerializer<TestUserClass>) type.createSerializer(new ExecutionConfig());

        // create cached serializers for SubTestUserClassA and SubTestUserClassB
        pojoSerializer.getSubclassSerializer(SubTestUserClassA.class);
        pojoSerializer.getSubclassSerializer(SubTestUserClassB.class);

        assertThat(pojoSerializer.getSubclassSerializerCache())
                .containsOnlyKeys(SubTestUserClassA.class, SubTestUserClassB.class);

        // snapshot configuration and serialize to bytes
        TypeSerializerSnapshot pojoSerializerConfigSnapshot =
                pojoSerializer.snapshotConfiguration();
        byte[] serializedConfig;
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(
                    new DataOutputViewStreamWrapper(out), pojoSerializerConfigSnapshot);
            serializedConfig = out.toByteArray();
        }

        // instantiate new PojoSerializer

        pojoSerializer =
                (PojoSerializer<TestUserClass>) type.createSerializer(new ExecutionConfig());

        // read configuration from bytes
        try (ByteArrayInputStream in = new ByteArrayInputStream(serializedConfig)) {
            pojoSerializerConfigSnapshot =
                    TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(
                            new DataInputViewStreamWrapper(in),
                            Thread.currentThread().getContextClassLoader());
        }

        // reconfigure - check reconfiguration result and that subclass serializer cache is
        // repopulated
        @SuppressWarnings("unchecked")
        TypeSerializerSchemaCompatibility<TestUserClass> compatResult =
                pojoSerializerConfigSnapshot.resolveSchemaCompatibility(pojoSerializer);
        assertThat(compatResult.isCompatibleWithReconfiguredSerializer()).isTrue();
        assertThat(compatResult.getReconfiguredSerializer()).isInstanceOf(PojoSerializer.class);

        PojoSerializer<TestUserClass> reconfiguredPojoSerializer =
                (PojoSerializer<TestUserClass>) compatResult.getReconfiguredSerializer();

        assertThat(reconfiguredPojoSerializer.getSubclassSerializerCache())
                .containsOnlyKeys(SubTestUserClassA.class, SubTestUserClassB.class);
    }

    /**
     * Tests that: - Previous Pojo serializer did not have registrations, and created cached
     * serializers for subclasses - On restore, it had those subclasses registered
     *
     * <p>In this case, after reconfiguration, the cache should be repopulated, and registrations
     * should also exist for the subclasses.
     *
     * <p>Note: the cache still needs to be repopulated because previous data of those subclasses
     * were written with the cached serializers. In this case, the repopulated cache has
     * reconfigured serializers for the subclasses so that previous written data can be read, but
     * the registered serializers for the subclasses do not necessarily need to be reconfigured
     * since they will only be used to write new data.
     */
    @Test
    void testReconfigureWithPreviouslyNonregisteredSubclasses() throws Exception {
        // don't register any subclasses at first
        PojoSerializer<TestUserClass> pojoSerializer =
                (PojoSerializer<TestUserClass>) type.createSerializer(new ExecutionConfig());

        // create cached serializers for SubTestUserClassA and SubTestUserClassB
        pojoSerializer.getSubclassSerializer(SubTestUserClassA.class);
        pojoSerializer.getSubclassSerializer(SubTestUserClassB.class);

        // make sure serializers are in cache
        assertThat(pojoSerializer.getSubclassSerializerCache())
                .containsOnlyKeys(SubTestUserClassA.class, SubTestUserClassB.class);

        // make sure that registrations are empty
        assertThat(pojoSerializer.getRegisteredClasses()).isEmpty();
        assertThat(pojoSerializer.getRegisteredSerializers()).isEmpty();

        // snapshot configuration and serialize to bytes
        TypeSerializerSnapshot pojoSerializerConfigSnapshot =
                pojoSerializer.snapshotConfiguration();
        byte[] serializedConfig;
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(
                    new DataOutputViewStreamWrapper(out), pojoSerializerConfigSnapshot);
            serializedConfig = out.toByteArray();
        }

        // instantiate new PojoSerializer, with new execution config that has the subclass
        // registrations
        ExecutionConfig newExecutionConfig = new ExecutionConfig();
        newExecutionConfig.registerPojoType(SubTestUserClassA.class);
        newExecutionConfig.registerPojoType(SubTestUserClassB.class);
        pojoSerializer = (PojoSerializer<TestUserClass>) type.createSerializer(newExecutionConfig);

        // read configuration from bytes
        try (ByteArrayInputStream in = new ByteArrayInputStream(serializedConfig)) {
            pojoSerializerConfigSnapshot =
                    TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(
                            new DataInputViewStreamWrapper(in),
                            Thread.currentThread().getContextClassLoader());
        }

        // reconfigure - check reconfiguration result and that
        // 1) subclass serializer cache is repopulated
        // 2) registrations also contain the now registered subclasses
        @SuppressWarnings("unchecked")
        TypeSerializerSchemaCompatibility<TestUserClass> compatResult =
                pojoSerializerConfigSnapshot.resolveSchemaCompatibility(pojoSerializer);
        assertThat(compatResult.isCompatibleWithReconfiguredSerializer()).isTrue();
        assertThat(compatResult.getReconfiguredSerializer()).isInstanceOf(PojoSerializer.class);

        PojoSerializer<TestUserClass> reconfiguredPojoSerializer =
                (PojoSerializer<TestUserClass>) compatResult.getReconfiguredSerializer();

        assertThat(reconfiguredPojoSerializer.getSubclassSerializerCache())
                .containsOnlyKeys(SubTestUserClassA.class, SubTestUserClassB.class);

        assertThat(reconfiguredPojoSerializer.getRegisteredClasses())
                .containsOnlyKeys(SubTestUserClassA.class, SubTestUserClassB.class);
    }
}
