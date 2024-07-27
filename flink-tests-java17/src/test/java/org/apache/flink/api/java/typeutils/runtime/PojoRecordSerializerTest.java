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
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType.FlatFieldDescriptor;
import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/** A test for the {@link PojoSerializer} with Java Records. */
class PojoRecordSerializerTest extends SerializerTestBase<PojoRecordSerializerTest.TestUserClass> {
    private final TypeInformation<TestUserClass> type =
            TypeExtractor.getForClass(TestUserClass.class);

    @Override
    protected TypeSerializer<TestUserClass> createSerializer() {
        TypeSerializer<TestUserClass> serializer =
                type.createSerializer(new SerializerConfigImpl());
        assertThat(serializer).isInstanceOf(PojoSerializer.class);
        return serializer;
    }

    @Override
    protected boolean allowNullInstances(TypeSerializer<TestUserClass> serializer) {
        return true;
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
                    new Date(),
                    new NestedTestUserClass(
                            rnd.nextInt(), "foo@boo", rnd.nextDouble(), new int[] {10, 11, 12})),
            new TestUserClass(
                    rnd.nextInt(),
                    "bar",
                    rnd.nextDouble(),
                    null,
                    new NestedTestUserClass(
                            rnd.nextInt(), "bar@bas", rnd.nextDouble(), new int[] {20, 21, 22})),
            new TestUserClass(rnd.nextInt(), null, rnd.nextDouble(), null, null),
            new TestUserClass(
                    rnd.nextInt(),
                    "bar",
                    rnd.nextDouble(),
                    new Date(),
                    new NestedTestUserClass(
                            rnd.nextInt(), "bar@bas", rnd.nextDouble(), new int[] {20, 21, 22}))
        };
    }

    // User code class for testing the serializer
    public record TestUserClass(
            int dumm1, String dumm2, double dumm3, Date dumm5, NestedTestUserClass nestedClass) {}

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

    /** This tests if the hashes returned by the pojo and tuple comparators are the same. */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    void testTuplePojoTestEquality() throws IncompatibleKeysException {

        // test with a simple, string-key first.
        PojoTypeInfo<TestUserClass> pType = (PojoTypeInfo<TestUserClass>) type;
        List<FlatFieldDescriptor> result = new ArrayList<>();
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
                        new Date(),
                        new NestedTestUserClass(1, "haha", 4d, new int[] {5, 4, 3}));
        int pHash = pojoComp.hash(pojoTestRecord);

        Tuple1<String> tupleTest = new Tuple1<>("haha");
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
                new Tuple3<>(1, "haha", 4d); // its important here to use the same values.
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
}
