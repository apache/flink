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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.api.common.typeutils.SerializerTestInstance;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static java.util.Arrays.asList;

/** Tests for {@link ExternalSerializer}. */
@RunWith(Parameterized.class)
public class ExternalSerializerTest<T> extends SerializerTestInstance<T> {

    @Parameters(name = "{index}: {0}")
    public static List<TestSpec<?>> testData() {
        return asList(
                TestSpec.forDataType(DataTypes.INT()).withLength(4).addInstance(18).addInstance(42),
                TestSpec.forDataType(
                                DataTypes.ROW(
                                        DataTypes.FIELD("age", DataTypes.INT()),
                                        DataTypes.FIELD("name", DataTypes.STRING())))
                        .addInstance(Row.of(12, "Bob"))
                        .addInstance(Row.of(42, null)),
                TestSpec.forDataType(
                                DataTypes.STRUCTURED(
                                        ImmutableTestPojo.class,
                                        DataTypes.FIELD("age", DataTypes.INT()),
                                        DataTypes.FIELD("name", DataTypes.STRING())))
                        .addInstance(new ImmutableTestPojo(12, "Bob"))
                        .addInstance(new ImmutableTestPojo(42, null)),
                TestSpec.forDataType(
                                DataTypes.ARRAY(
                                                DataTypes.STRUCTURED(
                                                        ImmutableTestPojo.class,
                                                        DataTypes.FIELD("age", DataTypes.INT()),
                                                        DataTypes.FIELD(
                                                                "name", DataTypes.STRING())))
                                        .bridgedTo(List.class))
                        .addInstance(Collections.singletonList(new ImmutableTestPojo(12, "Bob")))
                        .addInstance(
                                Arrays.asList(
                                        new ImmutableTestPojo(42, "Alice"),
                                        null,
                                        null,
                                        new ImmutableTestPojo(42, null))),
                TestSpec.forDataType(DataTypes.ARRAY(DataTypes.INT()))
                        .addInstance(new Integer[] {0, 1, null, 3})
                        .addInstance(new Integer[0]));
    }

    @SuppressWarnings("unchecked")
    public ExternalSerializerTest(TestSpec<T> testSpec) {
        super(
                ExternalSerializer.of(testSpec.dataType),
                (Class<T>) testSpec.dataType.getConversionClass(),
                testSpec.length,
                testSpec.instances.toArray(
                        (T[]) Array.newInstance(testSpec.dataType.getConversionClass(), 0)));
    }

    @Override
    protected boolean allowNullInstances(TypeSerializer<T> serializer) {
        return true;
    }

    // --------------------------------------------------------------------------------------------

    private static class TestSpec<T> {

        private final DataType dataType;

        private final List<T> instances = new ArrayList<>();

        private int length = -1;

        private TestSpec(DataType dataType) {
            this.dataType = dataType;
        }

        static <T> TestSpec<T> forDataType(DataType dataType) {
            return new TestSpec<>(dataType);
        }

        TestSpec<T> withLength(int length) {
            this.length = length;
            return this;
        }

        TestSpec<T> addInstance(T instance) {
            instances.add(instance);
            return this;
        }

        @Override
        public String toString() {
            return dataType.toString();
        }
    }

    // --------------------------------------------------------------------------------------------

    /** Immutable POJO for testing. */
    public static class ImmutableTestPojo {

        public final int age;
        public final String name;

        public ImmutableTestPojo(int age, String name) {
            this.age = age;
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ImmutableTestPojo that = (ImmutableTestPojo) o;
            return age == that.age && Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(age, name);
        }
    }
}
