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

package org.apache.flink.table.types;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.utils.DataTypeFactoryMock;
import org.apache.flink.table.types.utils.TypeInfoDataTypeConverter;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.time.DayOfWeek;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.types.utils.DataTypeFactoryMock.dummyRaw;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/** Tests for {@link TypeInfoDataTypeConverter}. */
@RunWith(Parameterized.class)
public class TypeInfoDataTypeConverterTest {

    @Parameters(name = "{index}: {0}")
    public static List<TestSpec> testData() {
        return Arrays.asList(
                TestSpec.forType(Types.INT).expectDataType(DataTypes.INT().notNull()),
                TestSpec.forType(Types.BIG_DEC)
                        .expectDataType(DataTypes.DECIMAL(38, 18).nullable()),
                TestSpec.forType(
                                Types.ROW_NAMED(
                                        new String[] {"a", "b", "c"},
                                        Types.DOUBLE,
                                        Types.INT,
                                        Types.STRING))
                        .expectDataType(
                                DataTypes.ROW(
                                                DataTypes.FIELD("a", DataTypes.DOUBLE()),
                                                DataTypes.FIELD("b", DataTypes.INT()),
                                                DataTypes.FIELD("c", DataTypes.STRING()))
                                        .notNull()),
                TestSpec.forType(
                                Types.TUPLE(
                                        Types.INT,
                                        Types.TUPLE(Types.STRING, Types.BOOLEAN),
                                        Types.LONG))
                        .expectDataType(
                                DataTypes.STRUCTURED(
                                                Tuple3.class,
                                                DataTypes.FIELD("f0", DataTypes.INT().notNull()),
                                                DataTypes.FIELD(
                                                        "f1",
                                                        DataTypes.STRUCTURED(
                                                                        Tuple2.class,
                                                                        DataTypes.FIELD(
                                                                                "f0",
                                                                                DataTypes.STRING()
                                                                                        .nullable()),
                                                                        DataTypes.FIELD(
                                                                                "f1",
                                                                                DataTypes.BOOLEAN()
                                                                                        .notNull()))
                                                                .notNull()),
                                                DataTypes.FIELD("f2", DataTypes.BIGINT().notNull()))
                                        .notNull()),
                TestSpec.forType(Types.POJO(PojoWithFieldOrder.class))
                        .expectDataType(
                                DataTypes.STRUCTURED(
                                        PojoWithFieldOrder.class,
                                        DataTypes.FIELD("name", DataTypes.STRING()),
                                        DataTypes.FIELD(
                                                "gender",
                                                DataTypes.BOOLEAN()
                                                        .notNull()
                                                        .bridgedTo(boolean.class)),
                                        DataTypes.FIELD(
                                                "age",
                                                DataTypes.INT().notNull().bridgedTo(int.class)))),
                TestSpec.forType(Types.POJO(PojoWithDefaultFieldOrder.class))
                        .expectDataType(
                                DataTypes.STRUCTURED(
                                        PojoWithDefaultFieldOrder.class,
                                        DataTypes.FIELD("age", DataTypes.INT().nullable()),
                                        DataTypes.FIELD(
                                                "gender",
                                                DataTypes.BOOLEAN()
                                                        .notNull()
                                                        .bridgedTo(boolean.class)),
                                        DataTypes.FIELD("name", DataTypes.STRING()))),
                TestSpec.forType(new QueryableTypeInfo()).expectDataType(DataTypes.BYTES()),
                TestSpec.forType(Types.ENUM(DayOfWeek.class))
                        .lookupExpects(DayOfWeek.class)
                        .expectDataType(dummyRaw(DayOfWeek.class)));
    }

    @Parameter public TestSpec testSpec;

    @Test
    public void testConversion() {
        final DataType dataType =
                TypeInfoDataTypeConverter.toDataType(testSpec.typeFactory, testSpec.typeInfo);
        assertThat(dataType, equalTo(testSpec.expectedDataType));
    }

    // --------------------------------------------------------------------------------------------
    // Test utilities
    // --------------------------------------------------------------------------------------------

    /** Test specification shared with the Scala tests. */
    static class TestSpec {

        final DataTypeFactoryMock typeFactory = new DataTypeFactoryMock();

        final TypeInformation<?> typeInfo;

        DataType expectedDataType;

        private TestSpec(TypeInformation<?> typeInfo) {
            this.typeInfo = typeInfo;
        }

        static TestSpec forType(TypeInformation<?> typeInfo) {
            return new TestSpec(typeInfo);
        }

        TestSpec lookupExpects(Class<?> lookupClass) {
            typeFactory.dataType = Optional.of(dummyRaw(lookupClass));
            typeFactory.expectedClass = Optional.of(lookupClass);
            return this;
        }

        TestSpec expectDataType(DataType expectedDataType) {
            this.expectedDataType = expectedDataType;
            return this;
        }

        @Override
        public String toString() {
            return typeInfo + " to " + expectedDataType;
        }
    }

    // --------------------------------------------------------------------------------------------
    // Test classes for conversion
    // --------------------------------------------------------------------------------------------

    /** POJO that defines a field order via an additional constructor. */
    public static class PojoWithFieldOrder {

        public boolean gender;

        public String name;

        public int age;

        public PojoWithFieldOrder() {
            // default constructor
        }

        public PojoWithFieldOrder(String name, boolean gender, int age) {
            this.name = name;
            this.gender = gender;
            this.age = age;
        }
    }

    /** POJO that defines a field order via an additional constructor. */
    @SuppressWarnings("unused")
    public static class PojoWithDefaultFieldOrder {

        public boolean gender;

        public String name;

        public Integer age;

        public PojoWithDefaultFieldOrder() {
            // default constructor
        }
    }

    /** {@link TypeInformation} that contains a queryable {@link DataType}. */
    public static class QueryableTypeInfo extends TypeInformation<Object>
            implements DataTypeQueryable {

        @Override
        public boolean isBasicType() {
            return false;
        }

        @Override
        public boolean isTupleType() {
            return false;
        }

        @Override
        public int getArity() {
            return 0;
        }

        @Override
        public int getTotalFields() {
            return 0;
        }

        @Override
        public Class<Object> getTypeClass() {
            return null;
        }

        @Override
        public boolean isKeyType() {
            return false;
        }

        @Override
        public TypeSerializer<Object> createSerializer(ExecutionConfig config) {
            return null;
        }

        @Override
        public String toString() {
            return null;
        }

        @Override
        public boolean equals(Object obj) {
            return false;
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public boolean canEqual(Object obj) {
            return false;
        }

        @Override
        public DataType getDataType() {
            return DataTypes.BYTES();
        }
    }
}
