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

import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.utils.LogicalTypeDuplicator;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LogicalTypeDuplicator}. */
class LogicalTypeDuplicatorTest {

    private static final LogicalTypeDuplicator DUPLICATOR = new LogicalTypeDuplicator();

    private static final LogicalTypeDuplicator INT_REPLACER = new IntReplacer();

    private static Stream<Arguments> testData() {
        return Stream.of(
                Arguments.of(new CharType(2), new CharType(2)),
                Arguments.of(
                        createMultisetType(new IntType()), createMultisetType(new BigIntType())),
                Arguments.of(createArrayType(new IntType()), createArrayType(new BigIntType())),
                Arguments.of(createMapType(new IntType()), createMapType(new BigIntType())),
                Arguments.of(createRowType(new IntType()), createRowType(new BigIntType())),
                Arguments.of(
                        createDistinctType(new IntType()), createDistinctType(new BigIntType())),
                Arguments.of(createUserType(new IntType()), createUserType(new BigIntType())),
                Arguments.of(createHumanType(), createHumanType()));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("testData")
    void testDuplication(LogicalType logicalType, LogicalType replacedLogicalType) {
        assertThat(logicalType.accept(DUPLICATOR)).isEqualTo(logicalType);
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("testData")
    void testReplacement(LogicalType logicalType, LogicalType replacedLogicalType) {
        assertThat(logicalType.accept(INT_REPLACER)).isEqualTo(replacedLogicalType);
    }

    // --------------------------------------------------------------------------------------------

    private static class IntReplacer extends LogicalTypeDuplicator {
        @Override
        public LogicalType visit(IntType intType) {
            return new BigIntType();
        }
    }

    private static MultisetType createMultisetType(LogicalType replacedType) {
        return new MultisetType(new MultisetType(replacedType));
    }

    private static ArrayType createArrayType(LogicalType replacedType) {
        return new ArrayType(new ArrayType(replacedType));
    }

    private static MapType createMapType(LogicalType replacedType) {
        return new MapType(replacedType, new SmallIntType());
    }

    private static DistinctType createDistinctType(LogicalType replacedType) {
        return new DistinctType.Builder(ObjectIdentifier.of("cat", "db", "Money"), replacedType)
                .description("Money type desc.")
                .build();
    }

    private static RowType createRowType(LogicalType replacedType) {
        return new RowType(
                Arrays.asList(
                        new RowType.RowField("field1", new CharType(2)),
                        new RowType.RowField("field2", new BooleanType()),
                        new RowType.RowField("field3", replacedType)));
    }

    private static StructuredType createHumanType() {
        return StructuredType.newBuilder(ObjectIdentifier.of("cat", "db", "Human"), Human.class)
                .attributes(
                        Collections.singletonList(
                                new StructuredType.StructuredAttribute(
                                        "name", new VarCharType(), "Description.")))
                .description("Human type desc.")
                .setFinal(false)
                .setInstantiable(false)
                .build();
    }

    private static StructuredType createUserType(LogicalType replacedType) {
        return StructuredType.newBuilder(ObjectIdentifier.of("cat", "db", "User"), User.class)
                .attributes(
                        Collections.singletonList(
                                new StructuredType.StructuredAttribute("setting", replacedType)))
                .description("User type desc.")
                .setFinal(false)
                .setInstantiable(true)
                .superType(createHumanType())
                .build();
    }

    private abstract static class Human {
        public String name;
    }

    private static final class User extends Human {
        public int setting;
    }
}
