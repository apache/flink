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

package org.apache.flink.table.planner.functions;

import org.apache.flink.api.common.typeutils.base.LocalDateTimeSerializer;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.stream.Stream;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BINARY;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.BYTES;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.MAP;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIME;
import static org.apache.flink.table.api.DataTypes.TINYINT;
import static org.apache.flink.table.api.DataTypes.VARBINARY;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.row;
import static org.apache.flink.util.CollectionUtil.entry;
import static org.apache.flink.util.CollectionUtil.map;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BuiltInFunctionDefinitions#CAST} regarding {@link DataTypes#ROW}. */
class CastFunctionMiscITCase extends BuiltInFunctionTestBase {

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(
                TestSetSpec.forFunction(
                                BuiltInFunctionDefinitions.CAST,
                                "implicit with different field names")
                        .onFieldsWithData(Row.of(12, "Hello"))
                        .andDataTypes(DataTypes.of("ROW<otherNameInt INT, otherNameString STRING>"))
                        .withFunction(RowToFirstField.class)
                        .testResult(
                                call("RowToFirstField", $("f0")), "RowToFirstField(f0)", 12, INT()),
                TestSetSpec.forFunction(
                                BuiltInFunctionDefinitions.CAST, "implicit with type widening")
                        .onFieldsWithData(Row.of((byte) 12, "Hello"))
                        .andDataTypes(DataTypes.of("ROW<i TINYINT, s STRING>"))
                        .withFunction(RowToFirstField.class)
                        .testResult(
                                call("RowToFirstField", $("f0")), "RowToFirstField(f0)", 12, INT()),
                TestSetSpec.forFunction(
                                BuiltInFunctionDefinitions.CAST,
                                "implicit with nested type widening")
                        .onFieldsWithData(Row.of(Row.of(12, 42), "Hello"))
                        .andDataTypes(DataTypes.of("ROW<r ROW<i1 INT, i2 INT>, s STRING>"))
                        .withFunction(NestedRowToFirstField.class)
                        .testResult(
                                call("NestedRowToFirstField", $("f0")),
                                "NestedRowToFirstField(f0)",
                                Row.of(12, 42.0),
                                DataTypes.of("ROW<i INT, d DOUBLE>")),
                TestSetSpec.forFunction(
                                BuiltInFunctionDefinitions.CAST,
                                "explicit with nested rows and implicit nullability change")
                        .onFieldsWithData(Row.of(Row.of(12, 42, null), "Hello"))
                        .andDataTypes(DataTypes.of("ROW<r ROW<i1 INT, i2 INT, i3 INT>, s STRING>"))
                        .testResult(
                                $("f0").cast(
                                                ROW(
                                                        FIELD(
                                                                "r",
                                                                ROW(
                                                                        FIELD("s", STRING()),
                                                                        FIELD("b", BOOLEAN()),
                                                                        FIELD("i", INT()))),
                                                        FIELD("s", STRING()))),
                                "CAST(f0 AS ROW<r ROW<s STRING NOT NULL, b BOOLEAN, i INT>, s STRING>)",
                                Row.of(Row.of("12", true, null), "Hello"),
                                // the inner NOT NULL is ignored in SQL because the outer ROW is
                                // nullable and the cast does not allow setting the outer
                                // nullability but derives it from the source operand
                                DataTypes.of("ROW<r ROW<s STRING, b BOOLEAN, i INT>, s STRING>")),
                TestSetSpec.forFunction(
                                BuiltInFunctionDefinitions.CAST,
                                "explicit with nested rows and explicit nullability change")
                        .onFieldsWithData(Row.of(Row.of(12, 42, null), "Hello"))
                        .andDataTypes(DataTypes.of("ROW<r ROW<i1 INT, i2 INT, i3 INT>, s STRING>"))
                        .testTableApiResult(
                                $("f0").cast(
                                                ROW(
                                                        FIELD(
                                                                "r",
                                                                ROW(
                                                                        FIELD(
                                                                                "s",
                                                                                STRING().notNull()),
                                                                        FIELD("b", BOOLEAN()),
                                                                        FIELD("i", INT()))),
                                                        FIELD("s", STRING()))),
                                Row.of(Row.of("12", true, null), "Hello"),
                                DataTypes.of(
                                        "ROW<r ROW<s STRING NOT NULL, b BOOLEAN, i INT>, s STRING>")),
                TestSetSpec.forFunction(
                                BuiltInFunctionDefinitions.CAST,
                                "implicit between structured type and row")
                        .onFieldsWithData(12, "Ingo")
                        .withFunction(StructuredTypeConstructor.class)
                        .withFunction(RowToFirstField.class)
                        .testResult(
                                call(
                                        "RowToFirstField",
                                        call("StructuredTypeConstructor", row($("f0"), $("f1")))),
                                "RowToFirstField(StructuredTypeConstructor((f0, f1)))",
                                12,
                                INT()),
                TestSetSpec.forFunction(
                                BuiltInFunctionDefinitions.CAST,
                                "explicit between structured type and row")
                        .onFieldsWithData(12, "Ingo")
                        .withFunction(StructuredTypeConstructor.class)
                        .testTableApiResult(
                                call("StructuredTypeConstructor", row($("f0"), $("f1")))
                                        .cast(ROW(BIGINT(), STRING())),
                                Row.of(12L, "Ingo"),
                                ROW(BIGINT(), STRING())),
                TestSetSpec.forFunction(
                                BuiltInFunctionDefinitions.CAST,
                                "cast from RAW(Integer) to BINARY(3)")
                        .onFieldsWithData(123456)
                        .andDataTypes(INT())
                        .withFunction(IntegerToRaw.class)
                        .testTableApiResult(
                                call("IntegerToRaw", $("f0")).cast(BINARY(3)),
                                new byte[] {0, 1, -30},
                                BINARY(3)),
                TestSetSpec.forFunction(
                                BuiltInFunctionDefinitions.CAST, "cast from RAW(Integer) to BYTES")
                        .onFieldsWithData(123456)
                        .andDataTypes(INT())
                        .withFunction(IntegerToRaw.class)
                        .testTableApiResult(
                                call("IntegerToRaw", $("f0")).cast(BYTES()),
                                new byte[] {0, 1, -30, 64},
                                BYTES()),
                TestSetSpec.forFunction(
                                BuiltInFunctionDefinitions.CAST,
                                "cast from RAW(Integer) to BINARY(6)")
                        .onFieldsWithData(123456)
                        .andDataTypes(INT())
                        .withFunction(IntegerToRaw.class)
                        .testTableApiResult(
                                call("IntegerToRaw", $("f0")).cast(BINARY(6)),
                                new byte[] {0, 1, -30, 64, 0, 0},
                                BINARY(6)),
                TestSetSpec.forFunction(
                                BuiltInFunctionDefinitions.CAST,
                                "cast from RAW(UserPojo) to VARBINARY")
                        .onFieldsWithData(123456, "Flink")
                        .andDataTypes(INT(), STRING())
                        .withFunction(StructuredTypeConstructor.class)
                        .withFunction(PojoToRaw.class)
                        .testTableApiResult(
                                call(
                                                "PojoToRaw",
                                                call(
                                                        "StructuredTypeConstructor",
                                                        row($("f0"), $("f1"))))
                                        .cast(VARBINARY(50)),
                                new byte[] {0, 1, -30, 64, 0, 70, 0, 108, 0, 105, 0, 110, 0, 107},
                                VARBINARY(50)),
                TestSetSpec.forFunction(
                                BuiltInFunctionDefinitions.CAST, "test the x'....' binary syntax")
                        .onFieldsWithData("foo")
                        .testSqlResult(
                                "CAST(CAST(x'68656C6C6F20636F6465' AS BINARY(10)) AS VARCHAR)",
                                "hello code",
                                STRING().notNull()),
                TestSetSpec.forFunction(
                                BuiltInFunctionDefinitions.CAST, "test the x'....' binary syntax")
                        .onFieldsWithData("foo")
                        .testSqlResult(
                                "CAST(CAST(x'68656C6C6F20636F6465' AS BINARY(5)) AS VARCHAR)",
                                "hello",
                                STRING().notNull()),
                TestSetSpec.forFunction(
                                BuiltInFunctionDefinitions.CAST, "cast STRUCTURED to STRING")
                        .onFieldsWithData(123456, "Flink")
                        .andDataTypes(INT(), STRING())
                        .withFunction(StructuredTypeConstructor.class)
                        .testTableApiResult(
                                call("StructuredTypeConstructor", row($("f0"), $("f1")))
                                        .cast(STRING()),
                                "(i=123456, s=Flink)",
                                STRING()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.CAST, "cast MULTISET to STRING")
                        .onFieldsWithData(map(entry("a", 1), entry("b", 2)))
                        .andDataTypes(MAP(STRING(), INT()))
                        .withFunction(JsonFunctionsITCase.CreateMultiset.class)
                        .testTableApiResult(
                                call("CreateMultiset", $("f0")).cast(STRING()),
                                "{a=1, b=2}",
                                STRING()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.CAST, "cast RAW to STRING")
                        .onFieldsWithData("2020-11-11T18:08:01.123")
                        .andDataTypes(STRING())
                        .withFunction(LocalDateTimeToRaw.class)
                        .testTableApiResult(
                                call("LocalDateTimeToRaw", $("f0")).cast(STRING()),
                                "2020-11-11T18:08:01.123",
                                STRING()),
                TestSetSpec.forFunction(
                                BuiltInFunctionDefinitions.TRY_CAST, "try cast from STRING to TIME")
                        .onFieldsWithData("Flink", "12:34:56")
                        .andDataTypes(STRING(), STRING())
                        .testResult(
                                $("f0").tryCast(TIME()),
                                "TRY_CAST(f0 AS TIME)",
                                null,
                                TIME().nullable())
                        .testResult(
                                $("f1").tryCast(TIME()),
                                "TRY_CAST(f1 AS TIME)",
                                LocalTime.of(12, 34, 56, 0),
                                TIME().nullable()),
                TestSetSpec.forFunction(
                                BuiltInFunctionDefinitions.TRY_CAST,
                                "try cast from TIME NOT NULL to STRING NOT NULL")
                        .onFieldsWithData(LocalTime.parse("12:34:56"))
                        .andDataTypes(TIME().notNull())
                        .testResult(
                                $("f0").tryCast(STRING()),
                                "TRY_CAST(f0 AS STRING)",
                                "12:34:56",
                                STRING().nullable()),
                TestSetSpec.forFunction(
                                BuiltInFunctionDefinitions.TRY_CAST,
                                "try cast from ROW<INT, STRING> to ROW<TINYINT, TIME>")
                        .onFieldsWithData(Row.of(1, "abc"), Row.of(1, "12:34:56"))
                        .andDataTypes(ROW(INT(), STRING()), ROW(INT(), STRING()))
                        .testResult(
                                $("f0").tryCast(ROW(TINYINT(), TIME())),
                                "TRY_CAST(f0 AS ROW(f0 TINYINT, f1 TIME))",
                                null,
                                ROW(TINYINT(), TIME()).nullable())
                        .testResult(
                                $("f1").tryCast(ROW(TINYINT(), TIME())),
                                "TRY_CAST(f1 AS ROW(f0 TINYINT, f1 TIME))",
                                Row.of((byte) 1, LocalTime.of(12, 34, 56, 0)),
                                ROW(TINYINT(), TIME()).nullable()));
    }

    // --------------------------------------------------------------------------------------------

    /** Function that return the first field of the input row. */
    public static class RowToFirstField extends ScalarFunction {
        public Integer eval(@DataTypeHint("ROW<i INT, s STRING>") Row row) {
            assertThat(row.getField(0)).isInstanceOf(Integer.class);
            assertThat(row.getField(1)).isInstanceOf(String.class);
            return (Integer) row.getField(0);
        }
    }

    /** Function that return the first field of the nested input row. */
    public static class NestedRowToFirstField extends ScalarFunction {
        public @DataTypeHint("ROW<i INT, d DOUBLE>") Row eval(
                @DataTypeHint("ROW<r ROW<i INT, d DOUBLE>, s STRING>") Row row) {
            assertThat(row.getField(0)).isInstanceOf(Row.class);
            assertThat(row.getField(1)).isInstanceOf(String.class);
            return (Row) row.getField(0);
        }
    }

    /** Function that takes and forwards a structured type. */
    public static class StructuredTypeConstructor extends ScalarFunction {
        public UserPojo eval(UserPojo pojo) {
            return pojo;
        }
    }

    /** Testing POJO. */
    public static class UserPojo {
        public final Integer i;

        public final String s;

        public UserPojo(Integer i, String s) {
            this.i = i;
            this.s = s;
        }
    }

    /** Test Raw with built-in Java class. */
    public static class IntegerToRaw extends ScalarFunction {

        public @DataTypeHint(value = "RAW", bridgedTo = byte[].class) byte[] eval(Integer i) {
            ByteBuffer b = ByteBuffer.allocate(4);
            b.putInt(i);
            return b.array();
        }
    }

    /** Test Raw with custom class. */
    public static class PojoToRaw extends ScalarFunction {

        public @DataTypeHint(value = "RAW", bridgedTo = byte[].class) byte[] eval(UserPojo up) {
            ByteBuffer b = ByteBuffer.allocate((up.s.length() * 2) + 4);
            b.putInt(up.i);
            for (char c : up.s.toCharArray()) {
                b.putChar(c);
            }
            return b.array();
        }
    }

    /** Test Raw with custom class. */
    public static class LocalDateTimeToRaw extends ScalarFunction {

        public @DataTypeHint(
                value = "RAW",
                bridgedTo = LocalDateTime.class,
                rawSerializer = LocalDateTimeSerializer.class) LocalDateTime eval(String str) {
            return LocalDateTime.parse(str);
        }
    }
}
