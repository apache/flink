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

package org.apache.flink.table.planner.plan.nodes.exec.serde;

import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.typeutils.LogicalRelDataTypeConverterTest.PojoClass;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.RawType;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.configuredSerdeContext;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.toJson;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.toObject;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/** Tests for {@link RelDataType} serialization and deserialization. */
@Execution(CONCURRENT)
public class RelDataTypeJsonSerdeTest {

    private static final FlinkTypeFactory FACTORY = FlinkTypeFactory.INSTANCE();

    @ParameterizedTest
    @MethodSource("testRelDataTypeSerde")
    public void testRelDataTypeSerde(RelDataType relDataType) throws IOException {
        final SerdeContext serdeContext = configuredSerdeContext();

        final String json = toJson(serdeContext, relDataType);
        final RelDataType actual = toObject(serdeContext, json, RelDataType.class);

        assertThat(actual).isSameAs(relDataType);
    }

    @Test
    public void testMissingPrecisionAndScale() throws IOException {
        final SerdeContext serdeContext = configuredSerdeContext();

        final String json =
                toJson(
                        serdeContext,
                        FACTORY.createSqlIntervalType(
                                new SqlIntervalQualifier(
                                        TimeUnit.DAY, TimeUnit.SECOND, SqlParserPos.ZERO)));
        final RelDataType actual = toObject(serdeContext, json, RelDataType.class);

        assertThat(actual)
                .isSameAs(
                        FACTORY.createSqlIntervalType(
                                new SqlIntervalQualifier(
                                        TimeUnit.DAY,
                                        DayTimeIntervalType.DEFAULT_DAY_PRECISION,
                                        TimeUnit.SECOND,
                                        DayTimeIntervalType.DEFAULT_FRACTIONAL_PRECISION,
                                        SqlParserPos.ZERO)));
    }

    @Test
    public void testNegativeScale() throws IOException {
        final SerdeContext serdeContext = configuredSerdeContext();

        final String json = toJson(serdeContext, FACTORY.createSqlType(SqlTypeName.DECIMAL, 5, -1));
        final RelDataType actual = toObject(serdeContext, json, RelDataType.class);

        assertThat(actual).isSameAs(FACTORY.createSqlType(SqlTypeName.DECIMAL, 6, 0));
    }

    // --------------------------------------------------------------------------------------------
    // Test data
    // --------------------------------------------------------------------------------------------

    public static List<RelDataType> testRelDataTypeSerde() {
        // the values in the list do not care about nullable.
        final List<RelDataType> types =
                Arrays.asList(
                        FACTORY.createSqlType(SqlTypeName.BOOLEAN),
                        FACTORY.createSqlType(SqlTypeName.TINYINT),
                        FACTORY.createSqlType(SqlTypeName.SMALLINT),
                        FACTORY.createSqlType(SqlTypeName.INTEGER),
                        FACTORY.createSqlType(SqlTypeName.BIGINT),
                        FACTORY.createSqlType(SqlTypeName.DECIMAL, 10, 3),
                        FACTORY.createSqlType(SqlTypeName.DECIMAL, 19, 0),
                        FACTORY.createSqlType(SqlTypeName.DECIMAL, 38, 19),
                        FACTORY.createSqlType(SqlTypeName.FLOAT),
                        FACTORY.createSqlType(SqlTypeName.DOUBLE),
                        FACTORY.createSqlType(SqlTypeName.DATE),
                        FACTORY.createSqlType(SqlTypeName.TIME),
                        FACTORY.createSqlType(SqlTypeName.TIMESTAMP),
                        FACTORY.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE),
                        FACTORY.createSqlIntervalType(
                                new SqlIntervalQualifier(
                                        TimeUnit.DAY,
                                        2,
                                        TimeUnit.MINUTE,
                                        RelDataType.PRECISION_NOT_SPECIFIED,
                                        SqlParserPos.ZERO)),
                        FACTORY.createSqlIntervalType(
                                new SqlIntervalQualifier(
                                        TimeUnit.DAY, 6, TimeUnit.SECOND, 9, SqlParserPos.ZERO)),
                        FACTORY.createSqlIntervalType(
                                new SqlIntervalQualifier(
                                        TimeUnit.HOUR,
                                        RelDataType.PRECISION_NOT_SPECIFIED,
                                        TimeUnit.SECOND,
                                        9,
                                        SqlParserPos.ZERO)),
                        FACTORY.createSqlIntervalType(
                                new SqlIntervalQualifier(
                                        TimeUnit.MINUTE,
                                        RelDataType.PRECISION_NOT_SPECIFIED,
                                        TimeUnit.SECOND,
                                        0,
                                        SqlParserPos.ZERO)),
                        FACTORY.createSqlIntervalType(
                                new SqlIntervalQualifier(
                                        TimeUnit.SECOND,
                                        RelDataType.PRECISION_NOT_SPECIFIED,
                                        TimeUnit.SECOND,
                                        6,
                                        SqlParserPos.ZERO)),
                        FACTORY.createSqlType(SqlTypeName.CHAR),
                        FACTORY.createSqlType(SqlTypeName.CHAR, 0),
                        FACTORY.createSqlType(SqlTypeName.CHAR, 32),
                        FACTORY.createSqlType(SqlTypeName.VARCHAR),
                        FACTORY.createSqlType(SqlTypeName.VARCHAR, 0),
                        FACTORY.createSqlType(SqlTypeName.VARCHAR, 10),
                        FACTORY.createSqlType(SqlTypeName.BINARY),
                        FACTORY.createSqlType(SqlTypeName.BINARY, 0),
                        FACTORY.createSqlType(SqlTypeName.BINARY, 100),
                        FACTORY.createSqlType(SqlTypeName.VARBINARY),
                        FACTORY.createSqlType(SqlTypeName.VARBINARY, 0),
                        FACTORY.createSqlType(SqlTypeName.VARBINARY, 1000),
                        FACTORY.createSqlType(SqlTypeName.NULL),
                        FACTORY.createSqlType(SqlTypeName.SYMBOL),
                        FACTORY.createMultisetType(FACTORY.createSqlType(SqlTypeName.VARCHAR), -1),
                        FACTORY.createArrayType(FACTORY.createSqlType(SqlTypeName.VARCHAR, 16), -1),
                        FACTORY.createArrayType(
                                FACTORY.createArrayType(
                                        FACTORY.createSqlType(SqlTypeName.VARCHAR, 16), -1),
                                -1),
                        FACTORY.createMapType(
                                FACTORY.createSqlType(SqlTypeName.INTEGER),
                                FACTORY.createSqlType(SqlTypeName.VARCHAR, 10)),
                        FACTORY.createMapType(
                                FACTORY.createMapType(
                                        FACTORY.createSqlType(SqlTypeName.INTEGER),
                                        FACTORY.createSqlType(SqlTypeName.VARCHAR, 10)),
                                FACTORY.createArrayType(
                                        FACTORY.createMapType(
                                                FACTORY.createSqlType(SqlTypeName.INTEGER),
                                                FACTORY.createSqlType(SqlTypeName.VARCHAR, 10)),
                                        -1)),
                        // simple struct type
                        FACTORY.createStructType(
                                StructKind.PEEK_FIELDS_NO_EXPAND,
                                Arrays.asList(
                                        FACTORY.createSqlType(SqlTypeName.INTEGER),
                                        FACTORY.createSqlType(SqlTypeName.DECIMAL, 10, 3)),
                                Arrays.asList("f1", "f2")),
                        // struct type with array type
                        FACTORY.createStructType(
                                StructKind.PEEK_FIELDS_NO_EXPAND,
                                Arrays.asList(
                                        FACTORY.createSqlType(SqlTypeName.VARCHAR),
                                        FACTORY.createArrayType(
                                                FACTORY.createSqlType(SqlTypeName.VARCHAR, 16),
                                                -1)),
                                Arrays.asList("f1", "f2")),
                        // nested struct type
                        FACTORY.createStructType(
                                StructKind.PEEK_FIELDS_NO_EXPAND,
                                Arrays.asList(
                                        FACTORY.createStructType(
                                                StructKind.PEEK_FIELDS_NO_EXPAND,
                                                Arrays.asList(
                                                        FACTORY.createSqlType(
                                                                SqlTypeName.VARCHAR, 5),
                                                        FACTORY.createSqlType(
                                                                SqlTypeName.VARCHAR, 10)),
                                                Arrays.asList("f1", "f2")),
                                        FACTORY.createArrayType(
                                                FACTORY.createSqlType(SqlTypeName.VARCHAR, 16),
                                                -1)),
                                Arrays.asList("f3", "f4")),
                        FACTORY.createRowtimeIndicatorType(true, false),
                        FACTORY.createRowtimeIndicatorType(true, true),
                        FACTORY.createProctimeIndicatorType(true),
                        FACTORY.createFieldTypeFromLogicalType(PojoClass.TYPE_WITH_IDENTIFIER));

        final List<RelDataType> mutableTypes = new ArrayList<>(types.size() * 2);
        for (RelDataType type : types) {
            mutableTypes.add(FACTORY.createTypeWithNullability(type, true));
            mutableTypes.add(FACTORY.createTypeWithNullability(type, false));
        }

        mutableTypes.add(
                FACTORY.createTypeWithNullability(
                        FACTORY.createFieldTypeFromLogicalType(
                                new RawType<>(true, Void.class, VoidSerializer.INSTANCE)),
                        true));
        mutableTypes.add(
                FACTORY.createTypeWithNullability(
                        FACTORY.createFieldTypeFromLogicalType(
                                new RawType<>(false, Void.class, VoidSerializer.INSTANCE)),
                        false));
        mutableTypes.add(
                FACTORY.createTypeWithNullability(
                        FACTORY.createFieldTypeFromLogicalType(
                                new RawType<>(true, Void.class, VoidSerializer.INSTANCE)),
                        false));

        return mutableTypes;
    }
}
