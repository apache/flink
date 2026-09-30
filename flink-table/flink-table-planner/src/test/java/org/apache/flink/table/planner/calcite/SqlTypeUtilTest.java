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

package org.apache.flink.table.planner.calcite;

import org.apache.flink.table.planner.typeutils.LogicalRelDataTypeConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.MAP;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.VARIANT;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SqlTypeUtil}. */
class SqlTypeUtilTest {

    private final FlinkTypeFactory typeFactory =
            new FlinkTypeFactory(
                    Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);

    private final SqlWriter writer =
            new SqlPrettyWriter(
                    SqlPrettyWriter.config()
                            .withAlwaysUseParentheses(false)
                            .withSelectListItemsOnSeparateLines(false)
                            .withIndentation(0));

    /**
     * Test case for <a href="https://issues.apache.org/jira/browse/FLINK-38913">[FLINK-38913]
     * ArrayIndexOutOfBoundsException when creating a table with computed rows including casts to
     * null</a>.
     */
    @Test
    void testConvertRowTypeToSpecAndUnparse() {
        RowType rowType =
                RowType.of(
                        new LogicalType[] {new IntType(), new VarCharType(1)},
                        new String[] {"a", "b"});
        assertThat(convertToSpecAndUnparse(rowType))
                .hasToString("ROW(\"a\" INTEGER, \"b\" VARCHAR(1) CHARACTER SET \"UTF-16LE\")");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("variantTypes")
    void testConvertTypeToSpecAndUnparse(DataType dataType, String expected) {
        assertThat(convertToSpecAndUnparse(dataType.getLogicalType())).isEqualTo(expected);
    }

    private static Stream<Arguments> variantTypes() {
        return Stream.of(
                Arguments.of(VARIANT(), "VARIANT"),
                Arguments.of(VARIANT().notNull(), "VARIANT"),
                Arguments.of(ARRAY(VARIANT()), "VARIANT ARRAY"),
                Arguments.of(ROW(FIELD("v", VARIANT())), "ROW(\"v\" VARIANT)"),
                Arguments.of(MAP(INT(), VARIANT()), "MAP< INTEGER, VARIANT >"));
    }

    private String convertToSpecAndUnparse(LogicalType logicalType) {
        RelDataType relDataType =
                LogicalRelDataTypeConverter.toRelDataType(logicalType, typeFactory);
        SqlDataTypeSpec typeSpec = SqlTypeUtil.convertTypeToSpec(relDataType);
        writer.reset();
        // unparse that will end up passing no comments through
        typeSpec.unparse(writer, 0, 0);
        return writer.toSqlString().getSql();
    }
}
