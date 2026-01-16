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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SqlTypeUtil}. */
class SqlTypeUtilTest {

    @Test
    void testConvertRowTypeToSpecAndUnparse() {
        // Create a ROW type with 2 fields
        FlinkTypeFactory typeFactory =
                new FlinkTypeFactory(
                        Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);

        RelDataType rowType =
                typeFactory.createStructType(
                        Arrays.asList(intType, varcharType), Arrays.asList("f0", "f1"));

        SqlDataTypeSpec typeSpec = SqlTypeUtil.convertTypeToSpec(rowType);

        // Try to unparse - without the bounds check fix, this throws IndexOutOfBoundsException
        SqlWriter writer =
                new SqlPrettyWriter(
                        SqlPrettyWriter.config()
                                .withAlwaysUseParentheses(false)
                                .withSelectListItemsOnSeparateLines(false)
                                .withIndentation(0));
        // unparse that will end up passing no comments through
        typeSpec.unparse(writer, 0, 0);
        String result = writer.toSqlString().getSql();
        assertThat(result).contains("ROW").contains("f0").contains("f1");
    }
}
