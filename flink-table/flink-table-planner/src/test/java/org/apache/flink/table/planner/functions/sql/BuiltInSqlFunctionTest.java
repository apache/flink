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

package org.apache.flink.table.planner.functions.sql;

import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BuiltInSqlFunction}. */
class BuiltInSqlFunctionTest {

    private static final FlinkTypeFactory FACTORY =
            new FlinkTypeFactory(
                    BuiltInSqlFunctionTest.class.getClassLoader(), FlinkTypeSystem.INSTANCE);

    @Test
    void testVersioned() {
        final BuiltInSqlFunction function =
                BuiltInSqlFunction.newBuilder().name("OP").version(2).build();

        assertThat(function.getName()).isEqualTo("OP");
        assertThat(function.getQualifiedName()).isEqualTo("$OP$2");
        assertThat(function.isInternal()).isFalse();
        assertThat(function.getVersion()).contains(2);
    }

    @Test
    void testInternal() {
        final BuiltInSqlFunction internal =
                BuiltInSqlFunction.newBuilder().name("$OP$1").internal().build();

        assertThat(internal.getName()).isEqualTo("$OP$1");
        assertThat(internal.getQualifiedName()).isEqualTo("$OP$1");
        assertThat(internal.isInternal()).isTrue();
        assertThat(internal.getVersion()).isEmpty();
    }

    @Test
    void testVersionedCopy() {
        final BuiltInSqlFunction currentDate =
                BuiltInSqlFunction.newBuilder(SqlStdOperatorTable.CURRENT_DATE).version(2).build();

        assertThat(currentDate).isInstanceOf(SqlFunction.class);
        assertThat(currentDate.getName()).isEqualTo("$CURRENT_DATE$2");
        assertThat(currentDate.getQualifiedName()).isEqualTo("$CURRENT_DATE$2");
        assertThat(VersionedSqlOperator.toQualifiedName(currentDate)).isEqualTo("$CURRENT_DATE$2");
        assertThat(currentDate.isInternal()).isFalse();
        assertThat(currentDate.getVersion()).contains(2);
        assertThat(currentDate.getKind()).isEqualTo(SqlStdOperatorTable.CURRENT_DATE.getKind());
        assertThat(currentDate.getSyntax()).isEqualTo(SqlSyntax.FUNCTION_ID);
        assertThat(currentDate.getFunctionType()).isEqualTo(SqlFunctionCategory.TIMEDATE);
        assertThat(currentDate.isDynamicFunction()).isTrue();
    }

    @Test
    void testTypeInferenceOfWrappedFunction() {
        final RexBuilder rexBuilder = new RexBuilder(FACTORY);
        final RexNode string =
                rexBuilder.makeInputRef(FACTORY.createSqlType(SqlTypeName.VARCHAR), 0);
        final BuiltInSqlFunction upper =
                BuiltInSqlFunction.newBuilder(SqlStdOperatorTable.UPPER).version(2).build();

        assertThat(rexBuilder.makeCall(upper, string).getType())
                .isEqualTo(rexBuilder.makeCall(SqlStdOperatorTable.UPPER, string).getType());
    }

    @Test
    void testInternalCopy() {
        final BuiltInSqlFunction internal =
                BuiltInSqlFunction.newBuilder(SqlStdOperatorTable.UPPER)
                        .version(1)
                        .internal()
                        .build();

        assertThat(internal.getName()).isEqualTo("$UPPER$1");
        assertThat(internal.getQualifiedName()).isEqualTo("$UPPER$1");
        assertThat(internal.isInternal()).isTrue();
        assertThat(internal.getVersion()).isEmpty();
    }
}
