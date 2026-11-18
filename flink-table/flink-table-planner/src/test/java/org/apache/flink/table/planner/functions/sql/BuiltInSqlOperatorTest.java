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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BuiltInSqlOperator}. */
class BuiltInSqlOperatorTest {

    private static final FlinkTypeFactory FACTORY =
            new FlinkTypeFactory(
                    BuiltInSqlOperatorTest.class.getClassLoader(), FlinkTypeSystem.INSTANCE);

    @Test
    void testVersionedCopy() {
        final BuiltInSqlOperator item =
                BuiltInSqlOperator.newBuilder(SqlStdOperatorTable.ITEM).version(2).build();

        assertThat(item.getName()).isEqualTo("$ITEM$2");
        assertThat(item.getQualifiedName()).isEqualTo("$ITEM$2");
        assertThat(VersionedSqlOperator.toQualifiedName(item)).isEqualTo("$ITEM$2");
        assertThat(VersionedSqlOperator.unwrapVersion(item)).contains(2);
        assertThat(item.getKind()).isEqualTo(SqlKind.ITEM);
        assertThat(item.getSyntax()).isEqualTo(SqlStdOperatorTable.ITEM.getSyntax());

        // Operators that are not versioned use the default version
        assertThat(VersionedSqlOperator.toQualifiedName(SqlStdOperatorTable.LESS_THAN))
                .isEqualTo("$<$1");
    }

    @Test
    void testTypeInferenceOfWrappedOperator() {
        final RexBuilder rexBuilder = new RexBuilder(FACTORY);
        final RexNode array =
                rexBuilder.makeInputRef(
                        FACTORY.createArrayType(FACTORY.createSqlType(SqlTypeName.INTEGER), -1), 0);
        final RexNode index = rexBuilder.makeExactLiteral(BigDecimal.ONE);
        final BuiltInSqlOperator item =
                BuiltInSqlOperator.newBuilder(SqlStdOperatorTable.ITEM).version(2).build();

        assertThat(rexBuilder.makeCall(item, array, index).getType())
                .isEqualTo(rexBuilder.makeCall(SqlStdOperatorTable.ITEM, array, index).getType());
    }

    @Test
    void testInternal() {
        final BuiltInSqlOperator internal =
                BuiltInSqlOperator.newBuilder(SqlStdOperatorTable.ITEM)
                        .version(1)
                        .internal()
                        .build();

        assertThat(internal.getName()).isEqualTo("$ITEM$1");
        assertThat(internal.getQualifiedName()).isEqualTo("$ITEM$1");
        assertThat(VersionedSqlOperator.toQualifiedName(internal)).isEqualTo("$ITEM$1");
        assertThat(VersionedSqlOperator.unwrapIsInternal(internal)).isTrue();
        assertThat(VersionedSqlOperator.unwrapVersion(internal)).isEmpty();

        // Symbols of Calcite's standard operators are allowed
        assertThat(
                        BuiltInSqlOperator.newBuilder(SqlStdOperatorTable.LESS_THAN)
                                .internal()
                                .build()
                                .getName())
                .isEqualTo("$<$1");
        assertThat(
                        BuiltInSqlOperator.newBuilder(SqlStdOperatorTable.CONCAT)
                                .internal()
                                .build()
                                .getName())
                .isEqualTo("$||$1");

        assertThatThrownBy(
                        () ->
                                BuiltInSqlOperator.newBuilder(
                                                new SqlSpecialOperator("A.B", SqlKind.OTHER))
                                        .internal()
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith(
                        "Internal function '$A.B$1' does not adhere to the naming convention");
    }

    @Test
    void testInvalidVersion() {
        assertThatThrownBy(
                        () ->
                                BuiltInSqlOperator.newBuilder(SqlStdOperatorTable.ITEM)
                                        .version(0)
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Version must be at least 1.");
    }
}
