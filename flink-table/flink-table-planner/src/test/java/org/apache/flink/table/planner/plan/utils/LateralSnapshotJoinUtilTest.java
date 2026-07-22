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

package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.plan.schema.TimeIndicatorRelDataType;

import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LateralSnapshotJoinUtil}. */
class LateralSnapshotJoinUtilTest {

    private FlinkTypeFactory typeFactory;

    @BeforeEach
    void setup() {
        typeFactory =
                new FlinkTypeFactory(
                        Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);
    }

    @ParameterizedTest(name = "nullableCols={0}, leftOuter={1}")
    @CsvSource({"true, true", "true, false", "false, true", "false, false"})
    void testDeriveRowType(boolean nullableCols, boolean leftOuter) {
        // Probe (left) side: a scalar column and a rowtime attribute (both forwarded unchanged --
        // left time attributes are not materialized).
        final TimeIndicatorRelDataType leftRowtime = rowtime(nullableCols);
        final RelDataType leftType =
                typeFactory
                        .builder()
                        .add("pk", varchar(nullableCols))
                        .add("pts", leftRowtime)
                        .build();

        // Build (right) side: a scalar column and a rowtime attribute whose originalType has the
        // same nullability as the indicator.
        final TimeIndicatorRelDataType buildRowtime = rowtime(nullableCols);
        final RelDataType rightType =
                typeFactory
                        .builder()
                        .add("bk", varchar(nullableCols))
                        .add("bts", buildRowtime)
                        .build();

        final RelDataType rowType =
                LateralSnapshotJoinUtil.deriveRowType(
                        typeFactory,
                        leftType,
                        rightType,
                        leftOuter ? JoinRelType.LEFT : JoinRelType.INNER,
                        Collections.emptyList());

        final List<RelDataTypeField> fields = rowType.getFieldList();
        assertThat(fields)
                .extracting(RelDataTypeField::getName)
                .containsExactly("pk", "pts", "bk", "bts");

        // Left fields are forwarded unchanged (left is never the null-padded side of INNER/LEFT).
        assertThat(fields.get(0).getType())
                .as("left scalar forwarded unchanged")
                .isEqualTo(leftType.getFieldList().get(0).getType());
        assertThat(fields.get(1).getType())
                .as("left time attribute forwarded unchanged (still a time indicator)")
                .isInstanceOf(TimeIndicatorRelDataType.class)
                .isEqualTo(leftRowtime);

        // Build-side scalar: base type preserved; nullable iff the column is nullable or it is
        // null-padded by a LEFT join.
        assertThat(fields.get(2).getType().getSqlTypeName()).isEqualTo(SqlTypeName.VARCHAR);
        assertThat(fields.get(2).getType().isNullable())
                .as("build-side scalar nullability")
                .isEqualTo(nullableCols || leftOuter);

        // Build-side time attribute: materialized to a regular timestamp (no longer an indicator);
        // nullable iff the column is nullable or it is null-padded by a LEFT join.
        assertThat(fields.get(3).getType())
                .as("build-side time attribute is materialized")
                .isNotInstanceOf(TimeIndicatorRelDataType.class);
        assertThat(fields.get(3).getType().getSqlTypeName()).isEqualTo(SqlTypeName.TIMESTAMP);
        assertThat(fields.get(3).getType().isNullable())
                .as("materialized build-side rowtime nullability")
                .isEqualTo(nullableCols || leftOuter);
    }

    /**
     * Regression guard for the case where a build-side time attribute's {@code originalType}
     * nullability does NOT match the indicator's. {@code
     * FlinkTypeFactory#createTypeWithNullability} can widen a NOT NULL rowtime to nullable by
     * flipping only the indicator, leaving {@code originalType} inconsistent. {@link
     * LateralSnapshotJoinUtil#deriveRowType} must materialize using the indicator's nullability,
     * not {@code originalType}'s.
     */
    @ParameterizedTest(name = "nullableTimeCol={0}")
    @ValueSource(booleans = {true, false})
    void testDeriveRowTypeIgnoresStaleOriginalTypeNullability(boolean nullableTimeCol) {
        // originalType is deliberately the opposite nullability of the indicator.
        final TimeIndicatorRelDataType buildRowtime = rowtime(nullableTimeCol, !nullableTimeCol);
        assertThat(buildRowtime.isNullable()).isEqualTo(nullableTimeCol);
        assertThat(buildRowtime.getOriginalType().isNullable()).isEqualTo(!nullableTimeCol);

        final RelDataType leftType =
                typeFactory
                        .builder()
                        .add("pk", typeFactory.createSqlType(SqlTypeName.VARCHAR))
                        .build();
        final RelDataType rightType =
                typeFactory
                        .builder()
                        .add("bk", typeFactory.createSqlType(SqlTypeName.VARCHAR))
                        .add("bts", buildRowtime)
                        .build();

        final RelDataType rowType =
                LateralSnapshotJoinUtil.deriveRowType(
                        typeFactory,
                        leftType,
                        rightType,
                        JoinRelType.INNER,
                        Collections.emptyList());

        final RelDataTypeField bts = rowType.getField("bts", true, false);
        assertThat(bts).isNotNull();
        assertThat(bts.getType())
                .as("build-side time attribute is materialized")
                .isNotInstanceOf(TimeIndicatorRelDataType.class);
        // Nullability must follow the indicator, not the (opposite) originalType.
        assertThat(bts.getType().isNullable())
                .as("materialized rowtime uses indicator nullability, not originalType")
                .isEqualTo(nullableTimeCol);
    }

    private RelDataType varchar(boolean nullable) {
        return typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.VARCHAR), nullable);
    }

    private TimeIndicatorRelDataType rowtime(boolean nullable) {
        return rowtime(nullable, nullable);
    }

    /**
     * Builds an event-time {@link TimeIndicatorRelDataType} whose indicator and underlying {@code
     * originalType} nullabilities can be set independently.
     */
    private TimeIndicatorRelDataType rowtime(
            boolean indicatorNullable, boolean originalTypeNullable) {
        final BasicSqlType originalType =
                (BasicSqlType)
                        typeFactory.createTypeWithNullability(
                                typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3),
                                originalTypeNullable);
        return new TimeIndicatorRelDataType(
                typeFactory.getTypeSystem(),
                originalType,
                indicatorNullable,
                /* isEventTime= */ true);
    }
}
