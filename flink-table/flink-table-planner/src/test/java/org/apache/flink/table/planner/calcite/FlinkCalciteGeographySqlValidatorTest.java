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

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.utils.PlannerMocks;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.GeographyType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Tests geography constructor validation that depends on SQL function support. */
class FlinkCalciteGeographySqlValidatorTest {

    private final PlannerMocks plannerMocks = PlannerMocks.create();

    @Test
    void testArrayConstructorInfersGeographyElementType() {
        LogicalType logicalType =
                projectedLogicalType(
                        "ARRAY[ST_GEOGFROMTEXT('POINT (0 0)'), ST_GEOGFROMTEXT('POINT (1 1)')]");

        assertThat(logicalType).isEqualTo(new ArrayType(new GeographyType()));
    }

    @Test
    void testArrayConstructorInfersNullableGeographyElementType() {
        LogicalType logicalType =
                projectedLogicalType("ARRAY[ST_GEOGFROMTEXT('POINT (0 0)'), NULL]");

        assertThat(logicalType).isEqualTo(new ArrayType(new GeographyType()));
    }

    @Test
    void testMapConstructorInfersGeographyValueType() {
        LogicalType logicalType =
                projectedLogicalType(
                        "MAP['a', ST_GEOGFROMTEXT('POINT (0 0)'), 'b', ST_GEOGFROMTEXT('POINT (1 1)')]");

        assertThat(logicalType)
                .isEqualTo(new MapType(VarCharType.STRING_TYPE, new GeographyType()));
    }

    @Test
    void testIncompatibleExtensionTypesFailValidation() {
        Throwable thrown =
                catchThrowable(
                        () ->
                                projectedLogicalType(
                                        "ARRAY[ST_GEOGFROMTEXT('POINT (0 0)'), BITMAP_BUILD(1)]"));

        assertThat(thrown).isInstanceOf(ValidationException.class);
        assertThat(thrown.getCause()).isNotInstanceOf(AssertionError.class);
    }

    private LogicalType projectedLogicalType(String expression) {
        SqlNode parsed = plannerMocks.getPlanner().parser().parse("SELECT " + expression + " AS c");
        SqlNode validated = plannerMocks.getPlanner().validate(parsed);
        RelRoot relRoot = plannerMocks.getPlanner().rel(validated);
        return FlinkTypeFactory.toLogicalType(
                relRoot.rel.getRowType().getFieldList().get(0).getType());
    }
}
