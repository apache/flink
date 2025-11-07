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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.types.utils.TypeConversions;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.table.planner.plan.utils.DeltaJoinUtil.isFilterOnOneSetOfUpsertKeys;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DeltaJoinUtil}. */
class DeltaJoinUtilTest {

    @Test
    void testIsFilterOnOneSetOfUpsertKeys() {
        FlinkTypeFactory typeFactory =
                new FlinkTypeFactory(
                        Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);
        // input schema:
        // a string,
        // b bigint,
        // c bigint
        List<RelDataType> allFieldTypes =
                Stream.of(DataTypes.VARCHAR(100), DataTypes.BIGINT(), DataTypes.BIGINT())
                        .map(TypeConversions::fromDataToLogicalType)
                        .map(typeFactory::createFieldTypeFromLogicalType)
                        .collect(Collectors.toList());

        RexBuilder rexBuilder = new RexBuilder(typeFactory);

        // a = 'jim'
        RexNode filter =
                rexBuilder.makeCall(
                        SqlStdOperatorTable.EQUALS,
                        rexBuilder.makeInputRef(allFieldTypes.get(0), 0),
                        rexBuilder.makeLiteral("jim", allFieldTypes.get(0)));

        assertThat(isFilterOnOneSetOfUpsertKeys(filter, Set.of(ImmutableBitSet.of(0)))).isTrue();
        assertThat(isFilterOnOneSetOfUpsertKeys(filter, Set.of(ImmutableBitSet.of(2)))).isFalse();
        assertThat(isFilterOnOneSetOfUpsertKeys(filter, Set.of(ImmutableBitSet.of(0, 1)))).isTrue();
        assertThat(isFilterOnOneSetOfUpsertKeys(filter, Set.of(ImmutableBitSet.of(1, 2))))
                .isFalse();
        assertThat(
                        isFilterOnOneSetOfUpsertKeys(
                                filter, Set.of(ImmutableBitSet.of(1), ImmutableBitSet.of(2))))
                .isFalse();
        assertThat(
                        isFilterOnOneSetOfUpsertKeys(
                                filter, Set.of(ImmutableBitSet.of(1), ImmutableBitSet.of(0))))
                .isTrue();
        assertThat(isFilterOnOneSetOfUpsertKeys(filter, null)).isFalse();
    }
}
