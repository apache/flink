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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.plan.nodes.exec.testutils.IntegrationTestBase;
import org.apache.flink.table.test.program.TableTestProgram;

import org.apache.flink.shaded.guava33.com.google.common.collect.ImmutableList;

import java.util.List;

/** Semantic tests for {@link StreamExecMultiJoin}. */
public class MultiJoinIntegrationTests extends IntegrationTestBase {

    @Override
    public ImmutableList<ConfigOption<Boolean>> testOptions() {
        return ImmutableList.of(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED);
    }

    @Override
    public List<TableTestProgram> programs() {
        return List.of(
                // Tests from JoinITCase to test RowData behaviour on restore
                MultiJoinTestPrograms.MULTI_JOIN_TWO_WAY_WITH_EQUAL_PK,
                MultiJoinTestPrograms.MULTI_JOIN_TWO_WAY_WITHOUT_PK,
                // Tests with boolean fields
                MultiJoinTestPrograms.MULTI_JOIN_THREE_WAY_INNER_JOIN_MATCHING_BOOLEANS,
                MultiJoinTestPrograms.MULTI_JOIN_THREE_WAY_INNER_JOIN_MATCHING_BOOLEANS_V2,
                MultiJoinTestPrograms
                        .MULTI_JOIN_THREE_WAY_INNER_JOIN_MATCHING_BOOLEANS_NO_COMMON_KEY,
                // Nested type test
                MultiJoinTestPrograms.MULTI_JOIN_THREE_WAY_INNER_JOIN_NESTED_TYPE,
                // Tests for three-way left-inner join combinations with common join key
                MultiJoinTestPrograms.MULTI_JOIN_THREE_WAY_INNER_JOIN,
                MultiJoinTestPrograms.MULTI_JOIN_THREE_WAY_LEFT_INNER_JOIN,
                MultiJoinTestPrograms.MULTI_JOIN_THREE_WAY_INNER_LEFT_JOIN,
                MultiJoinTestPrograms.MULTI_JOIN_THREE_WAY_LEFT_OUTER_JOIN,
                // Tests without common join key
                MultiJoinTestPrograms.MULTI_JOIN_THREE_WAY_INNER_JOIN_NO_COMMON_JOIN_KEY,
                MultiJoinTestPrograms.MULTI_JOIN_THREE_WAY_INNER_LEFT_JOIN_NO_COMMON_JOIN_KEY,
                MultiJoinTestPrograms.MULTI_JOIN_THREE_WAY_LEFT_INNER_JOIN_NO_COMMON_JOIN_KEY,
                MultiJoinTestPrograms.MULTI_JOIN_THREE_WAY_LEFT_JOIN_NO_COMMON_JOIN_KEY,
                MultiJoinTestPrograms.MULTI_JOIN_FOUR_WAY_NO_COMMON_JOIN_KEY,
                // Tests with where
                MultiJoinTestPrograms.MULTI_JOIN_THREE_WAY_INNER_JOIN_WITH_WHERE,
                MultiJoinTestPrograms.MULTI_JOIN_THREE_WAY_LEFT_OUTER_JOIN_WITH_WHERE,
                MultiJoinTestPrograms.MULTI_JOIN_TWO_WAY_INNER_JOIN_WITH_WHERE_IN,
                // Tests with or
                MultiJoinTestPrograms.MULTI_JOIN_THREE_WAY_INNER_JOIN_WITH_OR,
                MultiJoinTestPrograms.MULTI_JOIN_THREE_WAY_INNER_LEFT_JOIN_WITH_OR,
                MultiJoinTestPrograms.MULTI_JOIN_THREE_WAY_LEFT_INNER_JOIN_WITH_OR,
                MultiJoinTestPrograms.MULTI_JOIN_THREE_WAY_LEFT_JOIN_WITH_OR,
                // Tests with IS NOT DISTINCT
                MultiJoinTestPrograms.MULTI_JOIN_LEFT_OUTER_WITH_NULL_KEYS,
                MultiJoinTestPrograms.MULTI_JOIN_NULL_SAFE_JOIN_WITH_NULL_KEYS,
                MultiJoinTestPrograms.MULTI_JOIN_INNER_JOIN_WITH_IS_NOT_DISTINCT,
                // Tests with constant join condition
                MultiJoinTestPrograms.MULTI_JOIN_THREE_WAY_INNER_JOIN_NO_JOIN_KEY,
                MultiJoinTestPrograms.MULTI_JOIN_INNER_LEFT_JOIN_WITH_CONSTANT_CONDITION,
                MultiJoinTestPrograms.MULTI_JOIN_LEFT_INNER_JOIN_WITH_CONSTANT_CONDITION,
                MultiJoinTestPrograms.MULTI_JOIN_LEFT_JOIN_WITH_CONSTANT_CONDITION,
                // Tests with hint
                MultiJoinTestPrograms.MULTI_JOIN_THREE_WAY_INNER_JOIN_WITH_HINT,
                // Complex tests
                MultiJoinTestPrograms.MULTI_JOIN_FOUR_WAY_COMPLEX,
                MultiJoinTestPrograms.MULTI_JOIN_THREE_WAY_INNER_JOIN_COMPLEX,
                MultiJoinTestPrograms.MULTI_JOIN_THREE_WAY_INNER_LEFT_JOIN_COMPLEX,
                MultiJoinTestPrograms.MULTI_JOIN_THREE_WAY_LEFT_INNER_JOIN_COMPLEX,
                MultiJoinTestPrograms.MULTI_JOIN_THREE_WAY_LEFT_JOIN_COMPLEX,
                MultiJoinTestPrograms.MULTI_JOIN_MIXED_CHANGELOG_MODES,
                MultiJoinTestPrograms.MULTI_JOIN_WITH_TIME_ATTRIBUTES_IN_CONDITIONS_MATERIALIZATION,
                MultiJoinTestPrograms.MULTI_JOIN_THREE_WAY_INNER_JOIN_MULTI_KEY_TYPES,
                MultiJoinTestPrograms.MULTI_JOIN_FOUR_WAY_MIXED_JOIN_MULTI_KEY_TYPES_SHUFFLED);
    }
}
