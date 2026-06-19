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

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.plan.nodes.exec.testutils.SemanticTestBase;
import org.apache.flink.table.test.program.TableTestProgram;

import java.util.List;

/** Semantic tests for {@link StreamExecMultiJoin}. */
public class MultiJoinSemanticTests extends SemanticTestBase {

    @Override
    protected void applyDefaultEnvironmentOptions(TableConfig config) {
        config.set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_NONDETERMINISTIC_UPDATE_STRATEGY,
                        OptimizerConfigOptions.NonDeterministicUpdateStrategy.TRY_RESOLVE)
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED, true);
    }

    @Override
    public List<TableTestProgram> programs() {
        return List.of(
                MultiJoinTestPrograms.MULTI_JOIN_THREE_WAY_INNER_JOIN,
                MultiJoinTestPrograms.MULTI_JOIN_THREE_WAY_LEFT_OUTER_JOIN,
                MultiJoinTestPrograms.MULTI_JOIN_THREE_WAY_LEFT_OUTER_JOIN_UPDATING,
                MultiJoinTestPrograms.MULTI_JOIN_THREE_WAY_LEFT_OUTER_JOIN_WITH_WHERE,
                MultiJoinTestPrograms.MULTI_JOIN_FOUR_WAY_COMPLEX,
                MultiJoinTestPrograms.MULTI_JOIN_WITH_TIME_ATTRIBUTES_MATERIALIZATION,
                MultiJoinTestPrograms.MULTI_JOIN_THREE_WAY_INNER_JOIN_NO_JOIN_KEY,
                MultiJoinTestPrograms.MULTI_JOIN_FOUR_WAY_NO_COMMON_JOIN_KEY,
                MultiJoinTestPrograms.MULTI_JOIN_THREE_WAY_LEFT_OUTER_JOIN_WITH_CTE,
                MultiJoinTestPrograms.MULTI_JOIN_MIXED_CHANGELOG_MODES,
                MultiJoinTestPrograms.MULTI_JOIN_LEFT_OUTER_WITH_NULL_KEYS,
                MultiJoinTestPrograms.MULTI_JOIN_NULL_SAFE_JOIN_WITH_NULL_KEYS,
                MultiJoinTestPrograms.MULTI_JOIN_WITH_TIME_ATTRIBUTES_IN_CONDITIONS_MATERIALIZATION,
                MultiJoinTestPrograms.MULTI_JOIN_TWO_WAY_INNER_JOIN_WITH_WHERE_IN,
                MultiJoinTestPrograms.MULTI_JOIN_THREE_WAY_INNER_JOIN_MULTI_KEY_TYPES,
                MultiJoinTestPrograms.MULTI_JOIN_FOUR_WAY_MIXED_JOIN_MULTI_KEY_TYPES_SHUFFLED,
                MultiJoinTestPrograms.MULTI_JOIN_THREE_WAY_INNER_JOIN_WITH_HINT);
    }
}
