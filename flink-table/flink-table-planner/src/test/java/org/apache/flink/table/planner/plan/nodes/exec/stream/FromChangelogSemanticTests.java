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

/** Semantic tests for the built-in FROM_CHANGELOG process table function. */
public class FromChangelogSemanticTests extends SemanticTestBase {

    @Override
    protected void applyDefaultEnvironmentOptions(TableConfig config) {
        super.applyDefaultEnvironmentOptions(config);
        config.set(
                OptimizerConfigOptions.TABLE_OPTIMIZER_NONDETERMINISTIC_UPDATE_STRATEGY,
                OptimizerConfigOptions.NonDeterministicUpdateStrategy.IGNORE);
    }

    @Override
    public List<TableTestProgram> programs() {
        return List.of(
                FromChangelogTestPrograms.RETRACT,
                FromChangelogTestPrograms.CUSTOM_OP_MAPPING,
                FromChangelogTestPrograms.CUSTOM_OP_NAME,
                FromChangelogTestPrograms.RETRACT_PARTITION_BY,
                FromChangelogTestPrograms.DELETION_FLAG_PARTITION_BY,
                FromChangelogTestPrograms.SKIP_INVALID_OP_HANDLING,
                FromChangelogTestPrograms.SKIP_NULL_OP_CODE,
                FromChangelogTestPrograms.TABLE_API_DEFAULT,
                FromChangelogTestPrograms.TABLE_API_RETRACT_PARTITION_BY,
                FromChangelogTestPrograms.ROUND_TRIP,
                FromChangelogTestPrograms.INVALID_OP_CODE,
                FromChangelogTestPrograms.NULL_OP_CODE);
    }
}
