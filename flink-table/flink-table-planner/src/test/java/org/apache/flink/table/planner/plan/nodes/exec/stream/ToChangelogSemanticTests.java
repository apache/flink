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

/** Semantic tests for the built-in TO_CHANGELOG process table function. */
public class ToChangelogSemanticTests extends SemanticTestBase {

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
                ToChangelogTestPrograms.INSERT,
                ToChangelogTestPrograms.RETRACT,
                ToChangelogTestPrograms.UPSERT,
                ToChangelogTestPrograms.RETRACT_PARTITION_BY,
                ToChangelogTestPrograms.CUSTOM_OP_MAPPING,
                ToChangelogTestPrograms.CUSTOM_OP_NAME,
                ToChangelogTestPrograms.TABLE_API_DEFAULT,
                ToChangelogTestPrograms.TABLE_API_RETRACT_PARTITION_BY,
                ToChangelogTestPrograms.LAG_ON_UPSERT_VIA_CHANGELOG,
                ToChangelogTestPrograms.LAG_ON_RETRACT_VIA_CHANGELOG,
                ToChangelogTestPrograms.DELETION_FLAG,
                ToChangelogTestPrograms.INVALID_DESCRIPTOR,
                ToChangelogTestPrograms.INVALID_OP_MAPPING,
                ToChangelogTestPrograms.DUPLICATE_ROW_KIND);
    }
}
