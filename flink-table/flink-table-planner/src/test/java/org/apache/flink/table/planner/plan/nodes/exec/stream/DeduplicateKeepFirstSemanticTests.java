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

/** Semantic tests for the built-in DEDUPLICATE_KEEP_FIRST process table function. */
public class DeduplicateKeepFirstSemanticTests extends SemanticTestBase {

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
                DeduplicateKeepFirstTestPrograms.KEYED,
                DeduplicateKeepFirstTestPrograms.TABLE_API,
                DeduplicateKeepFirstTestPrograms.TABLE_API_WITH_ARGS,
                DeduplicateKeepFirstTestPrograms.MULTI_KEY,
                DeduplicateKeepFirstTestPrograms.MULTI_COLUMN_KEY,
                DeduplicateKeepFirstTestPrograms.NULL_KEY,
                DeduplicateKeepFirstTestPrograms.NO_PARTITION_BY,
                DeduplicateKeepFirstTestPrograms.RESET_WITH_ZERO_TTL,
                DeduplicateKeepFirstTestPrograms.NO_TTL_RESET,
                DeduplicateKeepFirstTestPrograms.EVENT_TIME_COLUMN_CLASH,
                DeduplicateKeepFirstTestPrograms.WHOLE_ROW_KEY,
                DeduplicateKeepFirstTestPrograms.EVENT_TIME,
                DeduplicateKeepFirstTestPrograms.EVENT_TIME_MULTI_KEY,
                DeduplicateKeepFirstTestPrograms.EVENT_TIME_LATE_DROPPED,
                DeduplicateKeepFirstTestPrograms.UPDATING_INPUT,
                DeduplicateKeepFirstTestPrograms.UNSEEN_RETRACTION_IGNORED,
                DeduplicateKeepFirstTestPrograms.UPDATING_EVENT_TIME_FAILS,
                DeduplicateKeepFirstTestPrograms.NEGATIVE_TTL_FAILS,
                DeduplicateKeepFirstTestPrograms.NON_LITERAL_TTL_FAILS,
                DeduplicateKeepFirstTestPrograms.NON_LITERAL_RESET_FAILS);
    }
}
