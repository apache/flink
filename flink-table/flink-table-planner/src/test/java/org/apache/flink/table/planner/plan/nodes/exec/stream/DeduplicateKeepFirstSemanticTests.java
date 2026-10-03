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
                DeduplicateKeepFirstTestPrograms.KEYED_KEEP_FIRST,
                DeduplicateKeepFirstTestPrograms.KEYED_KEEP_FIRST_TABLE_API,
                DeduplicateKeepFirstTestPrograms.KEYED_KEEP_FIRST_TABLE_API_WITH_ARGS,
                DeduplicateKeepFirstTestPrograms.MULTI_KEY_INDEPENDENCE,
                DeduplicateKeepFirstTestPrograms.MULTI_COLUMN_KEY,
                DeduplicateKeepFirstTestPrograms.NULL_PARTITION_KEY,
                DeduplicateKeepFirstTestPrograms.NO_PARTITION_BY,
                DeduplicateKeepFirstTestPrograms.RESET_ON_TTL_OFF,
                DeduplicateKeepFirstTestPrograms.RESET_TTL_ON_DUPLICATE_FALSE,
                DeduplicateKeepFirstTestPrograms.INPUT_COLUMN_NAMED_EVENT_TIME,
                DeduplicateKeepFirstTestPrograms.WHOLE_ROW_AS_KEY,
                DeduplicateKeepFirstTestPrograms.EVENT_TIME_KEEP_EARLIEST,
                DeduplicateKeepFirstTestPrograms.EVENT_TIME_LATER_ARRIVAL_EARLIER_WINS,
                DeduplicateKeepFirstTestPrograms.EVENT_TIME_LATE_EVENT_DROPPED,
                DeduplicateKeepFirstTestPrograms.UPDATING_INPUT_SWALLOWED,
                DeduplicateKeepFirstTestPrograms.ON_TIME_WITH_UPDATING_INPUT_FAILS);
    }
}
