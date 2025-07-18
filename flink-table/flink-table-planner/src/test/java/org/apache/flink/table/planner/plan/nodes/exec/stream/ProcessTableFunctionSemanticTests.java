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

import org.apache.flink.table.planner.plan.nodes.exec.testutils.SemanticTestBase;
import org.apache.flink.table.test.program.TableTestProgram;

import java.util.List;

/** Semantic tests for {@link StreamExecProcessTableFunction}. */
public class ProcessTableFunctionSemanticTests extends SemanticTestBase {

    @Override
    public List<TableTestProgram> programs() {
        return List.of(
                ProcessTableFunctionTestPrograms.PROCESS_SCALAR_ARGS,
                ProcessTableFunctionTestPrograms.PROCESS_SCALAR_ARGS_TABLE_API,
                ProcessTableFunctionTestPrograms.PROCESS_ROW_SEMANTIC_TABLE,
                ProcessTableFunctionTestPrograms.PROCESS_ROW_SEMANTIC_TABLE_TABLE_API,
                ProcessTableFunctionTestPrograms.PROCESS_ROW_SEMANTIC_TABLE_TABLE_API_INLINE,
                ProcessTableFunctionTestPrograms.PROCESS_ROW_SEMANTIC_TABLE_TABLE_API_INLINE_NAMED,
                ProcessTableFunctionTestPrograms.PROCESS_TYPED_ROW_SEMANTIC_TABLE,
                ProcessTableFunctionTestPrograms.PROCESS_TYPED_ROW_SEMANTIC_TABLE_TABLE_API,
                ProcessTableFunctionTestPrograms.PROCESS_SET_SEMANTIC_TABLE,
                ProcessTableFunctionTestPrograms.PROCESS_SET_SEMANTIC_TABLE_TABLE_API,
                ProcessTableFunctionTestPrograms.PROCESS_SET_SEMANTIC_TABLE_TABLE_API_INLINE,
                ProcessTableFunctionTestPrograms.PROCESS_SET_SEMANTIC_TABLE_TABLE_API_INLINE_NAMED,
                ProcessTableFunctionTestPrograms.PROCESS_TYPED_SET_SEMANTIC_TABLE,
                ProcessTableFunctionTestPrograms.PROCESS_TYPED_SET_SEMANTIC_TABLE_TABLE_API,
                ProcessTableFunctionTestPrograms.PROCESS_POJO_ARGS,
                ProcessTableFunctionTestPrograms.PROCESS_EMPTY_ARGS,
                ProcessTableFunctionTestPrograms.PROCESS_ROW_SEMANTIC_TABLE_PASS_THROUGH,
                ProcessTableFunctionTestPrograms.PROCESS_SET_SEMANTIC_TABLE_PASS_THROUGH,
                ProcessTableFunctionTestPrograms.PROCESS_UPDATING_INPUT_RETRACT,
                ProcessTableFunctionTestPrograms.PROCESS_UPDATING_INPUT_UPSERT,
                ProcessTableFunctionTestPrograms.PROCESS_UPDATING_INPUT_ENFORCED_RETRACT,
                ProcessTableFunctionTestPrograms.PROCESS_UPDATING_INPUT_PARTIAL_DELETES,
                ProcessTableFunctionTestPrograms.PROCESS_UPDATING_INPUT_ENFORCED_FULL_DELETES,
                ProcessTableFunctionTestPrograms.PROCESS_UPDATING_OUTPUT_RETRACT,
                ProcessTableFunctionTestPrograms.PROCESS_UPDATING_OUTPUT_UPSERT,
                ProcessTableFunctionTestPrograms.PROCESS_UPDATING_OUTPUT_PARTIAL_DELETES,
                ProcessTableFunctionTestPrograms.PROCESS_UPDATING_OUTPUT_FULL_DELETES,
                ProcessTableFunctionTestPrograms.PROCESS_INVALID_ROW_KIND,
                ProcessTableFunctionTestPrograms.PROCESS_OPTIONAL_PARTITION_BY,
                ProcessTableFunctionTestPrograms.PROCESS_OPTIONAL_PARTITION_BY_TABLE_API,
                ProcessTableFunctionTestPrograms.PROCESS_ATOMIC_WRAPPING,
                ProcessTableFunctionTestPrograms.PROCESS_CONTEXT,
                ProcessTableFunctionTestPrograms.PROCESS_POJO_STATE,
                ProcessTableFunctionTestPrograms.PROCESS_DEFAULT_POJO_STATE,
                ProcessTableFunctionTestPrograms.PROCESS_MULTI_STATE,
                ProcessTableFunctionTestPrograms.PROCESS_CLEARING_STATE,
                ProcessTableFunctionTestPrograms.PROCESS_STATE_TTL,
                ProcessTableFunctionTestPrograms.PROCESS_DESCRIPTOR,
                ProcessTableFunctionTestPrograms.PROCESS_TIME_CONVERSIONS,
                ProcessTableFunctionTestPrograms.PROCESS_TIME_CONVERSIONS_TABLE_API,
                ProcessTableFunctionTestPrograms.PROCESS_REGULAR_TIMESTAMP,
                ProcessTableFunctionTestPrograms.PROCESS_NAMED_TIMERS,
                ProcessTableFunctionTestPrograms.PROCESS_UNNAMED_TIMERS,
                ProcessTableFunctionTestPrograms.PROCESS_LATE_EVENTS,
                ProcessTableFunctionTestPrograms.PROCESS_SCALAR_ARGS_TIME,
                ProcessTableFunctionTestPrograms.PROCESS_OPTIONAL_PARTITION_BY_TIME,
                ProcessTableFunctionTestPrograms.PROCESS_OPTIONAL_ON_TIME,
                ProcessTableFunctionTestPrograms.PROCESS_POJO_STATE_TIME,
                ProcessTableFunctionTestPrograms.PROCESS_CHAINED_TIME,
                ProcessTableFunctionTestPrograms.PROCESS_CHAINED_TIME_TABLE_API,
                ProcessTableFunctionTestPrograms.PROCESS_INVALID_ROW_SEMANTIC_TABLE_TIMERS,
                ProcessTableFunctionTestPrograms.PROCESS_INVALID_PASS_THROUGH_TIMERS,
                ProcessTableFunctionTestPrograms.PROCESS_LIST_STATE,
                ProcessTableFunctionTestPrograms.PROCESS_MAP_STATE,
                ProcessTableFunctionTestPrograms.PROCESS_MULTI_INPUT,
                ProcessTableFunctionTestPrograms.PROCESS_STATEFUL_MULTI_INPUT_WITH_TIMEOUT,
                ProcessTableFunctionTestPrograms.PROCESS_UPDATING_MULTI_INPUT);
    }
}
