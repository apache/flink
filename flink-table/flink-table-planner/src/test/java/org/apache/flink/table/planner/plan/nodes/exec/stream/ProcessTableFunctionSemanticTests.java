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
                ProcessTableFunctionTestPrograms.PROCESS_TABLE_AS_ROW,
                ProcessTableFunctionTestPrograms.PROCESS_TYPED_TABLE_AS_ROW,
                ProcessTableFunctionTestPrograms.PROCESS_TABLE_AS_SET,
                ProcessTableFunctionTestPrograms.PROCESS_TYPED_TABLE_AS_SET,
                ProcessTableFunctionTestPrograms.PROCESS_POJO_ARGS,
                ProcessTableFunctionTestPrograms.PROCESS_EMPTY_ARGS,
                ProcessTableFunctionTestPrograms.PROCESS_TABLE_AS_ROW_PASS_THROUGH,
                ProcessTableFunctionTestPrograms.PROCESS_TABLE_AS_SET_PASS_THROUGH,
                ProcessTableFunctionTestPrograms.PROCESS_UPDATING_INPUT,
                ProcessTableFunctionTestPrograms.PROCESS_OPTIONAL_PARTITION_BY,
                ProcessTableFunctionTestPrograms.PROCESS_ATOMIC_WRAPPING,
                ProcessTableFunctionTestPrograms.PROCESS_CONTEXT,
                ProcessTableFunctionTestPrograms.PROCESS_POJO_STATE,
                ProcessTableFunctionTestPrograms.PROCESS_DEFAULT_POJO_STATE,
                ProcessTableFunctionTestPrograms.PROCESS_MULTI_STATE,
                ProcessTableFunctionTestPrograms.PROCESS_CLEARING_STATE,
                ProcessTableFunctionTestPrograms.PROCESS_STATE_TTL);
    }
}
