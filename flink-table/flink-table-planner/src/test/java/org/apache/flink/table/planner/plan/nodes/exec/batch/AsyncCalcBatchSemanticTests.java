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

package org.apache.flink.table.planner.plan.nodes.exec.batch;

import org.apache.flink.table.planner.plan.nodes.exec.common.AsyncCalcTestPrograms;
import org.apache.flink.table.planner.plan.nodes.exec.testutils.BatchSemanticTestBase;
import org.apache.flink.table.test.program.TableTestProgram;

import java.util.List;

/** Semantic tests for {@link BatchExecAsyncCalc}. */
public class AsyncCalcBatchSemanticTests extends BatchSemanticTestBase {

    @Override
    public List<TableTestProgram> programs() {
        return List.of(
                AsyncCalcTestPrograms.ASYNC_CALC_UDF_IN_FLIGHT_AT_END_OF_INPUT,
                AsyncCalcTestPrograms.ASYNC_CALC_UDF_CORRELATE,
                AsyncCalcTestPrograms.ASYNC_CALC_UDF_AGG_ON_HASH_JOIN,
                AsyncCalcTestPrograms.ASYNC_CALC_UDF_JOIN_INPUT);
    }
}
