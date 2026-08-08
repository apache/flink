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

import org.apache.flink.table.planner.plan.nodes.exec.common.OverAggregateTestPrograms;
import org.apache.flink.table.planner.plan.nodes.exec.testutils.BatchSemanticTestBase;
import org.apache.flink.table.test.program.TableTestProgram;

import java.util.List;

/**
 * Batch semantic tests for RANGE OVER aggregates with TIMESTAMP types. Regression tests for
 * FLINK-25802 / FLINK-30499: the range-bound comparator code generator failed to compile for
 * TIMESTAMP and TIMESTAMP_LTZ ORDER BY columns.
 */
public class OverAggregateBatchSemanticTests extends BatchSemanticTestBase {

    @Override
    public List<TableTestProgram> programs() {
        return List.of(
                OverAggregateTestPrograms.OVER_AGGREGATE_RANGE_TIMESTAMP,
                OverAggregateTestPrograms.OVER_AGGREGATE_RANGE_TIMESTAMP_LTZ);
    }
}
