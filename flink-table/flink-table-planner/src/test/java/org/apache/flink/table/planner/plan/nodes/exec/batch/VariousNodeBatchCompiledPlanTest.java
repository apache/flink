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

import org.apache.flink.table.planner.plan.nodes.exec.stream.AsyncCalcTestPrograms;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ChangelogNormalizeTestPrograms;
import org.apache.flink.table.planner.plan.nodes.exec.stream.DeduplicationTestPrograms;
import org.apache.flink.table.planner.plan.nodes.exec.stream.GroupAggregateTestPrograms;
import org.apache.flink.table.planner.plan.nodes.exec.stream.GroupWindowAggregateTestPrograms;
import org.apache.flink.table.planner.plan.nodes.exec.stream.IncrementalGroupAggregateTestPrograms;
import org.apache.flink.table.planner.plan.nodes.exec.stream.IntervalJoinTestPrograms;
import org.apache.flink.table.planner.plan.nodes.exec.stream.MiniBatchAssignerTestPrograms;
import org.apache.flink.table.planner.plan.nodes.exec.stream.TemporalJoinTestPrograms;
import org.apache.flink.table.planner.plan.nodes.exec.stream.TemporalSortTestPrograms;
import org.apache.flink.table.planner.plan.nodes.exec.stream.WatermarkAssignerTestPrograms;
import org.apache.flink.table.planner.plan.nodes.exec.stream.WindowAggregateTestPrograms;
import org.apache.flink.table.planner.plan.nodes.exec.stream.WindowDeduplicateTestPrograms;
import org.apache.flink.table.planner.plan.nodes.exec.stream.WindowJoinTestPrograms;
import org.apache.flink.table.planner.plan.nodes.exec.stream.WindowRankTestPrograms;
import org.apache.flink.table.planner.plan.nodes.exec.stream.WindowTableFunctionTestPrograms;
import org.apache.flink.table.planner.plan.nodes.exec.testutils.BatchCompiledPlanTestBase;
import org.apache.flink.table.test.program.TableTestProgram;

import org.junit.jupiter.api.Disabled;

import java.util.Arrays;
import java.util.List;

/** Batch Compiled Plan tests for various nodes. */
@Disabled
public class VariousNodeBatchCompiledPlanTest extends BatchCompiledPlanTestBase {

    public VariousNodeBatchCompiledPlanTest() {
        super(
                BatchExecCalc
                        .class); // Just naming some node.  This test is running lots of programs.
    }

    @Override
    public List<TableTestProgram> programs() {
        return Arrays.asList(
                AsyncCalcTestPrograms.ASYNC_CALC_UDF_SIMPLE,
                AsyncCalcTestPrograms.ASYNC_CALC_UDF_COMPLEX,
                AsyncCalcTestPrograms.ASYNC_CALC_UDF_NESTED,
                AsyncCalcTestPrograms.ASYNC_CALC_UDF_CONDITION,
                AsyncCalcTestPrograms.ASYNC_CALC_UDF_FAILURE_EXCEPTION,
                ChangelogNormalizeTestPrograms.CHANGELOG_SOURCE,
                ChangelogNormalizeTestPrograms.CHANGELOG_SOURCE_MINI_BATCH,
                ChangelogNormalizeTestPrograms.UPSERT_SOURCE,
                DeduplicationTestPrograms.DEDUPLICATE,
                DeduplicationTestPrograms.DEDUPLICATE_PROCTIME,
                DeduplicationTestPrograms.DEDUPLICATE_DESC,
                GroupAggregateTestPrograms.GROUP_BY_SIMPLE,
                GroupAggregateTestPrograms.GROUP_BY_SIMPLE_MINI_BATCH,
                GroupAggregateTestPrograms.GROUP_BY_DISTINCT,
                GroupAggregateTestPrograms.GROUP_BY_DISTINCT_MINI_BATCH,
                GroupAggregateTestPrograms.GROUP_BY_UDF_WITH_MERGE,
                GroupAggregateTestPrograms.GROUP_BY_UDF_WITH_MERGE_MINI_BATCH,
                GroupAggregateTestPrograms.GROUP_BY_UDF_WITHOUT_MERGE,
                GroupAggregateTestPrograms.GROUP_BY_UDF_WITHOUT_MERGE_MINI_BATCH,
                GroupAggregateTestPrograms.AGG_WITH_STATE_TTL_HINT,
                GroupWindowAggregateTestPrograms.GROUP_TUMBLE_WINDOW_EVENT_TIME,
                GroupWindowAggregateTestPrograms.GROUP_HOP_WINDOW_EVENT_TIME,
                GroupWindowAggregateTestPrograms.GROUP_SESSION_WINDOW_EVENT_TIME,
                GroupWindowAggregateTestPrograms.GROUP_TUMBLE_WINDOW_PROC_TIME,
                GroupWindowAggregateTestPrograms.GROUP_HOP_WINDOW_PROC_TIME,
                GroupWindowAggregateTestPrograms.GROUP_SESSION_WINDOW_PROC_TIME,
                IncrementalGroupAggregateTestPrograms.INCREMENTAL_GROUP_AGGREGATE_SIMPLE,
                IncrementalGroupAggregateTestPrograms.INCREMENTAL_GROUP_AGGREGATE_COMPLEX,
                IntervalJoinTestPrograms.INTERVAL_JOIN_EVENT_TIME,
                IntervalJoinTestPrograms.INTERVAL_JOIN_PROC_TIME,
                IntervalJoinTestPrograms.INTERVAL_JOIN_NEGATIVE_INTERVAL,
                MiniBatchAssignerTestPrograms.MINI_BATCH_ASSIGNER_ROW_TIME,
                MiniBatchAssignerTestPrograms.MINI_BATCH_ASSIGNER_PROC_TIME,
                TemporalJoinTestPrograms.TEMPORAL_JOIN_TABLE_JOIN,
                TemporalJoinTestPrograms.TEMPORAL_JOIN_TABLE_JOIN_NESTED_KEY,
                TemporalJoinTestPrograms.TEMPORAL_JOIN_TABLE_JOIN_KEY_FROM_MAP,
                TemporalJoinTestPrograms.TEMPORAL_JOIN_TEMPORAL_FUNCTION,
                TemporalSortTestPrograms.TEMPORAL_SORT_PROCTIME,
                TemporalSortTestPrograms.TEMPORAL_SORT_ROWTIME,
                WatermarkAssignerTestPrograms.WATERMARK_ASSIGNER_BASIC_FILTER,
                WatermarkAssignerTestPrograms.WATERMARK_ASSIGNER_PUSHDOWN_METADATA,
                WindowAggregateTestPrograms.TUMBLE_WINDOW_EVENT_TIME,
                WindowAggregateTestPrograms.TUMBLE_WINDOW_EVENT_TIME_TWO_PHASE,
                WindowAggregateTestPrograms.TUMBLE_WINDOW_EVENT_TIME_TWO_PHASE_DISTINCT_SPLIT,
                WindowAggregateTestPrograms.TUMBLE_WINDOW_EVENT_TIME_WITH_OFFSET,
                WindowAggregateTestPrograms.TUMBLE_WINDOW_EVENT_TIME_TWO_PHASE_WITH_OFFSET,
                WindowAggregateTestPrograms
                        .TUMBLE_WINDOW_EVENT_TIME_TWO_PHASE_WITH_OFFSET_DISTINCT_SPLIT,
                WindowAggregateTestPrograms.HOP_WINDOW_EVENT_TIME,
                WindowAggregateTestPrograms.HOP_WINDOW_EVENT_TIME_TWO_PHASE,
                WindowAggregateTestPrograms.HOP_WINDOW_EVENT_TIME_TWO_PHASE_DISTINCT_SPLIT,
                WindowAggregateTestPrograms.HOP_WINDOW_EVENT_TIME_WITH_OFFSET,
                WindowAggregateTestPrograms.HOP_WINDOW_EVENT_TIME_TWO_PHASE_WITH_OFFSET,
                WindowAggregateTestPrograms
                        .HOP_WINDOW_EVENT_TIME_TWO_PHASE_WITH_OFFSET_DISTINCT_SPLIT,
                WindowAggregateTestPrograms.CUMULATE_WINDOW_EVENT_TIME,
                WindowAggregateTestPrograms.CUMULATE_WINDOW_EVENT_TIME_TWO_PHASE,
                WindowAggregateTestPrograms.CUMULATE_WINDOW_EVENT_TIME_TWO_PHASE_DISTINCT_SPLIT,
                WindowAggregateTestPrograms.CUMULATE_WINDOW_EVENT_TIME_WITH_OFFSET,
                WindowAggregateTestPrograms.CUMULATE_WINDOW_EVENT_TIME_TWO_PHASE_WITH_OFFSET,
                WindowAggregateTestPrograms.CUMULATE_WINDOW_EVENT_TIME_WITH_OFFSET_DISTINCT_SPLIT,
                WindowAggregateTestPrograms.SESSION_WINDOW_EVENT_TIME,
                WindowAggregateTestPrograms.SESSION_WINDOW_EVENT_TIME_TWO_PHASE,
                WindowAggregateTestPrograms.SESSION_WINDOW_EVENT_TIME_TWO_PHASE_DISTINCT_SPLIT,
                WindowAggregateTestPrograms.SESSION_WINDOW_PARTITION_EVENT_TIME,
                WindowAggregateTestPrograms.SESSION_WINDOW_PARTITION_EVENT_TIME_TWO_PHASE,
                WindowAggregateTestPrograms
                        .SESSION_WINDOW_PARTITION_EVENT_TIME_TWO_PHASE_DISTINCT_SPLIT,
                WindowDeduplicateTestPrograms.WINDOW_DEDUPLICATE_ASC_TUMBLE_FIRST_ROW,
                WindowDeduplicateTestPrograms.WINDOW_DEDUPLICATE_ASC_TUMBLE_FIRST_ROW_CONDITION_ONE,
                WindowDeduplicateTestPrograms
                        .WINDOW_DEDUPLICATE_ASC_TUMBLE_FIRST_ROW_CONDITION_THREE,
                WindowDeduplicateTestPrograms
                        .WINDOW_DEDUPLICATE_ASC_PARTITION_BY_ITEM_TUMBLE_FIRST_ROW,
                WindowDeduplicateTestPrograms.WINDOW_DEDUPLICATE_DESC_TUMBLE_LAST_ROW,
                WindowDeduplicateTestPrograms
                        .WINDOW_DEDUPLICATE_DESC_PARTITION_BY_ITEM_TUMBLE_FIRST_ROW,
                WindowDeduplicateTestPrograms.WINDOW_DEDUPLICATE_ASC_HOP_FIRST_ROW,
                WindowDeduplicateTestPrograms.WINDOW_DEDUPLICATE_ASC_CUMULATE_FIRST_ROW,
                WindowJoinTestPrograms.WINDOW_JOIN_INNER_TUMBLE_EVENT_TIME,
                WindowJoinTestPrograms.WINDOW_JOIN_LEFT_TUMBLE_EVENT_TIME,
                WindowJoinTestPrograms.WINDOW_JOIN_RIGHT_TUMBLE_EVENT_TIME,
                WindowJoinTestPrograms.WINDOW_JOIN_FULL_OUTER_TUMBLE_EVENT_TIME,
                WindowJoinTestPrograms.WINDOW_JOIN_SEMI_TUMBLE_EVENT_TIME,
                WindowJoinTestPrograms.WINDOW_JOIN_ANTI_TUMBLE_EVENT_TIME,
                WindowJoinTestPrograms.WINDOW_JOIN_HOP_EVENT_TIME,
                WindowJoinTestPrograms.WINDOW_JOIN_CUMULATE_EVENT_TIME,
                WindowRankTestPrograms.WINDOW_RANK_TUMBLE_TVF_MIN_TOP_N,
                WindowRankTestPrograms.WINDOW_RANK_TUMBLE_TVF_AGG_MIN_TOP_N,
                WindowRankTestPrograms.WINDOW_RANK_TUMBLE_TVF_MAX_TOP_N,
                WindowRankTestPrograms.WINDOW_RANK_TUMBLE_TVF_AGG_MAX_TOP_N,
                WindowRankTestPrograms.WINDOW_RANK_HOP_TVF_MIN_TOP_N,
                WindowRankTestPrograms.WINDOW_RANK_CUMULATE_TVF_MIN_TOP_N,

                // Processing time Window TableFunction is not supported yet.
                WindowTableFunctionTestPrograms.WINDOW_TABLE_FUNCTION_TUMBLE_TVF_AGG_PROC_TIME,
                WindowTableFunctionTestPrograms.WINDOW_TABLE_FUNCTION_HOP_TVF_AGG_PROC_TIME,
                WindowTableFunctionTestPrograms.WINDOW_TABLE_FUNCTION_CUMULATE_TVF_AGG_PROC_TIME);
    }
}
