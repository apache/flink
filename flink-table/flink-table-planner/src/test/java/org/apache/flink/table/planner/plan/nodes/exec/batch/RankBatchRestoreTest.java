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

import org.apache.flink.table.planner.plan.nodes.exec.common.RankTestPrograms;
import org.apache.flink.table.planner.plan.nodes.exec.testutils.BatchRestoreTestBase;
import org.apache.flink.table.test.program.TableTestProgram;

import java.util.Arrays;
import java.util.List;

/** Batch Compiled Plan tests for {@link BatchExecRank}. */
public class RankBatchRestoreTest extends BatchRestoreTestBase {

    public RankBatchRestoreTest() {
        super(BatchExecRank.class);
    }

    @Override
    public List<TableTestProgram> programs() {
        return Arrays.asList(
                RankTestPrograms.RANK_TEST_APPEND_FAST_STRATEGY,
                // org.apache.flink.table.api.TableException: Querying a table in batch mode is
                // currently only possible for INSERT-only table sources. But the source for table
                // 'default_catalog.default_database.MyTable' produces other changelog messages than
                // just INSERT.
                // RankTestPrograms.RANK_TEST_RETRACT_STRATEGY,
                RankTestPrograms.RANK_TEST_UPDATE_FAST_STRATEGY,
                RankTestPrograms.RANK_N_TEST,
                RankTestPrograms.RANK_2_TEST);
    }
}
