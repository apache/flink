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

import org.apache.flink.table.planner.plan.nodes.exec.testutils.RestoreTestBase;
import org.apache.flink.table.test.program.TableTestProgram;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Restore tests for {@link StreamExecGroupAggregate}. */
public class GroupAggregateRestoreTest extends RestoreTestBase {

    public GroupAggregateRestoreTest() {
        super(StreamExecGroupAggregate.class, Collections.singletonList(StreamExecExchange.class));
    }

    @Override
    public List<TableTestProgram> programs() {
        return Arrays.asList(
                GroupAggregateTestPrograms.GROUP_BY_SIMPLE,
                GroupAggregateTestPrograms.GROUP_BY_SIMPLE_MINI_BATCH,
                GroupAggregateTestPrograms.GROUP_BY_DISTINCT,
                GroupAggregateTestPrograms.GROUP_BY_DISTINCT_MINI_BATCH,
                GroupAggregateTestPrograms.GROUP_BY_UDF_WITH_MERGE,
                GroupAggregateTestPrograms.GROUP_BY_UDF_WITH_MERGE_MINI_BATCH,
                GroupAggregateTestPrograms.GROUP_BY_UDF_WITHOUT_MERGE,
                GroupAggregateTestPrograms.GROUP_BY_UDF_WITHOUT_MERGE_MINI_BATCH,
                GroupAggregateTestPrograms.AGG_WITH_STATE_TTL_HINT);
    }
}
