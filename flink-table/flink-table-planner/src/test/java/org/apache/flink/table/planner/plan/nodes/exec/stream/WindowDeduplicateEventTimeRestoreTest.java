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
import java.util.List;

/** Restore tests for {@link StreamExecWindowDeduplicate}. */
public class WindowDeduplicateEventTimeRestoreTest extends RestoreTestBase {

    public WindowDeduplicateEventTimeRestoreTest() {
        super(StreamExecWindowDeduplicate.class);
    }

    @Override
    public List<TableTestProgram> programs() {
        return Arrays.asList(
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
                WindowDeduplicateTestPrograms.WINDOW_DEDUPLICATE_ASC_CUMULATE_FIRST_ROW);
    }
}
