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

import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.plan.nodes.exec.common.TableSinkTestPrograms;
import org.apache.flink.table.planner.plan.nodes.exec.testutils.BatchRestoreTestBase;
import org.apache.flink.table.planner.plan.utils.ExecNodeMetadataUtil;
import org.apache.flink.table.test.program.TableTestProgram;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Batch Compiled Plan tests for {@link BatchExecSink}. */
public class TableSinkBatchRestoreTest extends BatchRestoreTestBase {

    public TableSinkBatchRestoreTest() {
        super(BatchExecSink.class);
    }

    @Test
    void testExecNodeMetadataContainsRequiredOptions() {
        assertThat(ExecNodeMetadataUtil.consumedOptions(BatchExecSink.class))
                .containsExactlyInAnyOrder(
                        ExecutionConfigOptions.TABLE_EXEC_SINK_NOT_NULL_ENFORCER.key(),
                        ExecutionConfigOptions.TABLE_EXEC_SINK_TYPE_LENGTH_ENFORCER.key(),
                        ExecutionConfigOptions.TABLE_EXEC_SINK_NESTED_CONSTRAINT_ENFORCER.key());
    }

    @Override
    public List<TableTestProgram> programs() {
        return Arrays.asList(
                TableSinkTestPrograms.SINK_BUCKETING_WITH_COUNT,
                TableSinkTestPrograms.SINK_BUCKETING_WITH_KEYS_AND_COUNT,
                TableSinkTestPrograms.SINK_BUCKETING_HASH_WITH_KEYS_AND_COUNT,
                TableSinkTestPrograms.SINK_BUCKETING_HASH_WITH_KEYS_AND_WITHOUT_COUNT,
                TableSinkTestPrograms.SINK_PARTITION,
                TableSinkTestPrograms.SINK_OVERWRITE,
                TableSinkTestPrograms.SINK_WRITING_METADATA,
                // Test needs materialization or other update
                // TableSinkTestPrograms.SINK_NDF_PRIMARY_KEY,
                TableSinkTestPrograms.SINK_PARTIAL_INSERT);
    }
}
