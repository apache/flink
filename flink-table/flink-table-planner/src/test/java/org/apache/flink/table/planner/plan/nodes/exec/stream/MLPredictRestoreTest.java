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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.plan.nodes.exec.testutils.RestoreTestBase;
import org.apache.flink.table.planner.plan.utils.ExecNodeMetadataUtil;
import org.apache.flink.table.test.program.TableTestProgram;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.plan.nodes.exec.stream.MLPredictTestPrograms.ASYNC_UNORDERED_ML_PREDICT;
import static org.apache.flink.table.planner.plan.nodes.exec.stream.MLPredictTestPrograms.SYNC_ML_PREDICT;
import static org.apache.flink.table.planner.plan.nodes.exec.stream.MLPredictTestPrograms.SYNC_ML_PREDICT_WITH_RUNTIME_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

/** Restore tests for {@link StreamExecMLPredictTableFunction}. */
public class MLPredictRestoreTest extends RestoreTestBase {

    public MLPredictRestoreTest() {
        super(StreamExecMLPredictTableFunction.class);
    }

    @Test
    public void testExecNodeMetadataContainsRequiredOptions() {
        assertThat(
                        new HashSet<>(
                                Arrays.asList(
                                        Objects.requireNonNull(
                                                ExecNodeMetadataUtil.consumedOptions(
                                                        StreamExecMLPredictTableFunction.class)))))
                .isEqualTo(
                        Arrays.asList(
                                        ExecutionConfigOptions
                                                .TABLE_EXEC_ASYNC_ML_PREDICT_MAX_CONCURRENT_OPERATIONS,
                                        ExecutionConfigOptions.TABLE_EXEC_ASYNC_ML_PREDICT_TIMEOUT,
                                        ExecutionConfigOptions
                                                .TABLE_EXEC_ASYNC_ML_PREDICT_OUTPUT_MODE)
                                .stream()
                                .map(ConfigOption::key)
                                .collect(Collectors.toSet()));
    }

    @Override
    public List<TableTestProgram> programs() {
        return List.of(
                SYNC_ML_PREDICT, ASYNC_UNORDERED_ML_PREDICT, SYNC_ML_PREDICT_WITH_RUNTIME_CONFIG);
    }
}
