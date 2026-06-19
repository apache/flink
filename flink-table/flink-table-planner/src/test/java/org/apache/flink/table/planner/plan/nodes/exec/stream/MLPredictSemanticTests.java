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

import static org.apache.flink.table.planner.plan.nodes.exec.stream.MLPredictTestPrograms.ASYNC_ML_PREDICT_MODEL_API;
import static org.apache.flink.table.planner.plan.nodes.exec.stream.MLPredictTestPrograms.ASYNC_ML_PREDICT_TABLE_API;
import static org.apache.flink.table.planner.plan.nodes.exec.stream.MLPredictTestPrograms.ASYNC_ML_PREDICT_TABLE_API_MAP_EXPRESSION_CONFIG;
import static org.apache.flink.table.planner.plan.nodes.exec.stream.MLPredictTestPrograms.ML_PREDICT_ANON_MODEL_API;
import static org.apache.flink.table.planner.plan.nodes.exec.stream.MLPredictTestPrograms.ML_PREDICT_MODEL_API;
import static org.apache.flink.table.planner.plan.nodes.exec.stream.MLPredictTestPrograms.SYNC_ML_PREDICT_TABLE_API;

/** Semantic tests for {@link StreamExecMLPredictTableFunction} using Table API. */
public class MLPredictSemanticTests extends SemanticTestBase {

    @Override
    public List<TableTestProgram> programs() {
        return List.of(
                SYNC_ML_PREDICT_TABLE_API,
                ASYNC_ML_PREDICT_TABLE_API,
                ASYNC_ML_PREDICT_TABLE_API_MAP_EXPRESSION_CONFIG,
                ML_PREDICT_MODEL_API,
                ASYNC_ML_PREDICT_MODEL_API,
                ML_PREDICT_ANON_MODEL_API);
    }
}
