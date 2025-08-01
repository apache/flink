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

package org.apache.flink.state.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.state.api.runtime.SavepointLoader;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

@Internal
@FunctionHint(
        output =
                @DataTypeHint(
                        "ROW<checkpoint-id BIGINT NOT NULL, "
                                + "operator-name STRING, "
                                + "operator-uid STRING, operator-uid-hash STRING NOT NULL, "
                                + "operator-parallelism INT NOT NULL, "
                                + "operator-max-parallelism INT NOT NULL, "
                                + "operator-subtask-state-count INT NOT NULL, "
                                + "operator-coordinator-state-size-in-bytes BIGINT NOT NULL, "
                                + "operator-total-size-in-bytes BIGINT NOT NULL>"))
public class SavepointMetadataTableFunction extends TableFunction<Row> {
    public SavepointMetadataTableFunction(SpecializedFunction.SpecializedContext context) {}

    public void eval(String savepointPath) {
        try {
            CheckpointMetadata checkpointMetadata =
                    SavepointLoader.loadSavepointMetadata(savepointPath);

            for (OperatorState operatorState : checkpointMetadata.getOperatorStates()) {
                Row row = Row.withNames();
                row.setField("checkpoint-id", checkpointMetadata.getCheckpointId());
                row.setField("operator-name", operatorState.getOperatorName().orElse(null));
                row.setField("operator-uid", operatorState.getOperatorUid().orElse(null));
                row.setField("operator-uid-hash", operatorState.getOperatorID().toHexString());
                row.setField("operator-parallelism", operatorState.getParallelism());
                row.setField("operator-max-parallelism", operatorState.getMaxParallelism());
                row.setField("operator-subtask-state-count", operatorState.getStates().size());
                row.setField(
                        "operator-coordinator-state-size-in-bytes",
                        operatorState.getCoordinatorState() != null
                                ? operatorState.getCoordinatorState().getStateSize()
                                : 0L);
                row.setField("operator-total-size-in-bytes", operatorState.getCheckpointedSize());
                collect(row);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
