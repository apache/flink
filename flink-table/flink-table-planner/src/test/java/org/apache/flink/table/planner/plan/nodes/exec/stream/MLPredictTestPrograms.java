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

import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.test.program.ModelTestStep;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Programs for verifying {@link StreamExecMLPredictTableFunction}. */
public class MLPredictTestPrograms {

    static final String[] FEATURES_SCHEMA =
            new String[] {"id INT PRIMARY KEY NOT ENFORCED", "feature STRING"};

    static final Row[] FEATURES_BEFORE_DATA =
            new Row[] {
                Row.ofKind(RowKind.INSERT, 1, "Flink"),
                Row.ofKind(RowKind.INSERT, 2, "Spark"),
                Row.ofKind(RowKind.INSERT, 3, "Hive")
            };

    static final Row[] FEATURES_AFTER_DATA =
            new Row[] {
                Row.ofKind(RowKind.INSERT, 4, "Mysql"), Row.ofKind(RowKind.INSERT, 5, "Postgres")
            };

    static final SourceTestStep FEATURES_TABLE =
            SourceTestStep.newBuilder("features")
                    .addSchema(FEATURES_SCHEMA)
                    .producedBeforeRestore(FEATURES_BEFORE_DATA)
                    .producedAfterRestore(FEATURES_AFTER_DATA)
                    .build();

    // -------------------------------------------------------------------------------------------

    static final String[] MODEL_INPUT_SCHEMA = new String[] {"feature STRING"};
    static final String[] MODEL_OUTPUT_SCHEMA = new String[] {"category STRING"};

    static final Map<Row, List<Row>> MODEL_DATA =
            new HashMap<Row, List<Row>>() {
                {
                    put(
                            Row.ofKind(RowKind.INSERT, "Flink"),
                            List.of(Row.ofKind(RowKind.INSERT, "Big Data")));
                    put(
                            Row.ofKind(RowKind.INSERT, "Spark"),
                            List.of(Row.ofKind(RowKind.INSERT, "Big Data")));
                    put(
                            Row.ofKind(RowKind.INSERT, "Hive"),
                            List.of(Row.ofKind(RowKind.INSERT, "Big Data")));

                    put(
                            Row.ofKind(RowKind.INSERT, "Mysql"),
                            List.of(Row.ofKind(RowKind.INSERT, "Database")));
                    put(
                            Row.ofKind(RowKind.INSERT, "Postgres"),
                            List.of(Row.ofKind(RowKind.INSERT, "Database")));
                }
            };

    static final ModelTestStep SYNC_MODEL =
            ModelTestStep.newBuilder("chatgpt")
                    .addInputSchema(MODEL_INPUT_SCHEMA)
                    .addOutputSchema(MODEL_OUTPUT_SCHEMA)
                    .data(MODEL_DATA)
                    .build();

    static final ModelTestStep ASYNC_MODEL =
            ModelTestStep.newBuilder("chatgpt")
                    .addInputSchema(MODEL_INPUT_SCHEMA)
                    .addOutputSchema(MODEL_OUTPUT_SCHEMA)
                    .addOption("async", "true")
                    .data(MODEL_DATA)
                    .build();

    // -------------------------------------------------------------------------------------------

    static final String[] SINK_SCHEMA =
            new String[] {"id INT PRIMARY KEY NOT ENFORCED", "feature STRING", "category STRING"};

    static final SinkTestStep SINK_TABLE =
            SinkTestStep.newBuilder("sink_t")
                    .addSchema(SINK_SCHEMA)
                    .consumedBeforeRestore(
                            "+I[1, Flink, Big Data]",
                            "+I[2, Spark, Big Data]",
                            "+I[3, Hive, Big Data]")
                    .consumedAfterRestore("+I[4, Mysql, Database]", "+I[5, Postgres, Database]")
                    .build();

    // -------------------------------------------------------------------------------------------

    public static final TableTestProgram SYNC_ML_PREDICT =
            TableTestProgram.of("sync-ml-predict", "ml-predict in sync mode.")
                    .setupTableSource(FEATURES_TABLE)
                    .setupModel(SYNC_MODEL)
                    .setupTableSink(SINK_TABLE)
                    .runSql(
                            "INSERT INTO sink_t SELECT * FROM ML_PREDICT(TABLE features, MODEL chatgpt, DESCRIPTOR(feature))")
                    .build();

    public static final TableTestProgram ASYNC_UNORDERED_ML_PREDICT =
            TableTestProgram.of("async-unordered-ml-predict", "ml-predict in async unordered mode.")
                    .setupTableSource(FEATURES_TABLE)
                    .setupModel(ASYNC_MODEL)
                    .setupTableSink(SINK_TABLE)
                    .setupConfig(
                            ExecutionConfigOptions.TABLE_EXEC_ASYNC_ML_PREDICT_OUTPUT_MODE,
                            ExecutionConfigOptions.AsyncOutputMode.ALLOW_UNORDERED)
                    .runSql(
                            "INSERT INTO sink_t SELECT * FROM ML_PREDICT(TABLE features, MODEL chatgpt, DESCRIPTOR(feature))")
                    .build();

    public static final TableTestProgram SYNC_ML_PREDICT_WITH_RUNTIME_CONFIG =
            TableTestProgram.of(
                            "sync-ml-predict-with-runtime-options",
                            "ml-predict in sync mode with runtime config.")
                    .setupTableSource(FEATURES_TABLE)
                    .setupModel(ASYNC_MODEL)
                    .setupTableSink(SINK_TABLE)
                    .runSql(
                            "INSERT INTO sink_t SELECT * FROM ML_PREDICT(TABLE features, MODEL chatgpt, DESCRIPTOR(feature), MAP['async', 'false'])")
                    .build();
    ;
}
