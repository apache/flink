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

import org.apache.flink.table.planner.functions.sql.ml.SqlMLEvaluateTableFunction;
import org.apache.flink.table.test.program.ModelTestStep;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Programs for verifying {@link SqlMLEvaluateTableFunction}. */
public class MLEvaluateTestPrograms {

    static final String[] FEATURES_SCHEMA =
            new String[] {"id INT PRIMARY KEY NOT ENFORCED", "feature STRING", "label STRING"};

    static final Row[] FEATURES_BEFORE_DATA =
            new Row[] {
                Row.ofKind(RowKind.INSERT, 1, "Flink", "positive"),
                Row.ofKind(RowKind.INSERT, 2, "Spark", "negative"),
            };

    static final Row[] FEATURES_AFTER_DATA =
            new Row[] {
                Row.ofKind(RowKind.INSERT, 3, "Mysql", "positive"),
                Row.ofKind(RowKind.INSERT, 4, "Postgres", "negative")
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
            new HashMap<>() {
                {
                    put(
                            Row.ofKind(RowKind.INSERT, "Flink"),
                            List.of(Row.ofKind(RowKind.INSERT, "positive")));
                    put(
                            Row.ofKind(RowKind.INSERT, "Spark"),
                            List.of(Row.ofKind(RowKind.INSERT, "positive")));
                    put(
                            Row.ofKind(RowKind.INSERT, "Mysql"),
                            List.of(Row.ofKind(RowKind.INSERT, "positive")));
                    put(
                            Row.ofKind(RowKind.INSERT, "Postgres"),
                            List.of(Row.ofKind(RowKind.INSERT, "positive")));
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

    static final String[] SINK_SCHEMA = new String[] {"`result` MAP<STRING, DOUBLE>"};

    static final SinkTestStep SINK_TABLE =
            SinkTestStep.newBuilder("sink_t")
                    .addSchema(SINK_SCHEMA)
                    .consumedBeforeRestore(
                            "+I[{Accuracy=1.0, Precision=1.0, Recall=1.0, F1=1.0}]",
                            "-U[{Accuracy=1.0, Precision=1.0, Recall=1.0, F1=1.0}]",
                            "+U[{Accuracy=0.5, Precision=0.25, Recall=0.5, F1=0.3333333333333333}]")
                    .consumedAfterRestore(
                            "-U[{Accuracy=0.5, Precision=0.25, Recall=0.5, F1=0.3333333333333333}]",
                            "+U[{Accuracy=0.6666666666666666, Precision=0.3333333333333333, Recall=0.5, F1=0.4}]",
                            "-U[{Accuracy=0.6666666666666666, Precision=0.3333333333333333, Recall=0.5, F1=0.4}]",
                            "+U[{Accuracy=0.5, Precision=0.25, Recall=0.5, F1=0.3333333333333333}]")
                    .build();

    // -------------------------------------------------------------------------------------------

    public static final TableTestProgram SYNC_ML_EVALUATE =
            TableTestProgram.of("sync-ml-evaluate", "ml-evaluate in sync mode.")
                    .setupTableSource(FEATURES_TABLE)
                    .setupModel(SYNC_MODEL)
                    .setupTableSink(SINK_TABLE)
                    .runSql(
                            "INSERT INTO sink_t SELECT * FROM ML_EVALUATE(TABLE features, MODEL chatgpt, DESCRIPTOR(label), DESCRIPTOR(feature), 'classification')")
                    .build();

    public static final TableTestProgram SYNC_ML_EVALUATE_WITH_RUNTIME_CONFIG =
            TableTestProgram.of(
                            "sync-ml-evaluate-with-runtime-config",
                            "ml-evaluate in sync mode with runtime config.")
                    .setupTableSource(FEATURES_TABLE)
                    .setupModel(ASYNC_MODEL)
                    .setupTableSink(SINK_TABLE)
                    .runSql(
                            "INSERT INTO sink_t SELECT * FROM ML_EVALUATE(TABLE features, MODEL chatgpt, DESCRIPTOR(label), DESCRIPTOR(feature), 'classification', MAP['async', 'true'])")
                    .build();
}
