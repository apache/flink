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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.ModelDescriptor;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.factories.TestValuesModelFactory;
import org.apache.flink.table.test.program.ModelTestStep;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.ColumnList;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.api.Expressions.descriptor;

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

    public static final SourceTestStep SIMPLE_FEATURES_SOURCE =
            SourceTestStep.newBuilder("features")
                    .addSchema(FEATURES_SCHEMA)
                    .producedValues(FEATURES_BEFORE_DATA)
                    .build();

    static final SourceTestStep RESTORE_FEATURES_TABLE =
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

    public static final ModelTestStep SYNC_MODEL =
            ModelTestStep.newBuilder("chatgpt")
                    .addInputSchema(MODEL_INPUT_SCHEMA)
                    .addOutputSchema(MODEL_OUTPUT_SCHEMA)
                    .data(MODEL_DATA)
                    .build();

    public static final ModelTestStep ASYNC_MODEL =
            ModelTestStep.newBuilder("chatgpt")
                    .addInputSchema(MODEL_INPUT_SCHEMA)
                    .addOutputSchema(MODEL_OUTPUT_SCHEMA)
                    .addOption("async", "true")
                    .data(MODEL_DATA)
                    .build();

    // -------------------------------------------------------------------------------------------

    static final String[] SINK_SCHEMA =
            new String[] {"id INT PRIMARY KEY NOT ENFORCED", "feature STRING", "category STRING"};

    static final SinkTestStep RESTORE_SINK_TABLE =
            SinkTestStep.newBuilder("sink_t")
                    .addSchema(SINK_SCHEMA)
                    .consumedBeforeRestore(
                            "+I[1, Flink, Big Data]",
                            "+I[2, Spark, Big Data]",
                            "+I[3, Hive, Big Data]")
                    .consumedAfterRestore("+I[4, Mysql, Database]", "+I[5, Postgres, Database]")
                    .build();

    public static final SinkTestStep SIMPLE_SINK =
            SinkTestStep.newBuilder("sink")
                    .addSchema(SINK_SCHEMA)
                    .consumedValues(
                            "+I[1, Flink, Big Data]",
                            "+I[2, Spark, Big Data]",
                            "+I[3, Hive, Big Data]")
                    .build();

    // -------------------------------------------------------------------------------------------

    public static final TableTestProgram SYNC_ML_PREDICT =
            TableTestProgram.of("sync-ml-predict", "ml-predict in sync mode.")
                    .setupTableSource(RESTORE_FEATURES_TABLE)
                    .setupModel(SYNC_MODEL)
                    .setupTableSink(RESTORE_SINK_TABLE)
                    .runSql(
                            "INSERT INTO sink_t SELECT * FROM ML_PREDICT(TABLE features, MODEL chatgpt, DESCRIPTOR(feature))")
                    .build();

    public static final TableTestProgram ASYNC_UNORDERED_ML_PREDICT =
            TableTestProgram.of("async-unordered-ml-predict", "ml-predict in async unordered mode.")
                    .setupTableSource(RESTORE_FEATURES_TABLE)
                    .setupModel(ASYNC_MODEL)
                    .setupTableSink(RESTORE_SINK_TABLE)
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
                    .setupTableSource(RESTORE_FEATURES_TABLE)
                    .setupModel(ASYNC_MODEL)
                    .setupTableSink(RESTORE_SINK_TABLE)
                    .runSql(
                            "INSERT INTO sink_t SELECT * FROM ML_PREDICT(TABLE features, MODEL chatgpt, DESCRIPTOR(feature), MAP['async', 'false'])")
                    .build();

    public static final TableTestProgram SYNC_ML_PREDICT_TABLE_API =
            TableTestProgram.of(
                            "sync-ml-predict-table-api", "ml-predict in sync mode using Table API.")
                    .setupTableSource(SIMPLE_FEATURES_SOURCE)
                    .setupModel(SYNC_MODEL)
                    .setupTableSink(SIMPLE_SINK)
                    .runTableApi(
                            env ->
                                    env.fromCall(
                                            "ML_PREDICT",
                                            env.from("features").asArgument("INPUT"),
                                            env.fromModel("chatgpt").asArgument("MODEL"),
                                            descriptor("feature").asArgument("ARGS")),
                            "sink")
                    .build();

    public static final TableTestProgram ASYNC_ML_PREDICT_TABLE_API =
            TableTestProgram.of(
                            "async-ml-predict-table-api",
                            "ml-predict in async mode using Table API.")
                    .setupTableSource(SIMPLE_FEATURES_SOURCE)
                    .setupModel(ASYNC_MODEL)
                    .setupTableSink(SIMPLE_SINK)
                    .setupConfig(
                            ExecutionConfigOptions.TABLE_EXEC_ASYNC_ML_PREDICT_OUTPUT_MODE,
                            ExecutionConfigOptions.AsyncOutputMode.ALLOW_UNORDERED)
                    .runTableApi(
                            env ->
                                    env.fromCall(
                                            "ML_PREDICT",
                                            env.from("features").asArgument("INPUT"),
                                            env.fromModel("chatgpt").asArgument("MODEL"),
                                            descriptor("feature").asArgument("ARGS"),
                                            Expressions.lit(
                                                            Map.of("async", "true"),
                                                            DataTypes.MAP(
                                                                            DataTypes.STRING(),
                                                                            DataTypes.STRING())
                                                                    .notNull())
                                                    .asArgument("CONFIG")),
                            "sink")
                    .build();

    public static final TableTestProgram ASYNC_ML_PREDICT_TABLE_API_MAP_EXPRESSION_CONFIG =
            TableTestProgram.of(
                            "async-ml-predict-table-api-map-expression-config",
                            "ml-predict in async mode using Table API and map expression.")
                    .setupTableSource(SIMPLE_FEATURES_SOURCE)
                    .setupModel(ASYNC_MODEL)
                    .setupTableSink(SIMPLE_SINK)
                    .setupConfig(
                            ExecutionConfigOptions.TABLE_EXEC_ASYNC_ML_PREDICT_OUTPUT_MODE,
                            ExecutionConfigOptions.AsyncOutputMode.ALLOW_UNORDERED)
                    .runTableApi(
                            env ->
                                    env.fromCall(
                                            "ML_PREDICT",
                                            env.from("features").asArgument("INPUT"),
                                            env.fromModel("chatgpt").asArgument("MODEL"),
                                            descriptor("feature").asArgument("ARGS"),
                                            Expressions.map(
                                                            "async",
                                                            "true",
                                                            "max-concurrent-operations",
                                                            "10")
                                                    .asArgument("CONFIG")),
                            "sink")
                    .build();

    public static final TableTestProgram ML_PREDICT_MODEL_API =
            TableTestProgram.of("ml-predict-model-api", "ml-predict using model API")
                    .setupTableSource(SIMPLE_FEATURES_SOURCE)
                    .setupModel(SYNC_MODEL)
                    .setupTableSink(SIMPLE_SINK)
                    .runTableApi(
                            env ->
                                    env.fromModel("chatgpt")
                                            .predict(
                                                    env.from("features"), ColumnList.of("feature")),
                            "sink")
                    .build();

    public static final TableTestProgram ASYNC_ML_PREDICT_MODEL_API =
            TableTestProgram.of("async-ml-predict-model-api", "async ml-predict using model API")
                    .setupTableSource(SIMPLE_FEATURES_SOURCE)
                    .setupModel(ASYNC_MODEL)
                    .setupTableSink(SIMPLE_SINK)
                    .setupConfig(
                            ExecutionConfigOptions.TABLE_EXEC_ASYNC_ML_PREDICT_OUTPUT_MODE,
                            ExecutionConfigOptions.AsyncOutputMode.ALLOW_UNORDERED)
                    .runTableApi(
                            env ->
                                    env.fromModel("chatgpt")
                                            .predict(
                                                    env.from("features"),
                                                    ColumnList.of("feature"),
                                                    Map.of(
                                                            "async",
                                                            "true",
                                                            "max-concurrent-operations",
                                                            "10")),
                            "sink")
                    .build();

    public static final TableTestProgram ML_PREDICT_ANON_MODEL_API =
            TableTestProgram.of(
                            "ml-predict-anonymous-model-api",
                            "ml-predict using anonymous model API")
                    .setupTableSource(SIMPLE_FEATURES_SOURCE)
                    .setupTableSink(SIMPLE_SINK)
                    .runTableApi(
                            env ->
                                    env.from(
                                                    ModelDescriptor.forProvider("values")
                                                            .inputSchema(
                                                                    Schema.newBuilder()
                                                                            .column(
                                                                                    "feature",
                                                                                    "STRING")
                                                                            .build())
                                                            .outputSchema(
                                                                    Schema.newBuilder()
                                                                            .column(
                                                                                    "category",
                                                                                    "STRING")
                                                                            .build())
                                                            .option(
                                                                    "data-id",
                                                                    TestValuesModelFactory
                                                                            .registerData(
                                                                                    SYNC_MODEL
                                                                                            .data))
                                                            .build())
                                            .predict(
                                                    env.from("features"), ColumnList.of("feature")),
                            "sink")
                    .build();
}
