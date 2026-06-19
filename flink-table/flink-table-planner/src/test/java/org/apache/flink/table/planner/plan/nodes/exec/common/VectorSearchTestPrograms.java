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

package org.apache.flink.table.planner.plan.nodes.exec.common;

import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecMLPredictTableFunction;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;

/** {@link TableTestProgram} definitions for testing {@link StreamExecMLPredictTableFunction}. */
public class VectorSearchTestPrograms {

    static final String[] SOURCE_SCHEMA =
            new String[] {"id BIGINT", "content STRING", "vector ARRAY<FLOAT>"};

    static final Row[] INPUT_BEFORE_DATA =
            new Row[] {Row.of(1L, "Spark", new Float[] {5f, 12f, 13f})};

    static final Row[] INPUT_AFTER_DATA =
            new Row[] {Row.of(2L, "Flink", new Float[] {-5f, -12f, -13f})};

    static final SourceTestStep SOURCE_TABLE =
            SourceTestStep.newBuilder("src_t")
                    .addSchema(SOURCE_SCHEMA)
                    .producedBeforeRestore(INPUT_BEFORE_DATA)
                    .producedAfterRestore(INPUT_AFTER_DATA)
                    .build();

    // -------------------------------------------------------------------------------------------

    static final String[] VECTOR_TABLE_SCHEMA =
            new String[] {"label STRING", "vector ARRAY<FLOAT>"};

    static final Row[] VECTOR_TABLE_DATA =
            new Row[] {
                Row.of("Batch", new Float[] {5f, 12f, 13f}),
                Row.of("Streaming", new Float[] {-5f, -12f, -13f}),
                Row.of("Big Data", new Float[] {1f, 1f, 0f})
            };

    static final SourceTestStep ASYNC_VECTOR_TABLE =
            SourceTestStep.newBuilder("async_vector_table")
                    .addSchema(VECTOR_TABLE_SCHEMA)
                    .addOption("async", "true")
                    .addOption("enable-vector-search", "true")
                    .producedBeforeRestore(VECTOR_TABLE_DATA)
                    .producedAfterRestore(VECTOR_TABLE_DATA)
                    .build();

    static final SourceTestStep SYNC_VECTOR_TABLE =
            SourceTestStep.newBuilder("sync_vector_table")
                    .addSchema(VECTOR_TABLE_SCHEMA)
                    .addOption("enable-vector-search", "true")
                    .producedBeforeRestore(VECTOR_TABLE_DATA)
                    .producedAfterRestore(VECTOR_TABLE_DATA)
                    .build();

    // -------------------------------------------------------------------------------------------

    static final String[] SINK_SCHEMA =
            new String[] {"id BIGINT", "content STRING", "label STRING"};

    static final SinkTestStep SINK_TABLE =
            SinkTestStep.newBuilder("sink_t")
                    .addSchema(SINK_SCHEMA)
                    .consumedBeforeRestore("+I[1, Spark, Batch]", "+I[1, Spark, Big Data]")
                    .consumedAfterRestore("+I[2, Flink, Streaming]", "+I[2, Flink, Big Data]")
                    .build();

    // -------------------------------------------------------------------------------------------

    public static final TableTestProgram SYNC_VECTOR_SEARCH =
            TableTestProgram.of("sync-vector-search", "vector search in sync mode.")
                    .setupTableSource(SOURCE_TABLE)
                    .setupTableSource(SYNC_VECTOR_TABLE)
                    .setupTableSink(SINK_TABLE)
                    .runSql(
                            "INSERT INTO sink_t SELECT id, content, label FROM src_t, LATERAL TABLE(\n"
                                    + "VECTOR_SEARCH(TABLE sync_vector_table, DESCRIPTOR(vector), src_t.vector, 2))")
                    .build();

    public static final TableTestProgram ASYNC_VECTOR_SEARCH =
            TableTestProgram.of("async-vector-search", "vector search in async mode.")
                    .setupTableSource(SOURCE_TABLE)
                    .setupTableSource(ASYNC_VECTOR_TABLE)
                    .setupTableSink(SINK_TABLE)
                    .runSql(
                            "INSERT INTO sink_t SELECT id, content, label FROM src_t, LATERAL TABLE(\n"
                                    + "VECTOR_SEARCH(TABLE async_vector_table, DESCRIPTOR(vector), src_t.vector, 2))")
                    .build();

    public static final TableTestProgram VECTOR_SEARCH_WITH_RUNTIME_CONFIG =
            TableTestProgram.of(
                            "vector-search-with-runtime-config",
                            "VECTOR_SEARCH with runtime config")
                    .setupTableSource(SOURCE_TABLE)
                    .setupTableSource(ASYNC_VECTOR_TABLE)
                    .setupTableSink(SINK_TABLE)
                    .runSql(
                            "INSERT INTO sink_t SELECT id, content, label FROM src_t, LATERAL TABLE(\n"
                                    + "VECTOR_SEARCH(\n"
                                    + "  SEARCH_TABLE => TABLE async_vector_table,\n"
                                    + "  COLUMN_TO_SEARCH => DESCRIPTOR(vector),\n"
                                    + "  COLUMN_TO_QUERY => src_t.vector,\n"
                                    + "  TOP_K => 2,\n"
                                    + "  CONFIG => MAP['async', 'false']\n"
                                    + "))")
                    .build();
}
