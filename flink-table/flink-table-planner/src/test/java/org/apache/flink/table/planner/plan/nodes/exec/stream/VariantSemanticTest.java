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
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.planner.plan.nodes.exec.testutils.SemanticTestBase;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.types.variant.Variant;
import org.apache.flink.types.variant.VariantBuilder;

import java.util.List;

/** Semantic tests for {@link DataTypes#VARIANT()} type. */
public class VariantSemanticTest extends SemanticTestBase {

    static final VariantBuilder BUILDER = Variant.newBuilder();

    static final TableTestProgram PARSE_JSON_OF_NULL =
            TableTestProgram.of(
                            "parse-json-of-null", "validates PARSE_JSON handle null value properly")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema("v STRING")
                                    .producedValues(Row.of("1"), new Row(1))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("v VARIANT")
                                    .consumedValues(Row.of(BUILDER.of((byte) 1)), new Row(1))
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT PARSE_JSON(v) FROM t")
                    .build();

    static final TableTestProgram TRY_PARSE_JSON_OF_NULL =
            TableTestProgram.of(
                            "try-parse-json-of-null",
                            "validates TRY_PARSE_JSON handle null value properly")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema("v STRING")
                                    .producedValues(Row.of("1"), new Row(1))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("v VARIANT")
                                    .consumedValues(Row.of(BUILDER.of((byte) 1)), new Row(1))
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT TRY_PARSE_JSON(v) FROM t")
                    .build();

    static final TableTestProgram PARSE_JSON_FAIL_MALFORMED_JSON =
            TableTestProgram.of(
                            "parse-json-fail-malformed-json",
                            "validates PARSE_JSON throw exception on malformed json")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema("v STRING")
                                    .producedValues(Row.of("1"), Row.of("{"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("v VARIANT")
                                    .consumedValues(Row.of(BUILDER.of((byte) 1)))
                                    .build())
                    .runFailingSql(
                            "INSERT INTO sink_t SELECT PARSE_JSON(v) FROM t",
                            TableRuntimeException.class,
                            "Failed to parse json string")
                    .build();

    static final TableTestProgram TRY_PARSE_JSON_HANDLE_MALFORMED_JSON =
            TableTestProgram.of(
                            "try-parse-json-handle-malformed-json",
                            "validates TRY_PARSE_JSON handle malformed json properly")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema("v STRING")
                                    .producedValues(Row.of("1"), Row.of("{"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("v VARIANT")
                                    .consumedValues(Row.of(BUILDER.of((byte) 1)), new Row(1))
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT TRY_PARSE_JSON(v) FROM t")
                    .build();

    static final TableTestProgram BUILTIN_AGG_WITH_RETRACTION;

    static final TableTestProgram BUILTIN_AGG;

    static {
        Variant v1 = BUILDER.of(1);
        Variant v2 = BUILDER.of(2);

        BUILTIN_AGG =
                TableTestProgram.of("builtin-agg", "validates builtin agg")
                        .setupTableSource(
                                SourceTestStep.newBuilder("t")
                                        .addSchema("v VARIANT")
                                        .producedValues(Row.of(v1), Row.of(v2), Row.of(v2))
                                        .build())
                        .setupTableSink(
                                SinkTestStep.newBuilder("sink_t")
                                        .addSchema(
                                                "fv VARIANT", "lv VARIANT", "c BIGINT", "dc BIGINT")
                                        .consumedValues(
                                                Row.of(v1, v1, 1, 1),
                                                Row.ofKind(RowKind.UPDATE_BEFORE, v1, v1, 1, 1),
                                                Row.ofKind(RowKind.UPDATE_AFTER, v1, v2, 2, 2),
                                                Row.ofKind(RowKind.UPDATE_BEFORE, v1, v2, 2, 2),
                                                Row.ofKind(RowKind.UPDATE_AFTER, v1, v2, 3, 2))
                                        .build())
                        .runSql(
                                "INSERT INTO sink_t SELECT FIRST_VALUE(v), LAST_VALUE(v), COUNT(v), COUNT(DISTINCT v) FROM t")
                        .build();

        BUILTIN_AGG_WITH_RETRACTION =
                TableTestProgram.of(
                                "builtin-agg-with-retraction",
                                "validates builtin agg with retraction")
                        .setupTableSource(
                                SourceTestStep.newBuilder("t")
                                        .addSchema("v VARIANT")
                                        .addOption("changelog-mode", "I,UB,UA,D")
                                        .producedValues(
                                                Row.of(v1),
                                                Row.of(v2),
                                                Row.of(v2),
                                                Row.ofKind(RowKind.DELETE, v1))
                                        .build())
                        .setupTableSink(
                                SinkTestStep.newBuilder("sink_t")
                                        .addSchema(
                                                "fv VARIANT", "lv VARIANT", "c BIGINT", "dc BIGINT")
                                        .consumedValues(
                                                Row.of(v1, v1, 1, 1),
                                                Row.ofKind(RowKind.UPDATE_BEFORE, v1, v1, 1, 1),
                                                Row.ofKind(RowKind.UPDATE_AFTER, v1, v2, 2, 2),
                                                Row.ofKind(RowKind.UPDATE_BEFORE, v1, v2, 2, 2),
                                                Row.ofKind(RowKind.UPDATE_AFTER, v1, v2, 3, 2),
                                                Row.ofKind(RowKind.UPDATE_BEFORE, v1, v2, 3, 2),
                                                Row.ofKind(RowKind.UPDATE_AFTER, v2, v2, 2, 1))
                                        .build())
                        .runSql(
                                "INSERT INTO sink_t SELECT FIRST_VALUE(v), LAST_VALUE(v), COUNT(v), COUNT(DISTINCT v) FROM t")
                        .build();
    }

    static final TableTestProgram VARIANT_AS_UDF_ARG =
            TableTestProgram.of("variant-as-udf-arg", "validates variant as udf argument")
                    .setupTemporarySystemFunction("udf", MyUdf.class)
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema("v VARIANT")
                                    .producedValues(
                                            Row.of(
                                                    BUILDER.object()
                                                            .add("k", BUILDER.of(1))
                                                            .build()),
                                            Row.of(
                                                    BUILDER.object()
                                                            .add("k", BUILDER.of(2))
                                                            .build()),
                                            Row.of(
                                                    BUILDER.object()
                                                            .add("k", BUILDER.ofNull())
                                                            .build()),
                                            new Row(1))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("v INTEGER")
                                    .consumedValues(Row.of(1), Row.of(2), new Row(1), new Row(1))
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT udf(v) FROM t")
                    .build();

    static final TableTestProgram VARIANT_AS_UDAF_ARG =
            TableTestProgram.of("variant-as-udaf-arg", "validates variant as udaf argument")
                    .setupTemporarySystemFunction("udf", MyAggFunc.class)
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema("v VARIANT")
                                    .producedValues(
                                            Row.of(
                                                    BUILDER.object()
                                                            .add("k", BUILDER.of(1))
                                                            .build()),
                                            Row.of(
                                                    BUILDER.object()
                                                            .add("k", BUILDER.of(2))
                                                            .build()),
                                            Row.of(
                                                    BUILDER.object()
                                                            .add("k", BUILDER.ofNull())
                                                            .build()),
                                            new Row(1))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("v BIGINT")
                                    .consumedValues(
                                            Row.of(1),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, 1),
                                            Row.ofKind(RowKind.UPDATE_AFTER, 3))
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT udf(v) FROM t")
                    .build();

    static final TableTestProgram VARIANT_AS_AGG_KEY =
            TableTestProgram.of("variant-as-agg-key", "validates variant as agg key")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema("k VARIANT", "v INTEGER")
                                    .producedValues(
                                            Row.of(BUILDER.of(1), 1),
                                            Row.of(BUILDER.of(2), 2),
                                            Row.of(BUILDER.of(1), 2))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("k VARIANT", "total INTEGER")
                                    .consumedValues(
                                            Row.of(1, 1),
                                            Row.of(2, 2),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, 1, 1),
                                            Row.ofKind(RowKind.UPDATE_AFTER, 1, 3))
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT k, SUM(v) AS total FROM t GROUP BY k")
                    .build();

    @Override
    public List<TableTestProgram> programs() {
        return List.of(
                TRY_PARSE_JSON_OF_NULL,
                PARSE_JSON_OF_NULL,
                PARSE_JSON_FAIL_MALFORMED_JSON,
                TRY_PARSE_JSON_HANDLE_MALFORMED_JSON,
                BUILTIN_AGG,
                BUILTIN_AGG_WITH_RETRACTION,
                VARIANT_AS_UDF_ARG,
                VARIANT_AS_UDAF_ARG,
                VARIANT_AS_AGG_KEY);
    }

    public static class MyUdf extends ScalarFunction {

        public Integer eval(Variant v) {
            if (v == null) {
                return null;
            }
            Variant k = v.getField("k");
            if (k.isNull()) {
                return null;
            }
            return k.getInt();
        }
    }

    public static class MyAggFunc extends AggregateFunction<Long, List<Long>> {
        public Long getValue(List<Long> accumulator) {
            return accumulator.get(0);
        }

        public List<Long> createAccumulator() {
            return List.of(0L);
        }

        public void accumulate(List<Long> accumulator, Variant v) {
            if (v == null) {
                return;
            }
            Variant variant = v.getField("k");
            if (variant == null || variant.isNull()) {
                return;
            }
            accumulator.set(0, accumulator.get(0) + variant.getInt());
        }
    }
}
