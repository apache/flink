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
import org.apache.flink.table.api.ValidationException;
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

import static org.apache.flink.table.api.Expressions.$;

/** Semantic tests for {@link DataTypes#VARIANT()} type. */
@SuppressWarnings("checkstyle:LocalFinalVariableName")
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

    static final TableTestProgram VARIANT_ARRAY_ACCESS;

    static final TableTestProgram VARIANT_OBJECT_ACCESS;

    static final TableTestProgram VARIANT_NESTED_ACCESS;

    static final TableTestProgram VARIANT_ARRAY_ERROR_ACCESS;

    static final TableTestProgram VARIANT_OBJECT_ERROR_ACCESS;

    public static final SourceTestStep VARIANT_ARRAY_SOURCE =
            SourceTestStep.newBuilder("t")
                    .addSchema("v VARIANT")
                    .producedValues(
                            Row.of(
                                    BUILDER.array()
                                            .add(BUILDER.of(1))
                                            .add(BUILDER.of("hello"))
                                            .add(BUILDER.of(3.14))
                                            .build()),
                            Row.of(
                                    BUILDER.array()
                                            .add(BUILDER.of(10))
                                            .add(BUILDER.of("world"))
                                            .build()),
                            new Row(1))
                    .build();

    public static final SinkTestStep VARIANT_ARRAY_SINK =
            SinkTestStep.newBuilder("sink_t")
                    .addSchema("v1 VARIANT", "v2 VARIANT", "v3 VARIANT")
                    .consumedValues(
                            Row.of(BUILDER.of(1), BUILDER.of("hello"), BUILDER.of(3.14)),
                            Row.of(BUILDER.of(10), BUILDER.of("world"), null),
                            new Row(3))
                    .build();

    public static final SourceTestStep VARIANT_OBJECT_SOURCE =
            SourceTestStep.newBuilder("t")
                    .addSchema("v VARIANT")
                    .producedValues(
                            Row.of(
                                    BUILDER.object()
                                            .add("name", BUILDER.of("Alice"))
                                            .add("age", BUILDER.of(30))
                                            .add("city", BUILDER.of("NYC"))
                                            .build()),
                            Row.of(
                                    BUILDER.object()
                                            .add("name", BUILDER.of("Bob"))
                                            .add("age", BUILDER.of(25))
                                            .build()),
                            new Row(1))
                    .build();

    public static final SinkTestStep VARIANT_OBJECT_SINK =
            SinkTestStep.newBuilder("sink_t")
                    .addSchema("name VARIANT", "age VARIANT", "city VARIANT")
                    .consumedValues(
                            Row.of(BUILDER.of("Alice"), BUILDER.of(30), BUILDER.of("NYC")),
                            Row.of(BUILDER.of("Bob"), BUILDER.of(25), null),
                            new Row(3))
                    .build();

    public static final SourceTestStep VARIANT_NESTED_SOURCE =
            SourceTestStep.newBuilder("t")
                    .addSchema("v VARIANT")
                    .producedValues(
                            Row.of(
                                    BUILDER.object()
                                            .add(
                                                    "users",
                                                    BUILDER.array()
                                                            .add(
                                                                    BUILDER.object()
                                                                            .add(
                                                                                    "id",
                                                                                    BUILDER.of(1))
                                                                            .add(
                                                                                    "name",
                                                                                    BUILDER.of(
                                                                                            "Alice"))
                                                                            .build())
                                                            .build())
                                            .build()),
                            new Row(1))
                    .build();

    public static final SinkTestStep VARIANT_NESTED_SINK =
            SinkTestStep.newBuilder("sink_t")
                    .addSchema("user_id VARIANT", "user_name VARIANT")
                    .consumedValues(Row.of(BUILDER.of(1), BUILDER.of("Alice")), new Row(2))
                    .build();

    static {
        VARIANT_ARRAY_ACCESS =
                TableTestProgram.of(
                                "variant-array-access",
                                "validates variant array access using [] operator in sql and at() in table api")
                        .setupTableSource(VARIANT_ARRAY_SOURCE)
                        .setupTableSink(VARIANT_ARRAY_SINK)
                        .runSql("INSERT INTO sink_t SELECT v[1], v[2], v[3] FROM t")
                        .runTableApi(
                                t ->
                                        t.from("t")
                                                .select(
                                                        $("v").at(1).as("v1"),
                                                        $("v").at(2).as("v2"),
                                                        $("v").at(3).as("v3")),
                                "sink_t")
                        .build();

        VARIANT_OBJECT_ACCESS =
                TableTestProgram.of(
                                "variant-object-access",
                                "validates variant object field access using [] operator in sql and at() in table api")
                        .setupTableSource(VARIANT_OBJECT_SOURCE)
                        .setupTableSink(VARIANT_OBJECT_SINK)
                        .runSql("INSERT INTO sink_t SELECT v['name'], v['age'], v['city'] FROM t")
                        .runTableApi(
                                t ->
                                        t.from("t")
                                                .select(
                                                        $("v").at("name").as("name"),
                                                        $("v").at("age").as("age"),
                                                        $("v").at("city").as("city")),
                                "sink_t")
                        .build();

        VARIANT_NESTED_ACCESS =
                TableTestProgram.of(
                                "variant-nested-access",
                                "validates variant nested access using [] operator in sql and at() in table api")
                        .setupTableSource(VARIANT_NESTED_SOURCE)
                        .setupTableSink(VARIANT_NESTED_SINK)
                        .runSql(
                                "INSERT INTO sink_t SELECT v['users'][1]['id'], v['users'][1]['name'] FROM t")
                        .runTableApi(
                                t ->
                                        t.from("t")
                                                .select(
                                                        $("v").at("users")
                                                                .at(1)
                                                                .at("id")
                                                                .as("user_id"),
                                                        $("v").at("users")
                                                                .at(1)
                                                                .at("name")
                                                                .as("user_name")),
                                "sink_t")
                        .build();

        VARIANT_ARRAY_ERROR_ACCESS =
                TableTestProgram.of(
                                "variant-array-error-access",
                                "validates variant array access using [] operator in sql and at() in table api with string")
                        .setupTableSource(VARIANT_ARRAY_SOURCE)
                        .runFailingSql(
                                "SELECT v['1'], v['2'], v['3'] FROM t",
                                TableRuntimeException.class,
                                "String key access on variant requires an object variant, but a non-object variant was provided.")
                        .runFailingSql(
                                "SELECT v[1.5], v[4.2], v[3.3] FROM t",
                                ValidationException.class,
                                "Cannot apply 'ITEM' to arguments of type 'ITEM(<VARIANT>, <DECIMAL(2, 1)>)'. Supported form(s): <ARRAY>[<INTEGER>]\n"
                                        + "<MAP>[<ANY>]\n"
                                        + "<ROW>[<CHARACTER>|<INTEGER>]\n"
                                        + "<VARIANT>[<CHARACTER>|<INTEGER>]")
                        .build();

        VARIANT_OBJECT_ERROR_ACCESS =
                TableTestProgram.of(
                                "variant-object-error-access",
                                "validates variant object field access using [] operator in sql and at() in table api")
                        .setupTableSource(VARIANT_OBJECT_SOURCE)
                        .runFailingSql(
                                "SELECT v[1], v[2], v[3] FROM t",
                                TableRuntimeException.class,
                                "Integer index access on variant requires an array variant, but a non-array variant was provided.")
                        .runFailingSql(
                                "SELECT v[1.5], v[4.2], v[3.3] FROM t",
                                ValidationException.class,
                                "Cannot apply 'ITEM' to arguments of type 'ITEM(<VARIANT>, <DECIMAL(2, 1)>)'. Supported form(s): <ARRAY>[<INTEGER>]\n"
                                        + "<MAP>[<ANY>]\n"
                                        + "<ROW>[<CHARACTER>|<INTEGER>]\n"
                                        + "<VARIANT>[<CHARACTER>|<INTEGER>]")
                        .build();
    }

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
                VARIANT_AS_AGG_KEY,
                VARIANT_ARRAY_ACCESS,
                VARIANT_OBJECT_ACCESS,
                VARIANT_NESTED_ACCESS,
                VARIANT_ARRAY_ERROR_ACCESS,
                VARIANT_OBJECT_ERROR_ACCESS);
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
