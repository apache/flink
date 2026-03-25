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
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.planner.plan.nodes.exec.testutils.SemanticTestBase;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.types.bitmap.Bitmap;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Semantic tests for {@link DataTypes#BITMAP()} type. */
public class BitmapSemanticTest extends SemanticTestBase {

    @Override
    public List<TableTestProgram> programs() {
        return List.of(
                BUILTIN_AGG,
                BUILTIN_AGG_WITH_RETRACTION,
                BITMAP_AS_UDF_ARG,
                BITMAP_AS_UDAF_ARG,
                BITMAP_AS_UDTF_ARG,
                BITMAP_AS_GROUP_BY_KEY,
                BITMAP_AS_ORDER_BY_KEY,
                BITMAP_AS_DISTINCT_KEY);
    }

    static final TableTestProgram BUILTIN_AGG_WITH_RETRACTION;

    static final TableTestProgram BUILTIN_AGG;

    static {
        Bitmap bm1 = Bitmap.fromArray(new int[] {1, 2});
        Bitmap bm2 = Bitmap.fromArray(new int[] {2, 3});

        BUILTIN_AGG =
                TableTestProgram.of("builtin-agg", "validates builtin agg")
                        .setupTableSource(
                                SourceTestStep.newBuilder("t")
                                        .addSchema("bm BITMAP")
                                        .producedValues(Row.of(bm1), Row.of(bm2), Row.of(bm2))
                                        .build())
                        .setupTableSink(
                                SinkTestStep.newBuilder("sink_t")
                                        .addSchema("fbm BITMAP", "lbm BITMAP", "c BIGINT")
                                        .consumedValues(
                                                Row.ofKind(RowKind.INSERT, bm1, bm1, 1),
                                                Row.ofKind(RowKind.UPDATE_BEFORE, bm1, bm1, 1),
                                                Row.ofKind(RowKind.UPDATE_AFTER, bm1, bm2, 2),
                                                Row.ofKind(RowKind.UPDATE_BEFORE, bm1, bm2, 2),
                                                Row.ofKind(RowKind.UPDATE_AFTER, bm1, bm2, 3))
                                        .build())
                        .runSql(
                                "INSERT INTO sink_t SELECT FIRST_VALUE(bm), LAST_VALUE(bm), COUNT(bm) FROM t")
                        .build();

        BUILTIN_AGG_WITH_RETRACTION =
                TableTestProgram.of(
                                "builtin-agg-with-retraction",
                                "validates builtin agg with retraction")
                        .setupTableSource(
                                SourceTestStep.newBuilder("t")
                                        .addSchema("bm BITMAP")
                                        .addOption("changelog-mode", "I,UB,UA,D")
                                        .producedValues(
                                                Row.ofKind(RowKind.INSERT, bm1),
                                                Row.ofKind(RowKind.INSERT, bm2),
                                                Row.ofKind(RowKind.INSERT, bm2),
                                                Row.ofKind(RowKind.DELETE, bm1))
                                        .build())
                        .setupTableSink(
                                SinkTestStep.newBuilder("sink_t")
                                        .addSchema("fbm BITMAP", "lbm BITMAP", "c BIGINT")
                                        .consumedValues(
                                                Row.ofKind(RowKind.INSERT, bm1, bm1, 1),
                                                Row.ofKind(RowKind.UPDATE_BEFORE, bm1, bm1, 1),
                                                Row.ofKind(RowKind.UPDATE_AFTER, bm1, bm2, 2),
                                                Row.ofKind(RowKind.UPDATE_BEFORE, bm1, bm2, 2),
                                                Row.ofKind(RowKind.UPDATE_AFTER, bm1, bm2, 3),
                                                Row.ofKind(RowKind.UPDATE_BEFORE, bm1, bm2, 3),
                                                Row.ofKind(RowKind.UPDATE_AFTER, bm2, bm2, 2))
                                        .build())
                        .runSql(
                                "INSERT INTO sink_t SELECT FIRST_VALUE(bm), LAST_VALUE(bm), COUNT(bm) FROM t")
                        .build();
    }

    static final TableTestProgram BITMAP_AS_UDF_ARG =
            TableTestProgram.of("bitmap-as-udf-arg", "validates bitmap as udf argument")
                    .setupTemporarySystemFunction("udf", BitmapSemanticTest.MyUdf.class)
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema("bm BITMAP")
                                    .producedValues(
                                            Row.of(Bitmap.fromArray(new int[] {1})),
                                            Row.of(Bitmap.fromArray(new int[] {1, 2})),
                                            Row.of(Bitmap.fromArray(new int[] {1, 2, 3})),
                                            new Row(1))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("c INTEGER")
                                    .consumedValues(Row.of(1), Row.of(2), Row.of(3), new Row(1))
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT udf(bm) FROM t")
                    .build();

    static final TableTestProgram BITMAP_AS_UDAF_ARG =
            TableTestProgram.of("bitmap-as-udaf-arg", "validates bitmap as udaf argument")
                    .setupTemporarySystemFunction("udaf", MyUdaf.class)
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema("bm BITMAP")
                                    .producedValues(
                                            Row.of(Bitmap.fromArray(new int[] {1})),
                                            Row.of(Bitmap.fromArray(new int[] {1, 2})),
                                            Row.of(Bitmap.fromArray(new int[] {1, 2, 3})),
                                            new Row(1))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("c BIGINT")
                                    .consumedValues(
                                            Row.ofKind(RowKind.INSERT, 1),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, 1),
                                            Row.ofKind(RowKind.UPDATE_AFTER, 3),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, 3),
                                            Row.ofKind(RowKind.UPDATE_AFTER, 6))
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT udaf(bm) FROM t")
                    .build();

    static final TableTestProgram BITMAP_AS_UDTF_ARG =
            TableTestProgram.of("bitmap-as-udtf-arg", "validates bitmap as udtf argument")
                    .setupTemporarySystemFunction("udtf", MyUdtf.class)
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema("bm BITMAP")
                                    .producedValues(
                                            Row.of(Bitmap.fromArray(new int[] {0})),
                                            Row.of(Bitmap.fromArray(new int[] {1, 2, 3})))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("v INT")
                                    .consumedValues(Row.of(0), Row.of(1), Row.of(2), Row.of(3))
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT t2.v FROM t, LATERAL TABLE(udtf(bm)) AS t2(v)")
                    .build();

    static final TableTestProgram BITMAP_AS_GROUP_BY_KEY =
            TableTestProgram.of("bitmap-as-group-by-key", "validates bitmap as group by key")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema("bm BITMAP", "v INTEGER")
                                    .producedValues(
                                            Row.of(Bitmap.fromArray(new int[] {1}), 1),
                                            Row.of(Bitmap.fromArray(new int[] {2}), 2),
                                            Row.of(Bitmap.fromArray(new int[] {1}), 2))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("bm STRING", "total INTEGER")
                                    .consumedValues(
                                            Row.ofKind(RowKind.INSERT, "{1}", 1),
                                            Row.ofKind(RowKind.INSERT, "{2}", 2),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, "{1}", 1),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "{1}", 3))
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT CAST(bm AS STRING), SUM(v) AS total FROM t GROUP BY bm")
                    .build();

    static final TableTestProgram BITMAP_AS_ORDER_BY_KEY =
            TableTestProgram.of("bitmap-as-order-by-key", "validates bitmap fails as order by key")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema(
                                            "bm BITMAP",
                                            "ts TIMESTAMP(3)",
                                            "WATERMARK FOR ts AS ts - INTERVAL '1' SECOND")
                                    .producedValues(Row.of(Bitmap.empty(), LocalDateTime.now()))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("ts TIMESTAMP(3)")
                                    .consumedValues(new Row[0])
                                    .build())
                    .runFailingSql(
                            "INSERT INTO sink_t "
                                    + "SELECT FIRST_VALUE(ts) OVER (ORDER BY bm) "
                                    + "FROM TABLE(TUMBLE(TABLE t, DESCRIPTOR(ts), INTERVAL '1' SECOND))",
                            UnsupportedOperationException.class,
                            "Type(BITMAP) is not an orderable data type, "
                                    + "it is not supported as a ORDER_BY/GROUP_BY/JOIN_EQUAL field.")
                    .build();

    static final TableTestProgram BITMAP_AS_DISTINCT_KEY =
            TableTestProgram.of("bitmap-as-distinct-key", "validates bitmap as distinct key")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema("bm BITMAP", "v INTEGER")
                                    .producedValues(
                                            Row.of(Bitmap.fromArray(new int[] {1}), 1),
                                            Row.of(Bitmap.fromArray(new int[] {2}), 2),
                                            Row.of(Bitmap.fromArray(new int[] {2}), 2),
                                            Row.of(Bitmap.fromArray(new int[] {1}), 2))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("cnt BIGINT", "total INTEGER")
                                    .consumedValues(
                                            Row.ofKind(RowKind.INSERT, 1, 1),
                                            Row.ofKind(RowKind.INSERT, 1, 2),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, 1, 2),
                                            Row.ofKind(RowKind.UPDATE_AFTER, 2, 2))
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT COUNT(DISTINCT bm) AS cnt, v FROM t GROUP BY v")
                    .build();

    public static class MyUdf extends ScalarFunction {

        public Integer eval(Bitmap bm) {
            if (bm == null) {
                return null;
            }
            return bm.getCardinality();
        }
    }

    public static class MyUdaf extends AggregateFunction<Long, List<Long>> {

        public Long getValue(List<Long> accumulator) {
            return accumulator.get(0);
        }

        public List<Long> createAccumulator() {
            return new ArrayList<>(List.of(0L));
        }

        public void accumulate(List<Long> accumulator, Bitmap bm) {
            if (bm == null) {
                return;
            }
            accumulator.set(0, accumulator.get(0) + bm.getLongCardinality());
        }
    }

    public static class MyUdtf extends TableFunction<Integer> {

        public void eval(Bitmap bm) {
            if (bm == null) {
                return;
            }
            Arrays.stream(bm.toArray()).forEach(this::collect);
        }
    }
}
