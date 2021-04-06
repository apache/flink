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

package org.apache.flink.table.planner.runtime.stream.sql;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.expressions.utils.ResolvedExpressionMock;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/** Tests for connecting to the {@link DataStream} API. */
@RunWith(Parameterized.class)
public class DataStreamJavaITCase extends AbstractTestBase {

    private StreamExecutionEnvironment env;

    private StreamTableEnvironment tableEnv;

    enum ObjectReuse {
        ENABLED,
        DISABLED
    }

    @Parameters(name = "objectReuse = {0}")
    public static ObjectReuse[] objectReuse() {
        return ObjectReuse.values();
    }

    @Parameter public ObjectReuse objectReuse;

    @Before
    public void before() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        if (objectReuse == ObjectReuse.ENABLED) {
            env.getConfig().enableObjectReuse();
        } else if (objectReuse == ObjectReuse.DISABLED) {
            env.getConfig().disableObjectReuse();
        }
        tableEnv = StreamTableEnvironment.create(env);
    }

    @Test
    public void testFromDataStreamAtomic() {
        final DataStream<Integer> dataStream = env.fromElements(1, 2, 3, 4, 5);

        // wraps the atomic type
        final TableResult result = tableEnv.fromDataStream(dataStream).execute();

        testSchema(result, Column.physical("f0", DataTypes.INT().notNull()));

        testResult(result, Row.of(1), Row.of(2), Row.of(3), Row.of(4), Row.of(5));
    }

    @Test
    public void testToDataStreamAtomic() throws Exception {
        final Table table = tableEnv.fromValues(1, 2, 3, 4, 5);

        testResult(tableEnv.toDataStream(table, Integer.class), 1, 2, 3, 4, 5);
    }

    @Test
    public void testFromDataStreamWithRow() {
        final TypeInformation<Row> typeInfo =
                Types.ROW_NAMED(
                        new String[] {"b", "c", "a"},
                        Types.INT,
                        Types.ROW(Types.BOOLEAN, Types.STRING),
                        Types.MAP(Types.STRING, Types.DOUBLE));

        final Row[] rows =
                new Row[] {
                    Row.of(12, Row.of(false, "hello"), Collections.singletonMap("world", 2.0)),
                    Row.of(null, Row.of(false, null), Collections.singletonMap("world", null))
                };

        final DataStream<Row> dataStream = env.fromCollection(Arrays.asList(rows), typeInfo);

        final TableResult result = tableEnv.fromDataStream(dataStream).execute();

        testSchema(
                result,
                Column.physical("b", DataTypes.INT()),
                Column.physical(
                        "c",
                        DataTypes.ROW(
                                DataTypes.FIELD("f0", DataTypes.BOOLEAN()),
                                DataTypes.FIELD("f1", DataTypes.STRING()))),
                Column.physical("a", DataTypes.MAP(DataTypes.STRING(), DataTypes.DOUBLE())));

        testResult(result, rows);
    }

    @Test
    public void testToDataStreamWithRow() throws Exception {
        final Row[] rows =
                new Row[] {
                    Row.of(12, Row.of(false, "hello"), Collections.singletonMap("world", 2.0)),
                    Row.of(null, Row.of(false, null), Collections.singletonMap("world", 1.0))
                };

        final Table table = tableEnv.fromValues((Object[]) rows);

        testResult(tableEnv.toDataStream(table), rows);
    }

    @Test
    public void testFromAndToDataStreamWithPojo() throws Exception {
        final ComplexPojo[] pojos = {
            ComplexPojo.of(42, "hello", new ImmutablePojo(42.0, null)),
            ComplexPojo.of(42, null, null)
        };

        final DataStream<ComplexPojo> dataStream = env.fromElements(pojos);

        // reorders columns and enriches the immutable type
        final Table table =
                tableEnv.fromDataStream(
                        dataStream,
                        Schema.newBuilder()
                                .column("c", DataTypes.INT())
                                .column("a", DataTypes.STRING())
                                .column("p", DataTypes.of(ImmutablePojo.class))
                                .build());

        testSchema(
                table,
                Column.physical("c", DataTypes.INT()),
                Column.physical("a", DataTypes.STRING()),
                Column.physical(
                        "p",
                        DataTypes.STRUCTURED(
                                ImmutablePojo.class,
                                DataTypes.FIELD("d", DataTypes.DOUBLE()),
                                DataTypes.FIELD("b", DataTypes.BOOLEAN()))));

        tableEnv.createTemporaryView("t", table);

        final TableResult result = tableEnv.executeSql("SELECT p.d, p.b FROM t");

        testResult(result, Row.of(42.0, null), Row.of(null, null));

        testResult(tableEnv.toDataStream(table, ComplexPojo.class), pojos);
    }

    @Test
    public void testFromAndToDataStreamEventTime() throws Exception {
        final DataStream<Tuple3<Long, Integer, String>> dataStream = getWatermarkedDataStream();

        final Table table =
                tableEnv.fromDataStream(
                        dataStream,
                        Schema.newBuilder()
                                .columnByMetadata("rowtime", "TIMESTAMP(3)")
                                .watermark("rowtime", "SOURCE_WATERMARK()")
                                .build());

        testSchema(
                table,
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("f0", DataTypes.BIGINT().notNull()),
                                Column.physical("f1", DataTypes.INT().notNull()),
                                Column.physical("f2", DataTypes.STRING()),
                                Column.metadata("rowtime", DataTypes.TIMESTAMP(3), null, false)),
                        Collections.singletonList(
                                WatermarkSpec.of(
                                        "rowtime",
                                        ResolvedExpressionMock.of(
                                                DataTypes.TIMESTAMP(3), "`SOURCE_WATERMARK`()"))),
                        null));

        tableEnv.createTemporaryView("t", table);

        final TableResult result =
                tableEnv.executeSql(
                        "SELECT f2, SUM(f1) FROM t GROUP BY f2, TUMBLE(rowtime, INTERVAL '0.005' SECOND)");

        testResult(result, Row.of("a", 47), Row.of("c", 1000), Row.of("c", 1000));

        testResult(
                tableEnv.toDataStream(table)
                        .keyBy(k -> k.getField("f2"))
                        .window(TumblingEventTimeWindows.of(Time.milliseconds(5)))
                        .<Row>apply(
                                (key, window, input, out) -> {
                                    int sum = 0;
                                    for (Row row : input) {
                                        sum += row.<Integer>getFieldAs("f1");
                                    }
                                    out.collect(Row.of(key, sum));
                                })
                        .returns(Types.ROW(Types.STRING, Types.INT)),
                Row.of("a", 47),
                Row.of("c", 1000),
                Row.of("c", 1000));
    }

    private DataStream<Tuple3<Long, Integer, String>> getWatermarkedDataStream() {
        final DataStream<Tuple3<Long, Integer, String>> dataStream =
                env.fromCollection(
                        Arrays.asList(
                                Tuple3.of(1L, 42, "a"),
                                Tuple3.of(2L, 5, "a"),
                                Tuple3.of(3L, 1000, "c"),
                                Tuple3.of(100L, 1000, "c")),
                        Types.TUPLE(Types.LONG, Types.INT, Types.STRING));

        return dataStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<Long, Integer, String>>forMonotonousTimestamps()
                        .withTimestampAssigner((ctx) -> (element, recordTimestamp) -> element.f0));
    }

    private static void testSchema(Table table, Column... expectedColumns) {
        assertEquals(ResolvedSchema.of(expectedColumns), table.getResolvedSchema());
    }

    private static void testSchema(Table table, ResolvedSchema expectedSchema) {
        assertEquals(expectedSchema, table.getResolvedSchema());
    }

    private static void testSchema(TableResult result, Column... expectedColumns) {
        assertEquals(ResolvedSchema.of(expectedColumns), result.getResolvedSchema());
    }

    private static void testResult(TableResult result, Row... expectedRows) {
        final List<Row> actualRows = CollectionUtil.iteratorToList(result.collect());
        assertThat(actualRows, containsInAnyOrder(expectedRows));
    }

    @SafeVarargs
    private static <T> void testResult(DataStream<T> dataStream, T... expectedResult)
            throws Exception {
        try (CloseableIterator<T> iterator = dataStream.executeAndCollect()) {
            final List<T> list = CollectionUtil.iteratorToList(iterator);
            assertThat(list, containsInAnyOrder(expectedResult));
        }
    }

    // --------------------------------------------------------------------------------------------
    // Helper classes
    // --------------------------------------------------------------------------------------------

    /** POJO that is a generic type in DataStream API. */
    public static class ImmutablePojo {
        private final Boolean b;

        private final Double d;

        public ImmutablePojo(Double d, Boolean b) {
            this.d = d;
            this.b = b;
        }

        public Boolean getB() {
            return b;
        }

        public Double getD() {
            return d;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ImmutablePojo that = (ImmutablePojo) o;
            return Objects.equals(b, that.b) && Objects.equals(d, that.d);
        }

        @Override
        public int hashCode() {
            return Objects.hash(b, d);
        }
    }

    /** POJO that has no field order in DataStream API. */
    public static class ComplexPojo {
        public int c;

        public String a;

        public ImmutablePojo p;

        static ComplexPojo of(int c, String a, ImmutablePojo p) {
            final ComplexPojo complexPojo = new ComplexPojo();
            complexPojo.c = c;
            complexPojo.a = a;
            complexPojo.p = p;
            return complexPojo;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ComplexPojo that = (ComplexPojo) o;
            return c == that.c && Objects.equals(a, that.a) && Objects.equals(p, that.p);
        }

        @Override
        public int hashCode() {
            return Objects.hash(c, a, p);
        }
    }
}
