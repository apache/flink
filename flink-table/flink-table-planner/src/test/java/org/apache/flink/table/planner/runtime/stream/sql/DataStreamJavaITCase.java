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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.EnumTypeInfo;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.expressions.utils.ResolvedExpressionMock;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Either;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Collector;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.time.DayOfWeek;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.sourceWatermark;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;

/** Tests for connecting to the {@link DataStream} API. */
@RunWith(Parameterized.class)
public class DataStreamJavaITCase extends AbstractTestBase {

    private StreamExecutionEnvironment env;

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
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(4);
        if (objectReuse == ObjectReuse.ENABLED) {
            env.getConfig().enableObjectReuse();
        } else if (objectReuse == ObjectReuse.DISABLED) {
            env.getConfig().disableObjectReuse();
        }
    }

    @Test
    public void testFromDataStreamAtomic() {
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        final DataStream<Integer> dataStream = env.fromElements(1, 2, 3, 4, 5);

        // wraps the atomic type
        final TableResult result = tableEnv.fromDataStream(dataStream).execute();

        testSchema(result, Column.physical("f0", DataTypes.INT().notNull()));

        testResult(result, Row.of(1), Row.of(2), Row.of(3), Row.of(4), Row.of(5));
    }

    @Test
    public void testToDataStreamAtomic() throws Exception {
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        final Table table = tableEnv.fromValues(1, 2, 3, 4, 5);

        testResult(tableEnv.toDataStream(table, Integer.class), 1, 2, 3, 4, 5);
    }

    @Test
    public void testFromDataStreamWithRow() {
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

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
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

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
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

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

        final TableResult result = tableEnv.executeSql("SELECT p, p.d, p.b FROM t");

        testResult(
                result,
                Row.of(new ImmutablePojo(42.0, null), 42.0, null),
                Row.of(null, null, null));

        testResult(tableEnv.toDataStream(table, ComplexPojo.class), pojos);
    }

    @Test
    public void testFromAndToDataStreamWithRaw() throws Exception {
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        final List<Tuple2<DayOfWeek, ZoneOffset>> rawRecords =
                Arrays.asList(
                        Tuple2.of(DayOfWeek.MONDAY, ZoneOffset.UTC),
                        Tuple2.of(DayOfWeek.FRIDAY, ZoneOffset.ofHours(5)));

        final DataStream<Tuple2<DayOfWeek, ZoneOffset>> dataStream = env.fromCollection(rawRecords);

        // verify incoming type information
        assertThat(dataStream.getType(), instanceOf(TupleTypeInfo.class));
        final TupleTypeInfo<?> tupleInfo = (TupleTypeInfo<?>) dataStream.getType();
        assertThat(tupleInfo.getFieldTypes()[0], instanceOf(EnumTypeInfo.class));
        assertThat(tupleInfo.getFieldTypes()[1], instanceOf(GenericTypeInfo.class));

        final Table table = tableEnv.fromDataStream(dataStream);

        // verify schema conversion
        final List<DataType> columnDataTypes = table.getResolvedSchema().getColumnDataTypes();
        assertThat(columnDataTypes.get(0).getLogicalType(), instanceOf(RawType.class));
        assertThat(columnDataTypes.get(1).getLogicalType(), instanceOf(RawType.class));

        // test reverse operation
        testResult(
                table.execute(),
                Row.of(DayOfWeek.MONDAY, ZoneOffset.UTC),
                Row.of(DayOfWeek.FRIDAY, ZoneOffset.ofHours(5)));
        testResult(
                tableEnv.toDataStream(table, DataTypes.of(dataStream.getType())),
                rawRecords.toArray(new Tuple2[0]));
    }

    @Test
    public void testFromAndToDataStreamEventTime() throws Exception {
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        final DataStream<Tuple3<Long, Integer, String>> dataStream = getWatermarkedDataStream();

        final Table table =
                tableEnv.fromDataStream(
                        dataStream,
                        Schema.newBuilder()
                                .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
                                // uses SQL expressions
                                .watermark("rowtime", "SOURCE_WATERMARK()")
                                .build());

        testSchema(
                table,
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("f0", DataTypes.BIGINT().notNull()),
                                Column.physical("f1", DataTypes.INT().notNull()),
                                Column.physical("f2", DataTypes.STRING()),
                                Column.metadata(
                                        "rowtime", DataTypes.TIMESTAMP_LTZ(3), null, false)),
                        Collections.singletonList(
                                WatermarkSpec.of(
                                        "rowtime",
                                        ResolvedExpressionMock.of(
                                                DataTypes.TIMESTAMP_LTZ(3),
                                                "`SOURCE_WATERMARK`()"))),
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

    @Test
    public void testFromAndToChangelogStreamEventTime() throws Exception {
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        final DataStream<Tuple3<Long, Integer, String>> dataStream = getWatermarkedDataStream();

        final DataStream<Row> changelogStream =
                dataStream
                        .map(t -> Row.ofKind(RowKind.INSERT, t.f1, t.f2))
                        .returns(Types.ROW(Types.INT, Types.STRING));

        // derive physical columns and add a rowtime
        final Table table =
                tableEnv.fromChangelogStream(
                        changelogStream,
                        Schema.newBuilder()
                                .columnByMetadata("rowtime", DataTypes.TIMESTAMP_LTZ(3))
                                // uses Table API expressions
                                .columnByExpression("computed", $("f1").upperCase())
                                .watermark("rowtime", sourceWatermark())
                                .build());
        tableEnv.createTemporaryView("t", table);

        // access and reorder columns
        final Table reordered = tableEnv.sqlQuery("SELECT computed, rowtime, f0 FROM t");

        // write out the rowtime column with fully declared schema
        final DataStream<Row> result =
                tableEnv.toChangelogStream(
                        reordered,
                        Schema.newBuilder()
                                .column("f1", DataTypes.STRING())
                                .columnByMetadata("rowtime", DataTypes.TIMESTAMP_LTZ(3))
                                // uses Table API expressions
                                .columnByExpression("ignored", $("f1").upperCase())
                                .column("f0", DataTypes.INT())
                                .build());

        // test event time window and field access
        testResult(
                result.keyBy(k -> k.getField("f1"))
                        .window(TumblingEventTimeWindows.of(Time.milliseconds(5)))
                        .<Row>apply(
                                (key, window, input, out) -> {
                                    int sum = 0;
                                    for (Row row : input) {
                                        sum += row.<Integer>getFieldAs("f0");
                                    }
                                    out.collect(Row.of(key, sum));
                                })
                        .returns(Types.ROW(Types.STRING, Types.INT)),
                Row.of("A", 47),
                Row.of("C", 1000),
                Row.of("C", 1000));
    }

    @Test
    public void testFromAndToChangelogStreamRetract() throws Exception {
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        final List<Either<Row, Row>> inputOrOutput =
                Arrays.asList(
                        input(RowKind.INSERT, "bob", 0),
                        output(RowKind.INSERT, "bob", 0),
                        // --
                        input(RowKind.UPDATE_BEFORE, "bob", 0),
                        output(RowKind.DELETE, "bob", 0),
                        // --
                        input(RowKind.UPDATE_AFTER, "bob", 1),
                        output(RowKind.INSERT, "bob", 1),
                        // --
                        input(RowKind.INSERT, "alice", 1),
                        output(RowKind.INSERT, "alice", 1),
                        // --
                        input(RowKind.INSERT, "alice", 1),
                        output(RowKind.UPDATE_BEFORE, "alice", 1),
                        output(RowKind.UPDATE_AFTER, "alice", 2),
                        // --
                        input(RowKind.UPDATE_BEFORE, "alice", 1),
                        output(RowKind.UPDATE_BEFORE, "alice", 2),
                        output(RowKind.UPDATE_AFTER, "alice", 1),
                        // --
                        input(RowKind.UPDATE_AFTER, "alice", 2),
                        output(RowKind.UPDATE_BEFORE, "alice", 1),
                        output(RowKind.UPDATE_AFTER, "alice", 3),
                        // --
                        input(RowKind.UPDATE_BEFORE, "alice", 2),
                        output(RowKind.UPDATE_BEFORE, "alice", 3),
                        output(RowKind.UPDATE_AFTER, "alice", 1),
                        // --
                        input(RowKind.UPDATE_AFTER, "alice", 100),
                        output(RowKind.UPDATE_BEFORE, "alice", 1),
                        output(RowKind.UPDATE_AFTER, "alice", 101));

        final DataStream<Row> changelogStream = env.fromElements(getInput(inputOrOutput));
        tableEnv.createTemporaryView("t", tableEnv.fromChangelogStream(changelogStream));

        final Table result = tableEnv.sqlQuery("SELECT f0, SUM(f1) FROM t GROUP BY f0");

        testResult(result.execute(), getOutput(inputOrOutput));

        testResult(tableEnv.toChangelogStream(result), getOutput(inputOrOutput));
    }

    @Test
    public void testFromChangelogStreamUpsert() {
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        final List<Either<Row, Row>> inputOrOutput =
                Arrays.asList(
                        input(RowKind.INSERT, "bob", 0),
                        output(RowKind.INSERT, "bob", 0),
                        // --
                        input(RowKind.UPDATE_AFTER, "bob", 1),
                        output(RowKind.UPDATE_BEFORE, "bob", 0),
                        output(RowKind.UPDATE_AFTER, "bob", 1),
                        // --
                        input(RowKind.INSERT, "alice", 1),
                        output(RowKind.INSERT, "alice", 1),
                        // --
                        input(RowKind.INSERT, "alice", 1), // no impact
                        // --
                        input(RowKind.UPDATE_AFTER, "alice", 2),
                        output(RowKind.UPDATE_BEFORE, "alice", 1),
                        output(RowKind.UPDATE_AFTER, "alice", 2),
                        // --
                        input(RowKind.UPDATE_AFTER, "alice", 100),
                        output(RowKind.UPDATE_BEFORE, "alice", 2),
                        output(RowKind.UPDATE_AFTER, "alice", 100));

        final DataStream<Row> changelogStream = env.fromElements(getInput(inputOrOutput));
        tableEnv.createTemporaryView(
                "t",
                tableEnv.fromChangelogStream(
                        changelogStream,
                        Schema.newBuilder().primaryKey("f0").build(),
                        ChangelogMode.upsert()));

        final Table result = tableEnv.sqlQuery("SELECT f0, SUM(f1) FROM t GROUP BY f0");

        testResult(result.execute(), getOutput(inputOrOutput));
    }

    @Test
    public void testFromAndToChangelogStreamUpsert() throws Exception {
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        final List<Either<Row, Row>> inputOrOutput =
                Arrays.asList(
                        input(RowKind.INSERT, "bob", 0),
                        output(RowKind.INSERT, "bob", 0),
                        // --
                        input(RowKind.UPDATE_AFTER, "bob", 1),
                        output(RowKind.UPDATE_AFTER, "bob", 1),
                        // --
                        input(RowKind.INSERT, "alice", 1),
                        output(RowKind.INSERT, "alice", 1),
                        // --
                        input(RowKind.INSERT, "alice", 1), // no impact
                        // --
                        input(RowKind.UPDATE_AFTER, "alice", 2),
                        output(RowKind.UPDATE_AFTER, "alice", 2),
                        // --
                        input(RowKind.UPDATE_AFTER, "alice", 100),
                        output(RowKind.UPDATE_AFTER, "alice", 100));

        final DataStream<Row> changelogStream = env.fromElements(getInput(inputOrOutput));
        tableEnv.createTemporaryView(
                "t",
                tableEnv.fromChangelogStream(
                        changelogStream,
                        Schema.newBuilder().primaryKey("f0").build(),
                        ChangelogMode.upsert()));

        final Table result = tableEnv.sqlQuery("SELECT f0, SUM(f1) FROM t GROUP BY f0");

        testResult(
                tableEnv.toChangelogStream(
                        result,
                        Schema.newBuilder().primaryKey("f0").build(),
                        ChangelogMode.upsert()),
                getOutput(inputOrOutput));
    }

    @Test
    public void testToDataStreamCustomEventTime() throws Exception {
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        final TableConfig config = tableEnv.getConfig();

        // session time zone should not have an impact on the conversion
        final ZoneId originalZone = config.getLocalTimeZone();
        config.setLocalTimeZone(ZoneId.of("Europe/Berlin"));

        final LocalDateTime localDateTime1 = LocalDateTime.parse("1970-01-01T00:00:00.000");
        final LocalDateTime localDateTime2 = LocalDateTime.parse("1970-01-01T01:00:00.000");

        final DataStream<Tuple2<LocalDateTime, String>> dataStream =
                env.fromElements(
                        new Tuple2<>(localDateTime1, "alice"), new Tuple2<>(localDateTime2, "bob"));

        final Table table =
                tableEnv.fromDataStream(
                        dataStream,
                        Schema.newBuilder()
                                .column("f0", "TIMESTAMP(3)")
                                .column("f1", "STRING")
                                .watermark("f0", "SOURCE_WATERMARK()")
                                .build());

        testSchema(
                table,
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("f0", DataTypes.TIMESTAMP(3)),
                                Column.physical("f1", DataTypes.STRING())),
                        Collections.singletonList(
                                WatermarkSpec.of(
                                        "f0",
                                        ResolvedExpressionMock.of(
                                                DataTypes.TIMESTAMP(3), "`SOURCE_WATERMARK`()"))),
                        null));

        final DataStream<Long> rowtimeStream =
                tableEnv.toDataStream(table)
                        .process(
                                new ProcessFunction<Row, Long>() {
                                    @Override
                                    public void processElement(
                                            Row value, Context ctx, Collector<Long> out) {
                                        out.collect(ctx.timestamp());
                                    }
                                });

        testResult(
                rowtimeStream,
                localDateTime1.atOffset(ZoneOffset.UTC).toInstant().toEpochMilli(),
                localDateTime2.atOffset(ZoneOffset.UTC).toInstant().toEpochMilli());

        config.setLocalTimeZone(originalZone);
    }

    @Test
    public void testComplexUnifiedPipelineBatch() throws Exception {
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        final Table resultTable = getComplexUnifiedPipeline(env);

        testResult(resultTable.execute(), Row.of("Bob", 1L), Row.of("Alice", 1L));
    }

    @Test
    public void testComplexUnifiedPipelineStreaming() throws Exception {
        final Table resultTable = getComplexUnifiedPipeline(env);

        // more rows than in batch mode due to incremental computations
        testResult(
                resultTable.execute(),
                Row.of("Bob", 1L),
                Row.of("Bob", 2L),
                Row.of("Bob", 3L),
                Row.of("Alice", 1L));
    }

    @Test
    public void testAttachAsDataStream() throws Exception {
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        final String input1DataId =
                TestValuesTableFactory.registerData(Arrays.asList(Row.of(1, "a"), Row.of(2, "b")));

        tableEnv.createTemporaryTable(
                "InputTable1",
                TableDescriptor.forConnector("values")
                        .option("data-id", input1DataId)
                        .schema(
                                Schema.newBuilder()
                                        .column("i", DataTypes.INT())
                                        .column("s", DataTypes.STRING())
                                        .build())
                        .build());

        tableEnv.createTemporaryTable(
                "OutputTable1",
                TableDescriptor.forConnector("values")
                        .schema(
                                Schema.newBuilder()
                                        .column("i", DataTypes.INT())
                                        .column("s", DataTypes.STRING())
                                        .build())
                        .build());

        tableEnv.createTemporaryView("InputTable2", env.fromElements(1, 2, 3));

        tableEnv.createTemporaryTable(
                "OutputTable2",
                TableDescriptor.forConnector("values")
                        .schema(Schema.newBuilder().column("i", DataTypes.INT()).build())
                        .build());

        tableEnv.createStatementSet()
                .addInsert("OutputTable1", tableEnv.from("InputTable1"))
                .addInsert("OutputTable2", tableEnv.from("InputTable2"))
                .attachAsDataStream();

        // submits all source-to-sink pipelines
        testResult(env.fromElements(3, 4, 5), 3, 4, 5);

        assertThat(
                TestValuesTableFactory.getResults("OutputTable1"),
                containsInAnyOrder("+I[1, a]", "+I[2, b]"));

        assertThat(
                TestValuesTableFactory.getResults("OutputTable2"),
                containsInAnyOrder("+I[1]", "+I[2]", "+I[3]"));
    }

    @Test
    public void testMultiChangelogStreamUpsert() throws Exception {
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        createTableFromElements(
                tableEnv,
                "T1",
                ChangelogMode.insertOnly(),
                Schema.newBuilder()
                        .column("pk", "INT NOT NULL")
                        .column("x", "STRING NOT NULL")
                        .primaryKey("pk")
                        .build(),
                Arrays.asList(Types.INT, Types.STRING),
                Row.ofKind(RowKind.INSERT, 1, "1"),
                Row.ofKind(RowKind.INSERT, 2, "2"));

        createTableFromElements(
                tableEnv,
                "T2",
                ChangelogMode.upsert(),
                Schema.newBuilder()
                        .column("pk", "INT NOT NULL")
                        .column("y", "STRING NOT NULL")
                        .column("some_value", "DOUBLE NOT NULL")
                        .primaryKey("pk")
                        .build(),
                Arrays.asList(Types.INT, Types.STRING, Types.DOUBLE),
                Row.ofKind(RowKind.INSERT, 1, "A", 1.0),
                Row.ofKind(RowKind.INSERT, 2, "B", 2.0),
                Row.ofKind(RowKind.UPDATE_AFTER, 1, "A", 1.1),
                Row.ofKind(RowKind.UPDATE_AFTER, 2, "B", 2.1));

        createTableFromElements(
                tableEnv,
                "T3",
                ChangelogMode.insertOnly(),
                Schema.newBuilder()
                        .column("pk1", "STRING NOT NULL")
                        .column("pk2", "STRING NOT NULL")
                        .column("some_other_value", "DOUBLE NOT NULL")
                        .primaryKey("pk1", "pk2")
                        .build(),
                Arrays.asList(Types.STRING, Types.STRING, Types.DOUBLE),
                Row.ofKind(RowKind.INSERT, "1", "A", 10.0),
                Row.ofKind(RowKind.INSERT, "1", "B", 11.0));

        final Table resultTable =
                tableEnv.sqlQuery(
                        "SELECT\n"
                                + "T1.pk,\n"
                                + "T2.some_value * T3.some_other_value,\n"
                                + "T3.pk1,\n"
                                + "T3.pk2\n"
                                + "FROM T1\n"
                                + "LEFT JOIN T2 on T1.pk = T2.pk\n"
                                + "LEFT JOIN T3 ON T1.x = T3.pk1 AND T2.y = T3.pk2");

        final DataStream<Row> resultStream =
                tableEnv.toChangelogStream(
                        resultTable,
                        Schema.newBuilder()
                                .column("pk", "INT NOT NULL")
                                .column("some_calculated_value", "DOUBLE")
                                .column("pk1", "STRING")
                                .column("pk2", "STRING")
                                .primaryKey("pk")
                                .build(),
                        ChangelogMode.upsert());

        testMaterializedResult(
                resultStream, 0, Row.of(2, null, null, null), Row.of(1, 11.0, "1", "A"));
    }

    // --------------------------------------------------------------------------------------------
    // Helper methods
    // --------------------------------------------------------------------------------------------

    private Table getComplexUnifiedPipeline(StreamExecutionEnvironment env) {

        final DataStream<String> allowedNamesStream = env.fromElements("Bob", "Alice");

        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.createTemporaryView(
                "AllowedNamesTable", tableEnv.fromDataStream(allowedNamesStream).as("allowedName"));

        final Table nameCountTable =
                tableEnv.sqlQuery(
                        "SELECT name, COUNT(*) AS c "
                                + "FROM (VALUES ('Bob'), ('Alice'), ('Greg'), ('Bob')) AS NameTable(name) "
                                + "WHERE name IN (SELECT allowedName FROM AllowedNamesTable)"
                                + "GROUP BY name");

        final DataStream<Row> nameCountStream = tableEnv.toChangelogStream(nameCountTable);

        final DataStream<Tuple2<String, Long>> updatesPerNameStream =
                nameCountStream
                        .keyBy(r -> r.<String>getFieldAs("name"))
                        .process(
                                new KeyedProcessFunction<String, Row, Tuple2<String, Long>>() {

                                    ValueState<Long> count;

                                    @Override
                                    public void open(Configuration parameters) {
                                        count =
                                                getRuntimeContext()
                                                        .getState(
                                                                new ValueStateDescriptor<>(
                                                                        "count", Long.class));
                                    }

                                    @Override
                                    public void processElement(
                                            Row r, Context ctx, Collector<Tuple2<String, Long>> out)
                                            throws IOException {
                                        Long currentCount = count.value();
                                        if (currentCount == null) {
                                            currentCount = 0L;
                                        }
                                        final long updatedCount = currentCount + 1;
                                        count.update(updatedCount);

                                        out.collect(Tuple2.of(ctx.getCurrentKey(), updatedCount));
                                    }
                                });

        tableEnv.createTemporaryView("UpdatesPerName", updatesPerNameStream);

        return tableEnv.sqlQuery("SELECT DISTINCT f0, f1 FROM UpdatesPerName");
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

    private static Either<Row, Row> input(RowKind kind, Object... fields) {
        return Either.Left(Row.ofKind(kind, fields));
    }

    private static Row[] getInput(List<Either<Row, Row>> inputOrOutput) {
        return inputOrOutput.stream().filter(Either::isLeft).map(Either::left).toArray(Row[]::new);
    }

    private static Either<Row, Row> output(RowKind kind, Object... fields) {
        return Either.Right(Row.ofKind(kind, fields));
    }

    private static Row[] getOutput(List<Either<Row, Row>> inputOrOutput) {
        return inputOrOutput.stream()
                .filter(Either::isRight)
                .map(Either::right)
                .toArray(Row[]::new);
    }

    private void createTableFromElements(
            StreamTableEnvironment tableEnv,
            String name,
            ChangelogMode changelogMode,
            Schema schema,
            List<TypeInformation<?>> fieldTypeInfo,
            Row... elements) {
        final String[] fieldNames =
                schema.getColumns().stream()
                        .map(Schema.UnresolvedColumn::getName)
                        .toArray(String[]::new);
        final TypeInformation<?>[] fieldTypes = fieldTypeInfo.toArray(new TypeInformation[0]);
        final DataStream<Row> dataStream =
                env.fromElements(elements).returns(Types.ROW_NAMED(fieldNames, fieldTypes));
        final Table table = tableEnv.fromChangelogStream(dataStream, schema, changelogMode);
        tableEnv.createTemporaryView(name, table);
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

    private static void testMaterializedResult(
            DataStream<Row> dataStream, int primaryKeyPos, Row... expectedResult) throws Exception {
        try (CloseableIterator<Row> iterator = dataStream.executeAndCollect()) {
            final List<Row> materializedResult = new ArrayList<>();
            iterator.forEachRemaining(
                    row -> {
                        final RowKind kind = row.getKind();
                        row.setKind(RowKind.INSERT);
                        switch (kind) {
                            case UPDATE_AFTER:
                                final Object primaryKeyValue = row.getField(primaryKeyPos);
                                assert primaryKeyValue != null;
                                materializedResult.removeIf(
                                        r -> primaryKeyValue.equals(r.getField(primaryKeyPos)));
                                // fall through
                            case INSERT:
                                materializedResult.add(row);
                                break;
                            case UPDATE_BEFORE:
                            case DELETE:
                                materializedResult.remove(row);
                                break;
                        }
                    });
            assertThat(materializedResult, containsInAnyOrder(expectedResult));
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
