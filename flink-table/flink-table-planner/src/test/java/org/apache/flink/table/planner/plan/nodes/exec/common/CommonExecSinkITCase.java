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

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.transformations.SinkV1Adapter;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.operators.sink.TestSink;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkProvider;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.factories.TableFactoryHarness;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.testutils.junit.SharedObjects;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.types.Row;

import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_SINK_NOT_NULL_ENFORCER;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_SINK_TYPE_LENGTH_ENFORCER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link CommonExecSink}. */
@RunWith(Parameterized.class)
public class CommonExecSinkITCase extends AbstractTestBase {

    private static final int PARALLELISM = 4;

    private final boolean useSinkV2;
    private StreamExecutionEnvironment env;

    @Parameterized.Parameters
    public static Collection<Boolean> useSinkV2() {
        return Arrays.asList(true, false);
    }

    public CommonExecSinkITCase(boolean useSinkV2) {
        this.useSinkV2 = useSinkV2;
    }

    @Before
    public void before() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
    }

    @Rule public final SharedObjects sharedObjects = SharedObjects.create();

    @Test
    public void testStreamRecordTimestampInserterSinkRuntimeProvider()
            throws ExecutionException, InterruptedException {
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        final SharedReference<List<Long>> timestamps = sharedObjects.add(new ArrayList<>());
        final List<Row> rows =
                Arrays.asList(
                        Row.of(1, "foo", Instant.parse("2020-11-10T12:34:56.123Z")),
                        Row.of(2, "foo", Instant.parse("2020-11-10T11:34:56.789Z")),
                        Row.of(3, "foo", Instant.parse("2020-11-11T10:11:22.777Z")),
                        Row.of(4, "foo", Instant.parse("2020-11-11T10:11:23.888Z")));

        final TableDescriptor sourceDescriptor =
                TableFactoryHarness.newBuilder()
                        .schema(schemaStreamRecordTimestampInserter(true))
                        .source(new TestSource(rows))
                        .sink(buildRuntimeSinkProvider(new TestTimestampWriter(timestamps)))
                        .build();
        tableEnv.createTable("T1", sourceDescriptor);
        final String sqlStmt = "INSERT INTO T1 SELECT * FROM T1";
        assertPlan(tableEnv, sqlStmt, true);
        tableEnv.executeSql(sqlStmt).await();
        assertTimestampResults(timestamps, rows);
    }

    @Test
    public void testStreamRecordTimestampInserterDataStreamSinkProvider()
            throws ExecutionException, InterruptedException {
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        final SharedReference<List<Long>> timestamps = sharedObjects.add(new ArrayList<>());
        final List<Row> rows =
                Arrays.asList(
                        Row.of(1, "foo", Instant.parse("2020-11-10T11:34:56.123Z")),
                        Row.of(2, "foo", Instant.parse("2020-11-10T12:34:56.789Z")),
                        Row.of(3, "foo", Instant.parse("2020-11-11T10:11:22.777Z")),
                        Row.of(4, "foo", Instant.parse("2020-11-11T10:11:23.888Z")));

        final SinkFunction<RowData> sinkFunction =
                new SinkFunction<RowData>() {
                    @Override
                    public void invoke(RowData value, Context context) {
                        addElement(timestamps, context.timestamp());
                    }
                };
        final TableDescriptor sourceDescriptor =
                TableFactoryHarness.newBuilder()
                        .schema(schemaStreamRecordTimestampInserter(true))
                        .source(new TestSource(rows))
                        .sink(
                                new TableFactoryHarness.SinkBase() {
                                    @Override
                                    public DataStreamSinkProvider getSinkRuntimeProvider(
                                            DynamicTableSink.Context context) {
                                        return new DataStreamSinkProvider() {
                                            @Override
                                            public DataStreamSink<?> consumeDataStream(
                                                    ProviderContext providerContext,
                                                    DataStream<RowData> dataStream) {
                                                return dataStream.addSink(sinkFunction);
                                            }
                                        };
                                    }
                                })
                        .build();
        tableEnv.createTable("T1", sourceDescriptor);
        final String sqlStmt = "INSERT INTO T1 SELECT * FROM T1";
        assertPlan(tableEnv, sqlStmt, true);
        tableEnv.executeSql(sqlStmt).await();
        Collections.sort(timestamps.get());
        assertTimestampResults(timestamps, rows);
    }

    @Test
    public void testStreamRecordTimestampInserterNotApplied() {
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        final SharedReference<List<Long>> timestamps = sharedObjects.add(new ArrayList<>());
        final List<Row> rows =
                Arrays.asList(
                        Row.of(1, "foo", Instant.parse("2020-11-10T11:34:56.123Z")),
                        Row.of(2, "foo", Instant.parse("2020-11-10T12:34:56.789Z")),
                        Row.of(3, "foo", Instant.parse("2020-11-11T10:11:22.777Z")),
                        Row.of(4, "foo", Instant.parse("2020-11-11T10:11:23.888Z")));

        final TableDescriptor sourceDescriptor =
                TableFactoryHarness.newBuilder()
                        .schema(schemaStreamRecordTimestampInserter(false))
                        .source(new TestSource(rows))
                        .sink(buildRuntimeSinkProvider(new TestTimestampWriter(timestamps)))
                        .build();
        tableEnv.createTable("T1", sourceDescriptor);
        assertPlan(tableEnv, "INSERT INTO T1 SELECT * FROM T1", false);
    }

    @Test
    public void testUnifiedSinksAreUsableWithDataStreamSinkProvider()
            throws ExecutionException, InterruptedException {
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        final SharedReference<List<RowData>> fetched = sharedObjects.add(new ArrayList<>());
        final List<Row> rows = Arrays.asList(Row.of(1), Row.of(2));

        final TableDescriptor sourceDescriptor =
                TableFactoryHarness.newBuilder()
                        .schema(Schema.newBuilder().column("a", INT()).build())
                        .source(new TestSource(rows))
                        .sink(buildDataStreamSinkProvider(fetched))
                        .build();
        tableEnv.createTable("T1", sourceDescriptor);
        final String sqlStmt = "INSERT INTO T1 SELECT * FROM T1";
        tableEnv.executeSql(sqlStmt).await();
        final List<Integer> fetchedRows =
                fetched.get().stream().map(r -> r.getInt(0)).sorted().collect(Collectors.toList());
        assertThat(fetchedRows.get(0).intValue()).isEqualTo(1);
        assertThat(fetchedRows.get(1).intValue()).isEqualTo(2);
    }

    @Test
    public void testCharLengthEnforcer() throws ExecutionException, InterruptedException {
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        final List<Row> rows =
                Arrays.asList(
                        Row.of(1, "Apache Flink", "SQL RuleZ", 11, 111, "SQL"),
                        Row.of(2, "Apache", "SQL", 22, 222, "Flink"),
                        Row.of(3, "Apache", "Flink", 33, 333, "Apache Flink SQL"),
                        Row.of(4, "Flink Project", "SQL or SeQueL?", 44, 444, "Apache Flink SQL"));

        final TableDescriptor sourceDescriptor =
                TableFactoryHarness.newBuilder()
                        .schema(schemaForCharLengthEnforcer())
                        .source(new TestSource(rows))
                        .build();
        tableEnv.createTable("T1", sourceDescriptor);

        // Default config - ignore (no trim)
        TableResult result = tableEnv.executeSql("SELECT * FROM T1");
        result.await();

        final List<Row> results = new ArrayList<>();
        result.collect().forEachRemaining(results::add);
        assertThat(results).containsExactlyInAnyOrderElementsOf(rows);

        // Change config option to "trim_pad", to trim or pad the strings
        // accordingly, based on their type length
        try {
            tableEnv.getConfig()
                    .set(
                            TABLE_EXEC_SINK_TYPE_LENGTH_ENFORCER.key(),
                            ExecutionConfigOptions.TypeLengthEnforcer.TRIM_PAD.name());

            result = tableEnv.executeSql("SELECT * FROM T1");
            result.await();

            final List<Row> expected =
                    Arrays.asList(
                            Row.of(1, "Apache F", "SQL Ru", 11, 111, "SQL"),
                            Row.of(2, "Apache  ", "SQL   ", 22, 222, "Flink"),
                            Row.of(3, "Apache  ", "Flink ", 33, 333, "Apache"),
                            Row.of(4, "Flink Pr", "SQL or", 44, 444, "Apache"));
            final List<Row> resultsTrimmed = new ArrayList<>();
            result.collect().forEachRemaining(resultsTrimmed::add);
            assertThat(resultsTrimmed).containsExactlyInAnyOrderElementsOf(expected);

        } finally {
            tableEnv.getConfig()
                    .set(
                            TABLE_EXEC_SINK_TYPE_LENGTH_ENFORCER.key(),
                            ExecutionConfigOptions.TypeLengthEnforcer.IGNORE.name());
        }
    }

    @Test
    public void testBinaryLengthEnforcer() throws ExecutionException, InterruptedException {
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        final List<Row> rows =
                Arrays.asList(
                        Row.of(
                                1,
                                new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
                                new byte[] {1, 2, 3, 4, 5, 6, 7, 8},
                                11,
                                111,
                                new byte[] {1, 2, 3}),
                        Row.of(
                                2,
                                new byte[] {1, 2, 3, 4, 5},
                                new byte[] {1, 2, 3},
                                22,
                                222,
                                new byte[] {1, 2, 3, 4, 5, 6}),
                        Row.of(
                                3,
                                new byte[] {1, 2, 3, 4, 5, 6},
                                new byte[] {1, 2, 3, 4, 5},
                                33,
                                333,
                                new byte[] {1, 2, 3, 4, 5, 6, 7, 8}),
                        Row.of(
                                4,
                                new byte[] {1, 2, 3, 4, 5, 6, 7, 8},
                                new byte[] {1, 2, 3, 4, 5, 6},
                                44,
                                444,
                                new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}));

        final TableDescriptor sourceDescriptor =
                TableFactoryHarness.newBuilder()
                        .schema(schemaForBinaryLengthEnforcer())
                        .source(new TestSource(rows))
                        .build();
        tableEnv.createTable("T1", sourceDescriptor);

        // Default config - ignore (no trim)
        TableResult result = tableEnv.executeSql("SELECT * FROM T1");
        result.await();

        final List<Row> results = new ArrayList<>();
        result.collect().forEachRemaining(results::add);
        assertThat(results).containsExactlyInAnyOrderElementsOf(rows);

        // Change config option to "trim_pad", to trim or pad the strings
        // accordingly, based on their type length
        try {
            tableEnv.getConfig()
                    .set(
                            TABLE_EXEC_SINK_TYPE_LENGTH_ENFORCER.key(),
                            ExecutionConfigOptions.TypeLengthEnforcer.TRIM_PAD.name());

            result = tableEnv.executeSql("SELECT * FROM T1");
            result.await();

            final List<Row> expected =
                    Arrays.asList(
                            Row.of(
                                    1,
                                    new byte[] {1, 2, 3, 4, 5, 6, 7, 8},
                                    new byte[] {1, 2, 3, 4, 5, 6},
                                    11,
                                    111,
                                    new byte[] {1, 2, 3}),
                            Row.of(
                                    2,
                                    new byte[] {1, 2, 3, 4, 5, 0, 0, 0},
                                    new byte[] {1, 2, 3, 0, 0, 0},
                                    22,
                                    222,
                                    new byte[] {1, 2, 3, 4, 5, 6}),
                            Row.of(
                                    3,
                                    new byte[] {1, 2, 3, 4, 5, 6, 0, 0},
                                    new byte[] {1, 2, 3, 4, 5, 0},
                                    33,
                                    333,
                                    new byte[] {1, 2, 3, 4, 5, 6}),
                            Row.of(
                                    4,
                                    new byte[] {1, 2, 3, 4, 5, 6, 7, 8},
                                    new byte[] {1, 2, 3, 4, 5, 6},
                                    44,
                                    444,
                                    new byte[] {1, 2, 3, 4, 5, 6}));
            final List<Row> resultsTrimmed = new ArrayList<>();
            result.collect().forEachRemaining(resultsTrimmed::add);
            assertThat(resultsTrimmed).containsExactlyInAnyOrderElementsOf(expected);

        } finally {
            tableEnv.getConfig()
                    .set(
                            TABLE_EXEC_SINK_TYPE_LENGTH_ENFORCER.key(),
                            ExecutionConfigOptions.TypeLengthEnforcer.IGNORE.name());
        }
    }

    @Test
    public void testNullEnforcer() throws ExecutionException, InterruptedException {
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        final List<Row> rows =
                Arrays.asList(
                        Row.of(1, "Apache", 11),
                        Row.of(2, null, 22),
                        Row.of(null, "Flink", 33),
                        Row.of(null, null, 44));

        final SharedReference<List<RowData>> results = sharedObjects.add(new ArrayList<>());
        tableEnv.createTable(
                "T1",
                TableFactoryHarness.newBuilder()
                        .schema(schemaForNotNullEnforcer())
                        .source(new TestSource(rows))
                        .sink(buildRuntimeSinkProvider(new RecordWriter(results)))
                        .build());

        // Default config - ignore (no trim)
        assertThatThrownBy(() -> tableEnv.executeSql("INSERT INTO T1 SELECT * FROM T1").await())
                .isInstanceOf(ExecutionException.class)
                .satisfies(
                        anyCauseMatches(
                                "Column 'b' is NOT NULL, however, a null value is being written into it. "
                                        + "You can set job configuration 'table.exec.sink.not-null-enforcer'='DROP' "
                                        + "to suppress this exception and drop such records silently."));

        // Test not including a NOT NULL column
        results.get().clear();
        assertThatThrownBy(
                        () ->
                                tableEnv.executeSql("INSERT INTO T1(a, b) SELECT (a, b) FROM T1")
                                        .await())
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "SQL validation failed. At line 0, column 0: Column 'c' has no default "
                                + "value and does not allow NULLs");

        // Change config option to "drop", to drop the columns instead of throwing errors
        try {
            tableEnv.getConfig()
                    .set(
                            TABLE_EXEC_SINK_NOT_NULL_ENFORCER.key(),
                            ExecutionConfigOptions.NotNullEnforcer.DROP.name());

            results.get().clear();
            tableEnv.executeSql("INSERT INTO T1 SELECT * FROM T1").await();
            assertThat(results.get().size()).isEqualTo(2);
            assertThat(results.get().get(0).getInt(0)).isEqualTo(1);
            assertThat(results.get().get(0).getString(1).toString()).isEqualTo("Apache");
            assertThat(results.get().get(0).getInt(2)).isEqualTo(11);
            assertThat(results.get().get(1).isNullAt(0)).isTrue();
            assertThat(results.get().get(1).getString(1).toString()).isEqualTo("Flink");
            assertThat(results.get().get(1).getInt(2)).isEqualTo(33);
        } finally {
            tableEnv.getConfig()
                    .set(
                            TABLE_EXEC_SINK_NOT_NULL_ENFORCER.key(),
                            ExecutionConfigOptions.NotNullEnforcer.ERROR.name());
        }
    }

    @Test
    public void testFromValuesWatermarkPropagation() throws Exception {
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        final SharedReference<List<Long>> watermarks = sharedObjects.add(new ArrayList<>());
        final SinkFunction<RowData> sinkFunction =
                new SinkFunction<RowData>() {
                    @Override
                    public void writeWatermark(
                            org.apache.flink.api.common.eventtime.Watermark watermark) {
                        addElement(watermarks, watermark.getTimestamp());
                    }
                };
        final TableDescriptor sinkDescriptor =
                TableFactoryHarness.newBuilder()
                        .sink(
                                new TableFactoryHarness.SinkBase() {
                                    @Override
                                    public DataStreamSinkProvider getSinkRuntimeProvider(
                                            DynamicTableSink.Context context) {
                                        return new DataStreamSinkProvider() {
                                            @Override
                                            public DataStreamSink<?> consumeDataStream(
                                                    ProviderContext providerContext,
                                                    DataStream<RowData> dataStream) {
                                                return dataStream.addSink(sinkFunction);
                                            }
                                        };
                                    }
                                })
                        .build();

        final Table source =
                tableEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("a", DataTypes.INT())),
                        Row.of(1),
                        Row.of(2),
                        Row.of(3));
        source.executeInsert(sinkDescriptor).await();
        assertThat(watermarks.get().size()).isEqualTo(env.getParallelism());
        for (Long watermark : watermarks.get()) {
            assertThat(watermark).isEqualTo(Watermark.MAX_WATERMARK.getTimestamp());
        }
    }

    private static <T> void addElement(SharedReference<List<T>> elements, T element) {
        elements.applySync(l -> l.add(element));
    }

    private static TestSink<RowData> buildRecordWriterTestSink(
            TestSink.DefaultSinkWriter<RowData> writer) {
        return TestSink.newBuilder()
                .setWriter(writer)
                .setCommittableSerializer(TestSink.StringCommittableSerializer.INSTANCE)
                .build();
    }

    private TableFactoryHarness.SinkBase buildRuntimeSinkProvider(
            TestSink.DefaultSinkWriter<RowData> writer) {
        return new TableFactoryHarness.SinkBase() {
            @Override
            public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
                TestSink<RowData> sink = buildRecordWriterTestSink(writer);
                if (useSinkV2) {
                    return SinkV2Provider.of(SinkV1Adapter.wrap(sink));
                }
                return SinkProvider.of(sink);
            }
        };
    }

    @NotNull
    private TableFactoryHarness.SinkBase buildDataStreamSinkProvider(
            SharedReference<List<RowData>> fetched) {
        return new TableFactoryHarness.SinkBase() {
            @Override
            public DataStreamSinkProvider getSinkRuntimeProvider(Context context) {
                return new DataStreamSinkProvider() {
                    @Override
                    public DataStreamSink<?> consumeDataStream(
                            ProviderContext providerContext, DataStream<RowData> dataStream) {
                        TestSink<RowData> sink =
                                buildRecordWriterTestSink(new RecordWriter(fetched));
                        if (useSinkV2) {
                            return dataStream.sinkTo(SinkV1Adapter.wrap(sink));
                        }
                        return dataStream.sinkTo(sink);
                    }
                };
            }
        };
    }

    private static void assertPlan(
            StreamTableEnvironment tableEnv,
            String sql,
            boolean containsStreamRecordTimestampInserter) {
        final String explainStr = tableEnv.explainSql(sql, ExplainDetail.JSON_EXECUTION_PLAN);
        final String containedStr = "StreamRecordTimestampInserter(rowtime field: 2";
        if (containsStreamRecordTimestampInserter) {
            assertThat(explainStr).contains(containedStr);
        } else {
            assertThat(explainStr).doesNotContain(containedStr);
        }
    }

    private static Schema schemaStreamRecordTimestampInserter(boolean withWatermark) {
        final Schema.Builder builder =
                Schema.newBuilder()
                        .column("a", "INT")
                        .column("b", "STRING")
                        .column("ts", "TIMESTAMP_LTZ(3)");
        if (withWatermark) {
            builder.watermark("ts", "ts");
        }
        return builder.build();
    }

    private static Schema schemaForCharLengthEnforcer() {
        return Schema.newBuilder()
                .column("a", "INT")
                .column("b", "CHAR(8)")
                .column("c", "CHAR(6)")
                .column("d", "INT")
                .column("e", "INT")
                .column("f", "VARCHAR(6)")
                .build();
    }

    private static Schema schemaForBinaryLengthEnforcer() {
        return Schema.newBuilder()
                .column("a", "INT")
                .column("b", "BINARY(8)")
                .column("c", "BINARY(6)")
                .column("d", "INT")
                .column("e", "INT")
                .column("f", "VARBINARY(6)")
                .build();
    }

    private static Schema schemaForNotNullEnforcer() {
        return Schema.newBuilder()
                .column("a", "INT")
                .column("b", "STRING NOT NULL")
                .column("c", "INT NOT NULL")
                .build();
    }

    private static void assertTimestampResults(
            SharedReference<List<Long>> timestamps, List<Row> rows) {
        assertThat(timestamps.get()).hasSize(rows.size());
        for (int i = 0; i < rows.size(); i++) {
            assertThat(Instant.ofEpochMilli(timestamps.get().get(i)))
                    .isEqualTo(rows.get(i).getField(2));
        }
    }

    private static class TestSource extends TableFactoryHarness.ScanSourceBase {

        private final List<Row> rows;

        private TestSource(List<Row> rows) {
            super(false);
            this.rows = rows;
        }

        @Override
        public ScanTableSource.ScanRuntimeProvider getScanRuntimeProvider(
                ScanTableSource.ScanContext context) {
            final DynamicTableSource.DataStructureConverter converter =
                    context.createDataStructureConverter(
                            getFactoryContext().getPhysicalRowDataType());

            return SourceFunctionProvider.of(new TestSourceFunction(rows, converter), false);
        }
    }

    private static class TestSourceFunction implements SourceFunction<RowData> {

        private final List<Row> rows;
        private final DynamicTableSource.DataStructureConverter converter;

        public TestSourceFunction(
                List<Row> rows, DynamicTableSource.DataStructureConverter converter) {
            this.rows = rows;
            this.converter = converter;
        }

        @Override
        public void run(SourceContext<RowData> ctx) throws Exception {
            rows.stream().map(row -> (RowData) converter.toInternal(row)).forEach(ctx::collect);
        }

        @Override
        public void cancel() {}
    }

    private static class TestTimestampWriter extends TestSink.DefaultSinkWriter<RowData> {

        private final SharedReference<List<Long>> timestamps;

        private TestTimestampWriter(SharedReference<List<Long>> timestamps) {
            this.timestamps = timestamps;
        }

        @Override
        public void write(RowData element, Context context) {
            addElement(timestamps, context.timestamp());
            super.write(element, context);
        }
    }

    private static class RecordWriter extends TestSink.DefaultSinkWriter<RowData> {

        private final SharedReference<List<RowData>> rows;

        private RecordWriter(SharedReference<List<RowData>> rows) {
            this.rows = rows;
        }

        @Override
        public void write(RowData element, Context context) {
            addElement(rows, element);
            super.write(element, context);
        }
    }
}
