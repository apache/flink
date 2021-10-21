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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.runtime.operators.sink.TestSink;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.factories.TableFactoryHarness;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.testutils.junit.SharedObjects;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.types.Row;

import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.DataTypes.INT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;

/** Test for {@link CommonExecSink}. */
public class CommonExecSinkITCase extends AbstractTestBase {

    private StreamExecutionEnvironment env;

    @Before
    public void before() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
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
                        .source(new TimestampTestSource(rows))
                        .sink(
                                new TableFactoryHarness.SinkBase() {
                                    @Override
                                    public DynamicTableSink.SinkRuntimeProvider
                                            getSinkRuntimeProvider(
                                                    DynamicTableSink.Context context) {
                                        return SinkProvider.of(
                                                TestSink.newBuilder()
                                                        .setWriter(
                                                                new TestTimestampWriter(timestamps))
                                                        .setCommittableSerializer(
                                                                TestSink.StringCommittableSerializer
                                                                        .INSTANCE)
                                                        .build());
                                    }
                                })
                        .build();
        tableEnv.createTable("T1", sourceDescriptor);
        String sqlStmt = "INSERT INTO T1 SELECT * FROM T1";
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

        final TableDescriptor sourceDescriptor =
                TableFactoryHarness.newBuilder()
                        .schema(schemaStreamRecordTimestampInserter(true))
                        .source(new TimestampTestSource(rows))
                        .sink(
                                new TableFactoryHarness.SinkBase() {
                                    @Override
                                    public DataStreamSinkProvider getSinkRuntimeProvider(
                                            DynamicTableSink.Context context) {
                                        return dataStream ->
                                                dataStream.addSink(
                                                        new SinkFunction<RowData>() {
                                                            @Override
                                                            public void invoke(
                                                                    RowData value,
                                                                    Context context) {
                                                                addElement(
                                                                        timestamps,
                                                                        context.timestamp());
                                                            }
                                                        });
                                    }
                                })
                        .build();
        tableEnv.createTable("T1", sourceDescriptor);
        String sqlStmt = "INSERT INTO T1 SELECT * FROM T1";
        assertPlan(tableEnv, sqlStmt, true);
        tableEnv.executeSql(sqlStmt).await();
        Collections.sort(timestamps.get());
        assertTimestampResults(timestamps, rows);
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
                        .source(new TimestampTestSource(rows))
                        .sink(
                                new TableFactoryHarness.SinkBase() {
                                    @Override
                                    public DataStreamSinkProvider getSinkRuntimeProvider(
                                            DynamicTableSink.Context context) {
                                        return buildRecordTestSinkProvider(fetched);
                                    }
                                })
                        .build();
        tableEnv.createTable("T1", sourceDescriptor);
        String sqlStmt = "INSERT INTO T1 SELECT * FROM T1";
        tableEnv.executeSql(sqlStmt).await();
        final List<Integer> fetchedRows =
                fetched.get().stream().map(r -> r.getInt(0)).sorted().collect(Collectors.toList());
        assertEquals(fetchedRows.get(0).intValue(), 1);
        assertEquals(fetchedRows.get(1).intValue(), 2);
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
                        .source(new TimestampTestSource(rows))
                        .sink(
                                new TableFactoryHarness.SinkBase() {
                                    @Override
                                    public DynamicTableSink.SinkRuntimeProvider
                                            getSinkRuntimeProvider(
                                                    DynamicTableSink.Context context) {
                                        return SinkProvider.of(
                                                TestSink.newBuilder()
                                                        .setWriter(
                                                                new TestTimestampWriter(timestamps))
                                                        .setCommittableSerializer(
                                                                TestSink.StringCommittableSerializer
                                                                        .INSTANCE)
                                                        .build());
                                    }
                                })
                        .build();
        tableEnv.createTable("T1", sourceDescriptor);
        assertPlan(tableEnv, "INSERT INTO T1 SELECT * FROM T1", false);
    }

    private static DataStreamSinkProvider buildRecordTestSinkProvider(
            SharedReference<List<RowData>> fetched) {
        return dataStream ->
                dataStream.sinkTo(
                        TestSink.newBuilder()
                                .setWriter(new RecordWriter(fetched))
                                .setCommittableSerializer(
                                        TestSink.StringCommittableSerializer.INSTANCE)
                                .build());
    }

    private static <T> void addElement(SharedReference<List<T>> elements, T element) {
        elements.applySync(l -> l.add(element));
    }

    private static void assertPlan(
            StreamTableEnvironment tableEnv,
            String sql,
            boolean containsStreamRecordTimestampInserter) {
        Matcher<String> matcher = containsString("StreamRecordTimestampInserter(rowtime field: 2");
        if (!containsStreamRecordTimestampInserter) {
            matcher = not(matcher);
        }
        assertThat(tableEnv.explainSql(sql, ExplainDetail.JSON_EXECUTION_PLAN), matcher);
    }

    private static Schema schemaStreamRecordTimestampInserter(boolean withWatermark) {
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("a", "INT")
                        .column("b", "STRING")
                        .column("ts", "TIMESTAMP_LTZ(3)");
        if (withWatermark) {
            builder.watermark("ts", "ts");
        }
        return builder.build();
    }

    private static void assertTimestampResults(
            SharedReference<List<Long>> timestamps, List<Row> rows) {
        assertEquals(rows.size(), timestamps.get().size());
        for (int i = 0; i < rows.size(); i++) {
            assertEquals(rows.get(i).getField(2), Instant.ofEpochMilli(timestamps.get().get(i)));
        }
    }

    private static class TimestampTestSource extends TableFactoryHarness.ScanSourceBase {

        private final List<Row> rows;

        private TimestampTestSource(List<Row> rows) {
            super(false);
            this.rows = rows;
        }

        @Override
        public ScanTableSource.ScanRuntimeProvider getScanRuntimeProvider(
                ScanTableSource.ScanContext context) {
            final DynamicTableSource.DataStructureConverter converter =
                    context.createDataStructureConverter(
                            getFactoryContext()
                                    .getCatalogTable()
                                    .getResolvedSchema()
                                    .toPhysicalRowDataType());

            return SourceFunctionProvider.of(new TestSource(rows, converter), true);
        }
    }

    private static class TestSource implements SourceFunction<RowData> {

        private final List<Row> rows;
        private final DynamicTableSource.DataStructureConverter converter;

        public TestSource(List<Row> rows, DynamicTableSource.DataStructureConverter converter) {
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
