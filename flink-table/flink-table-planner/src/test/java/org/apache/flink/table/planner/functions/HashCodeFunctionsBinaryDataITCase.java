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

package org.apache.flink.table.planner.functions;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.CollectionInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.transformations.SinkV1Adapter;
import org.apache.flink.streaming.runtime.operators.sink.TestSink;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableResultImpl;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.planner.factories.TableFactoryHarness;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.testutils.junit.SharedObjects;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.DataTypes.STRING;

/** Test for {@link org.apache.flink.table.planner.codegen.calls.HashCodeCallGen}. */
@RunWith(Parameterized.class)
public class HashCodeFunctionsBinaryDataITCase extends AbstractTestBase {
    private static final int PARALLELISM = 4;

    private final boolean useSinkV2;
    private StreamExecutionEnvironment env;

    @Parameterized.Parameters
    public static Collection<Boolean> useSinkV2() {
        return Arrays.asList(true, false);
    }

    public HashCodeFunctionsBinaryDataITCase(boolean useSinkV2) {
        this.useSinkV2 = useSinkV2;
    }

    @Before
    public void before() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
    }

    @Rule public final SharedObjects sharedObjects = SharedObjects.create();

    @Test
    public void testBinaryRowDataHashCode() throws ExecutionException, InterruptedException {
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        final SharedReference<List<RowData>> fetched = sharedObjects.add(new ArrayList<>());
        final List<Row> rows = Arrays.asList(Row.of("1", "123", "123"), Row.of("1", "123", "123"));

        final TableDescriptor sourceDescriptor =
                TableFactoryHarness.newBuilder()
                        .schema(
                                Schema.newBuilder()
                                        .column("a", STRING())
                                        .column("b", STRING())
                                        .column("c", STRING())
                                        .build())
                        .source(new TestSource(rows))
                        .sink(buildDataStreamSinkProvider(fetched))
                        .build();
        tableEnv.createTable("T1", sourceDescriptor);
        final String sqlStmt = "SELECT HASHCODE(c) FROM T1";
        TableResult result = tableEnv.executeSql(sqlStmt);
        CloseableIterator<RowData> iterator = ((TableResultImpl) result).collectInternal();
        if (iterator.hasNext()) {
            RowData rowData = iterator.next();
            int hashcode = rowData.getInt(0);
            assert (hashcode == 1218575173);
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
            List<RowType.RowField> fields = new ArrayList<>();
            fields.add(new RowType.RowField("field1", new VarCharType()));
            fields.add(new RowType.RowField("field2", new VarCharType()));
            fields.add(new RowType.RowField("field3", new VarCharType()));
            final RowType rowType = new RowType(fields);

            List<RowData> rowDataList =
                    rows.stream()
                            .map(row -> convertToBinaryRowData(row, rowType))
                            .collect(Collectors.toList());

            CollectionInputFormat<RowData> inputFormat =
                    new CollectionInputFormat<>(rowDataList, new RowDataSerializer(rowType));

            return SourceFunctionProvider.of(
                    new InputFormatSourceFunction<>(
                            inputFormat, TypeInformation.of(new TypeHint<RowData>() {})),
                    false);
        }

        private static BinaryRowData convertToBinaryRowData(Row row, RowType rowType) {
            BinaryRowData rowData = new BinaryRowData(row.getArity());
            BinaryRowWriter writer = new BinaryRowWriter(rowData);

            for (int i = 0; i < row.getArity(); i++) {
                if (row.getField(i) == null) {
                    writer.setNullAt(i);
                } else {
                    Object field = row.getField(i);
                    LogicalType type = rowType.getTypeAt(i);

                    if (type instanceof IntType) {
                        writer.writeInt(i, (Integer) field);
                    } else if (type instanceof VarCharType) {
                        BinaryStringData str = BinaryStringData.fromString((String) field);
                        writer.writeString(i, str);
                    } else {
                        throw new RuntimeException("Unsupported type: " + type);
                    }
                }
            }

            writer.complete();
            return rowData;
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

    private static TestSink<RowData> buildRecordWriterTestSink(
            TestSink.DefaultSinkWriter<RowData> writer) {
        return TestSink.newBuilder()
                .setWriter(writer)
                .setCommittableSerializer(TestSink.StringCommittableSerializer.INSTANCE)
                .build();
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

    private static <T> void addElement(SharedReference<List<T>> elements, T element) {
        elements.applySync(l -> l.add(element));
    }
}
