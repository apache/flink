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

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.runtime.operators.sink.deprecated.TestSinkV2;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.data.binary.BinaryMapData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.data.writer.BinaryArrayWriter;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.planner.factories.TableFactoryHarness;
import org.apache.flink.table.runtime.typeutils.ArrayDataSerializer;
import org.apache.flink.table.runtime.typeutils.MapDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.junit.SharedObjectsExtension;
import org.apache.flink.testutils.junit.SharedReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for verifying runtime behaviour of {@link BuiltInFunctionDefinitions#INTERNAL_HASHCODE}.
 *
 * <p>Should be removed along with {@link
 * org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink}
 */
@Deprecated
public class HashcodeDeprecatedITCase {

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_EXTENSION = new MiniClusterExtension();

    @RegisterExtension
    private final SharedObjectsExtension sharedObjects = SharedObjectsExtension.create();

    @Test
    void testStreamRecordTimestampInserterSinkRuntimeProvider()
            throws ExecutionException, InterruptedException {
        final TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        final SharedReference<List<RowData>> results = sharedObjects.add(new ArrayList<>());

        final TableDescriptor sourceDescriptor =
                TableFactoryHarness.newBuilder()
                        .schema(
                                Schema.newBuilder()
                                        .column("f0", DataTypes.INT())
                                        .column(
                                                "f1",
                                                DataTypes.STRING().bridgedTo(StringData.class))
                                        .column(
                                                "f2",
                                                DataTypes.ARRAY(DataTypes.INT())
                                                        .bridgedTo(ArrayData.class))
                                        .column(
                                                "f3",
                                                DataTypes.MAP(
                                                                DataTypes.DOUBLE(),
                                                                DataTypes.BIGINT())
                                                        .bridgedTo(MapData.class))
                                        .build())
                        .source(new TestSource())
                        .build();
        final TableDescriptor sinkDescriptor =
                TableFactoryHarness.newBuilder()
                        .schema(
                                Schema.newBuilder()
                                        .column(
                                                "arrayOfHashcodes",
                                                DataTypes.ARRAY(DataTypes.INT()))
                                        .build())
                        .sink(buildRuntimeSinkProvider(new TestWriter(results)))
                        .build();
        tableEnv.createTable("T1", sourceDescriptor);
        tableEnv.createTable("T2", sinkDescriptor);
        final String sqlStmt =
                "INSERT INTO T2 SELECT array["
                        + "$HASHCODE$1(f0), "
                        + "$HASHCODE$1(f1), "
                        + "$HASHCODE$1(f2), "
                        + "$HASHCODE$1(f3)] FROM T1";
        tableEnv.executeSql(sqlStmt).await();
        final List<int[]> data =
                results.get().stream()
                        .map(d -> d.getArray(0).toIntArray())
                        .collect(Collectors.toList());
        assertThat(data).containsExactly(new int[] {42, 454226189, 2306, -2138406827});
    }

    private TableFactoryHarness.SinkBase buildRuntimeSinkProvider(
            TestSinkV2.DefaultSinkWriter<RowData> writer) {
        return new TableFactoryHarness.SinkBase() {
            @Override
            public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
                TestSinkV2<RowData> sink =
                        TestSinkV2.<RowData>newBuilder().setWriter(writer).build();
                return SinkV2Provider.of(sink);
            }
        };
    }

    private static class TestWriter extends TestSinkV2.DefaultSinkWriter<RowData> {

        private final SharedReference<List<RowData>> results;

        private TestWriter(SharedReference<List<RowData>> results) {
            this.results = results;
        }

        @Override
        public void write(RowData element, Context context) {
            results.applySync(r -> r.add(element));
            super.write(element, context);
        }
    }

    private static class TestSource extends TableFactoryHarness.ScanSourceBase {

        private TestSource() {
            super(false);
        }

        @Override
        public ScanTableSource.ScanRuntimeProvider getScanRuntimeProvider(
                ScanTableSource.ScanContext context) {
            return SourceFunctionProvider.of(new TestSourceFunction(), false);
        }
    }

    private static class TestSourceFunction implements SourceFunction<RowData> {

        public TestSourceFunction() {}

        @Override
        public void run(SourceContext<RowData> ctx) {
            final BinaryRowData row = new BinaryRowData(4);
            final BinaryRowWriter writer = new BinaryRowWriter(row);
            writer.writeInt(0, 42);
            writer.writeString(1, new BinaryStringData("ABCD"));
            final BinaryArrayData array = new BinaryArrayData();
            final BinaryArrayWriter arrayWriter = new BinaryArrayWriter(array, 2, 4);
            arrayWriter.writeInt(0, 42);
            arrayWriter.writeInt(1, 43);
            arrayWriter.complete();
            writer.writeArray(2, array, new ArrayDataSerializer(new IntType()));

            final BinaryArrayData keys = new BinaryArrayData();
            final BinaryArrayData values = new BinaryArrayData();
            final BinaryArrayWriter keysWriter = new BinaryArrayWriter(keys, 2, 8);
            final BinaryArrayWriter valuesWriter = new BinaryArrayWriter(values, 2, 8);
            keysWriter.writeDouble(0, 42D);
            valuesWriter.writeLong(0, 42L);
            keysWriter.writeDouble(1, 43D);
            valuesWriter.writeLong(1, 43L);
            keysWriter.complete();
            valuesWriter.complete();

            writer.writeMap(
                    3,
                    BinaryMapData.valueOf(keys, values),
                    new MapDataSerializer(new DoubleType(), new BigIntType()));
            writer.complete();

            ctx.collect(row);
            ctx.close();
        }

        @Override
        public void cancel() {}
    }
}
