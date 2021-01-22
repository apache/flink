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

package org.apache.flink.table.filesystem;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

/** Test for {@link FileSystemTableSink}. */
public class FileSystemTableSinkTest {

    private static final TableSchema TEST_SCHEMA =
            TableSchema.builder()
                    .field("f0", DataTypes.STRING())
                    .field("f1", DataTypes.BIGINT())
                    .field("f2", DataTypes.BIGINT())
                    .build();

    @Test
    public void testFileSystemTableSinkWithParallelismInStreaming() {

        int parallelism = 2;

        DescriptorProperties descriptor = new DescriptorProperties();
        descriptor.putString(FactoryUtil.CONNECTOR.key(), "filesystem");
        descriptor.putString("path", "/tmp");
        descriptor.putString("format", "testcsv");
        descriptor.putString("sink.parallelism", String.valueOf(parallelism));

        final DynamicTableSink tableSink = createSink(descriptor);
        Assert.assertTrue(tableSink instanceof FileSystemTableSink);

        final DynamicTableSink.SinkRuntimeProvider provider =
                tableSink.getSinkRuntimeProvider(new MockSinkContext(false));
        Assert.assertTrue(provider instanceof DataStreamSinkProvider);

        final DataStreamSinkProvider dataStreamSinkProvider = (DataStreamSinkProvider) provider;
        final DataStreamSink<?> dataStreamSink =
                dataStreamSinkProvider.consumeDataStream(createInputDataStream());
        final List<Transformation<?>> inputs = dataStreamSink.getTransformation().getInputs();
        Assert.assertTrue(inputs.get(0).getParallelism() == parallelism);
    }

    @Test
    public void testFileSystemTableSinkWithParallelismInBatch() {

        int parallelism = 2;

        DescriptorProperties descriptor = new DescriptorProperties();
        descriptor.putString(FactoryUtil.CONNECTOR.key(), "filesystem");
        descriptor.putString("path", "/tmp");
        descriptor.putString("format", "testcsv");
        descriptor.putString("sink.parallelism", String.valueOf(parallelism));

        final DynamicTableSink tableSink = createSink(descriptor);
        Assert.assertTrue(tableSink instanceof FileSystemTableSink);

        final DynamicTableSink.SinkRuntimeProvider provider =
                tableSink.getSinkRuntimeProvider(new MockSinkContext(true));
        Assert.assertTrue(provider instanceof DataStreamSinkProvider);

        final DataStreamSinkProvider dataStreamSinkProvider = (DataStreamSinkProvider) provider;
        final DataStreamSink<?> dataStreamSink =
                dataStreamSinkProvider.consumeDataStream(createInputDataStream());
        Assert.assertTrue(dataStreamSink.getTransformation().getParallelism() == parallelism);
    }

    private static DataStream<RowData> createInputDataStream() {
        final MockTransformation<RowData> mockTransformation =
                MockTransformation.createMockTransformation();
        final DummyStreamExecutionEnvironment mockEnv = new DummyStreamExecutionEnvironment();

        return new DataStream<>(mockEnv, mockTransformation);
    }

    private static DynamicTableSink createSink(DescriptorProperties properties) {
        return FactoryUtil.createTableSink(
                null,
                ObjectIdentifier.of("mycatalog", "mydb", "mytable"),
                new CatalogTableImpl(TEST_SCHEMA, properties.asMap(), ""),
                new Configuration(),
                Thread.currentThread().getContextClassLoader(),
                false);
    }

    private static class MockSinkContext implements DynamicTableSink.Context {

        private final boolean bounded;

        public MockSinkContext(boolean bounded) {
            this.bounded = bounded;
        }

        @Override
        public boolean isBounded() {
            return bounded;
        }

        @Override
        public TypeInformation<?> createTypeInformation(DataType consumedDataType) {
            return null;
        }

        @Override
        public DynamicTableSink.DataStructureConverter createDataStructureConverter(
                DataType consumedDataType) {
            return null;
        }
    }

    private static class MockTransformation<T> extends Transformation<T> {

        public static MockTransformation<RowData> createMockTransformation() {
            final InternalTypeInfo<RowData> typeInfo =
                    InternalTypeInfo.of(TEST_SCHEMA.toPhysicalRowDataType().getLogicalType());
            return new MockTransformation<RowData>(typeInfo);
        }

        public MockTransformation(TypeInformation<T> typeInfo) {
            super("MockTransform", typeInfo, 1);
        }

        @Override
        public List<Transformation<?>> getTransitivePredecessors() {
            return null;
        }

        @Override
        public List<Transformation<?>> getInputs() {
            return Collections.emptyList();
        }
    }

    private static class DummyStreamExecutionEnvironment extends StreamExecutionEnvironment {

        @Override
        public JobExecutionResult execute(StreamGraph streamGraph) throws Exception {
            return null;
        }
    }
}
