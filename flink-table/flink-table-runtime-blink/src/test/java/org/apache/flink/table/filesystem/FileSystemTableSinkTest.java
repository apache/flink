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

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.ExceptionUtils;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Test for {@link FileSystemTableSink}. */
public class FileSystemTableSinkTest {

    private static final TableSchema TEST_SCHEMA =
            TableSchema.builder()
                    .field("f0", DataTypes.STRING())
                    .field("f1", DataTypes.BIGINT())
                    .field("f2", DataTypes.BIGINT())
                    .build();

    @Test
    public void testFileSytemTableSinkWithParallelismInChangeLogMode() {

        int parallelism = 2;

        DescriptorProperties descriptor = new DescriptorProperties();
        descriptor.putString(FactoryUtil.CONNECTOR.key(), "filesystem");
        descriptor.putString("path", "/tmp");
        descriptor.putString("format", "testcsv");
        descriptor.putString(FactoryUtil.SINK_PARALLELISM.key(), String.valueOf(parallelism));

        final DynamicTableSink tableSink = createSink(descriptor);
        assertTrue(tableSink instanceof FileSystemTableSink);

        final DynamicTableSink.SinkRuntimeProvider provider =
                tableSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        assertTrue(provider instanceof DataStreamSinkProvider);

        try {
            tableSink.getChangelogMode(ChangelogMode.newBuilder().addContainedKind(RowKind.INSERT).addContainedKind( RowKind.DELETE).build());
            fail();
        } catch (Exception e) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(
                                    e, "when the input stream is not INSERT only")
                            .isPresent());
        }


    }

    @Test
    public void testFileSystemTableSinkWithParallelismInStreaming() {

        int parallelism = 2;

        DescriptorProperties descriptor = new DescriptorProperties();
        descriptor.putString(FactoryUtil.CONNECTOR.key(), "filesystem");
        descriptor.putString("path", "/tmp");
        descriptor.putString("format", "testcsv");
        descriptor.putString(FactoryUtil.SINK_PARALLELISM.key(), String.valueOf(parallelism));

        final DynamicTableSink tableSink = createSink(descriptor);
        Assert.assertTrue(tableSink instanceof FileSystemTableSink);

        final DynamicTableSink.SinkRuntimeProvider provider =
                tableSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        Assert.assertTrue(provider instanceof DataStreamSinkProvider);

        final DataStreamSinkProvider dataStreamSinkProvider = (DataStreamSinkProvider) provider;
        final DataStreamSink<?> dataStreamSink =
                dataStreamSinkProvider.consumeDataStream(createInputDataStream());
        final List<Transformation<?>> inputs = dataStreamSink.getTransformation().getInputs();
        assertEquals(inputs.get(0).getParallelism(), parallelism);
    }

    @Test
    public void testFileSystemTableSinkWithParallelismInBatch() {

        int parallelism = 2;

        DescriptorProperties descriptor = new DescriptorProperties();
        descriptor.putString(FactoryUtil.CONNECTOR.key(), "filesystem");
        descriptor.putString("path", "/tmp");
        descriptor.putString("format", "testcsv");
        descriptor.putString(FactoryUtil.SINK_PARALLELISM.key(), String.valueOf(parallelism));

        final DynamicTableSink tableSink = createSink(descriptor);
        assertTrue(tableSink instanceof FileSystemTableSink);

        final DynamicTableSink.SinkRuntimeProvider provider =
                tableSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(true));
        assertTrue(provider instanceof DataStreamSinkProvider);

        final DataStreamSinkProvider dataStreamSinkProvider = (DataStreamSinkProvider) provider;
        final DataStreamSink<?> dataStreamSink =
                dataStreamSinkProvider.consumeDataStream(createInputDataStream());
        assertEquals(dataStreamSink.getTransformation().getParallelism(), parallelism);
    }

    private static DataStream<RowData> createInputDataStream() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        return env.fromElements(new GenericRowData(3));
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


}
