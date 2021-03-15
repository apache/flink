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

package org.apache.flink.table.planner.factories;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkProvider;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.filesystem.FileSystemOptions;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Test implementation of {@link DynamicTableSourceFactory} and {@link DynamicTableSinkFactory} that
 * creates a file source and sink based on {@link SourceProvider} and {@link SinkProvider}.
 */
public class TestFileFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    // --------------------------------------------------------------------------------------------
    // Factory
    // --------------------------------------------------------------------------------------------

    private static final String IDENTIFIER = "test-file";

    private static final ConfigOption<String> RUNTIME_SOURCE =
            ConfigOptions.key("runtime-source")
                    .stringType()
                    .defaultValue("Source"); // another is "DataStream"

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        Configuration conf = Configuration.fromMap(context.getCatalogTable().getOptions());
        return new TestFileTableSource(
                new Path(conf.getString(FileSystemOptions.PATH)), conf.getString(RUNTIME_SOURCE));
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        Configuration conf = Configuration.fromMap(context.getCatalogTable().getOptions());
        return new TestFileTableSink(new Path(conf.getString(FileSystemOptions.PATH)));
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>(Collections.singletonList(RUNTIME_SOURCE));
    }

    private static class TestFileTableSource implements ScanTableSource {

        private final Path path;
        private final String runtimeSource;

        private TestFileTableSource(Path path, String runtimeSource) {
            this.path = path;
            this.runtimeSource = runtimeSource;
        }

        @Override
        public ChangelogMode getChangelogMode() {
            return ChangelogMode.insertOnly();
        }

        @Override
        public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
            FileSource<RowData> fileSource =
                    FileSource.forRecordStreamFormat(new FileFormat(), path).build();
            switch (runtimeSource) {
                case "Source":
                    return SourceProvider.of(fileSource);
                case "DataStream":
                    return new TestFileSourceDataStreamScanProvider(fileSource, asSummaryString());
                default:
                    throw new IllegalArgumentException(
                            "Unsupported runtime source class: " + runtimeSource);
            }
        }

        @Override
        public DynamicTableSource copy() {
            return new TestFileTableSource(path, runtimeSource);
        }

        @Override
        public String asSummaryString() {
            return "test-file-source";
        }
    }

    private static class TestFileTableSink implements DynamicTableSink {

        private final Path path;

        private TestFileTableSink(Path path) {
            this.path = path;
        }

        @Override
        public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
            return requestedMode;
        }

        @Override
        public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
            final FileSink<RowData> fileSink =
                    FileSink.forRowFormat(path, new RowDataEncoder()).build();
            return SinkProvider.of(fileSink);
        }

        @Override
        public DynamicTableSink copy() {
            return new TestFileTableSink(path);
        }

        @Override
        public String asSummaryString() {
            return "test-file-sink";
        }
    }

    private static class FileFormat extends SimpleStreamFormat<RowData> {

        @Override
        public Reader<RowData> createReader(Configuration config, FSDataInputStream stream) {
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
            return new Reader<RowData>() {
                @Override
                public RowData read() throws IOException {
                    String line = reader.readLine();
                    if (line == null) {
                        return null;
                    }
                    return GenericRowData.of(StringData.fromString(line));
                }

                @Override
                public void close() throws IOException {
                    reader.close();
                }
            };
        }

        @Override
        public TypeInformation<RowData> getProducedType() {
            // For ScanTableSource, the output type is determined by the planner,
            // and the result of this method will not be used.
            // The purpose of returning null is to verify that the planner can
            // handle the output type correctly.
            return null;
        }
    }

    private static class TestFileSourceDataStreamScanProvider implements DataStreamScanProvider {
        private final FileSource<RowData> fileSource;
        private final String name;

        private TestFileSourceDataStreamScanProvider(FileSource<RowData> fileSource, String name) {
            this.fileSource = fileSource;
            this.name = name;
        }

        @Override
        public DataStream<RowData> produceDataStream(StreamExecutionEnvironment execEnv) {
            return execEnv.fromSource(fileSource, WatermarkStrategy.noWatermarks(), name);
        }

        @Override
        public boolean isBounded() {
            return true;
        }
    }

    private static class RowDataEncoder implements Encoder<RowData> {

        private static final long serialVersionUID = 1L;

        private static final byte FIELD_DELIMITER = ",".getBytes(StandardCharsets.UTF_8)[0];
        private static final byte LINE_DELIMITER = "\n".getBytes(StandardCharsets.UTF_8)[0];

        public RowDataEncoder() {}

        @Override
        public void encode(RowData rowData, OutputStream stream) throws IOException {
            for (int index = 0; index < rowData.getArity(); index++) {
                stream.write(rowData.getString(index).toBytes());
                if (index != rowData.getArity() - 1) {
                    stream.write(FIELD_DELIMITER);
                }
            }
            stream.write(LINE_DELIMITER);
        }
    }
}
