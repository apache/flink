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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.filesystem.FileSystemOptions;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/** Test file source {@link DynamicTableSourceFactory}. */
public class TestFileSourceFactory implements DynamicTableSourceFactory {

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
    public String factoryIdentifier() {
        return "filesource";
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
            return InternalTypeInfo.ofFields(DataTypes.STRING().getLogicalType());
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
}
