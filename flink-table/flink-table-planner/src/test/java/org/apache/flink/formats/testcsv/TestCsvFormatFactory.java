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

package org.apache.flink.formats.testcsv;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.format.FileBasedStatisticsReportableInputFormat;
import org.apache.flink.table.connector.format.ProjectableDecodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.DataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Factory for csv test format.
 *
 * <p>NOTE: This is meant only for testing purpose and doesn't provide a feature complete stable csv
 * parser! If you need a feature complete CSV parser, check out the flink-csv package.
 */
public class TestCsvFormatFactory
        implements DeserializationFormatFactory, SerializationFormatFactory {

    private static final Logger LOG = LoggerFactory.getLogger(TestCsvFormatFactory.class);

    public static final String IDENTIFIER = "testcsv";

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
        return new HashSet<>();
    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        return new EncodingFormat<SerializationSchema<RowData>>() {
            @Override
            public SerializationSchema<RowData> createRuntimeEncoder(
                    DynamicTableSink.Context context, DataType consumedDataType) {
                DynamicTableSink.DataStructureConverter converter =
                        context.createDataStructureConverter(consumedDataType);

                return new TestCsvSerializationSchema(converter);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        return new TestCsvInputFormat();
    }

    private static class TestCsvInputFormat
            implements ProjectableDecodingFormat<DeserializationSchema<RowData>>,
                    FileBasedStatisticsReportableInputFormat {

        @Override
        public DeserializationSchema<RowData> createRuntimeDecoder(
                DynamicTableSource.Context context,
                DataType physicalDataType,
                int[][] projections) {
            DataType projectedPhysicalDataType =
                    Projection.of(projections).project(physicalDataType);
            return new TestCsvDeserializationSchema(
                    projectedPhysicalDataType,
                    context.createTypeInformation(projectedPhysicalDataType),
                    DataType.getFieldNames(physicalDataType),
                    // Check out the FileSystemTableSink#createSourceContext for more details on
                    // why we need this
                    ScanRuntimeProviderContext.INSTANCE::createDataStructureConverter);
        }

        @Override
        public ChangelogMode getChangelogMode() {
            return ChangelogMode.insertOnly();
        }

        @Override
        public TableStats reportStatistics(List<Path> files, DataType producedDataType) {
            // For Csv format, it's a heavy operation to obtain accurate statistics by scanning all
            // files. So, We obtain the estimated statistics by sampling, the specific way is to
            // sample the first 100 lines and calculate their row size, then compare row size with
            // total file size to get the estimated row count.
            final int totalSampleLineCnt = 100;
            try {
                long totalFileSize = 0;
                int sampledRowCnt = 0;
                long sampledRowSize = 0;
                for (Path file : files) {
                    FileSystem fs = FileSystem.get(file.toUri());
                    FileStatus status = fs.getFileStatus(file);
                    totalFileSize += status.getLen();

                    // sample the line size
                    if (sampledRowCnt < totalSampleLineCnt) {
                        try (InputStreamReader isr =
                                        new InputStreamReader(
                                                Files.newInputStream(
                                                        new File(file.toUri()).toPath()));
                                BufferedReader br = new BufferedReader(isr)) {
                            String line;
                            while (sampledRowCnt < totalSampleLineCnt
                                    && (line = br.readLine()) != null) {
                                sampledRowCnt += 1;
                                sampledRowSize += line.getBytes(StandardCharsets.UTF_8).length;
                            }
                        }
                    }
                }

                // If line break is "\r\n", br.readLine() will ignore '\n' which make sampledRowSize
                // smaller than totalFileSize. This will influence test result.
                if (sampledRowCnt < totalSampleLineCnt) {
                    sampledRowSize = totalFileSize;
                }
                if (sampledRowSize == 0) {
                    return TableStats.UNKNOWN;
                }

                int realSampledLineCnt = Math.min(totalSampleLineCnt, sampledRowCnt);
                long estimatedRowCount = totalFileSize * realSampledLineCnt / sampledRowSize;
                return new TableStats(estimatedRowCount);
            } catch (Exception e) {
                LOG.warn("Reporting statistics failed for Csv format: {}", e.getMessage());
                return TableStats.UNKNOWN;
            }
        }
    }
}
