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

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.factories.BulkWriterFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FileSystemFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.api.java.io.CsvOutputFormat.DEFAULT_FIELD_DELIMITER;
import static org.apache.flink.api.java.io.CsvOutputFormat.DEFAULT_LINE_DELIMITER;

/** Test csv {@link FileSystemFormatFactory}. */
public class TestCsvFileSystemFormatFactory
        implements FileSystemFormatFactory, BulkWriterFormatFactory {

    @Override
    public String factoryIdentifier() {
        return "testcsv";
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
    public InputFormat<RowData, ?> createReader(ReaderContext context) {
        return new TestRowDataCsvInputFormat(
                context.getPaths(),
                context.getSchema(),
                context.getPartitionKeys(),
                context.getDefaultPartName(),
                context.getProjectFields(),
                context.getPushedDownLimit());
    }

    private static void writeCsvToStream(DataType[] types, RowData rowData, OutputStream stream)
            throws IOException {
        LogicalType[] fieldTypes =
                Arrays.stream(types).map(DataType::getLogicalType).toArray(LogicalType[]::new);
        DataFormatConverters.DataFormatConverter converter =
                DataFormatConverters.getConverterForDataType(
                        TypeConversions.fromLogicalToDataType(RowType.of(fieldTypes)));

        Row row = (Row) converter.toExternal(rowData);
        StringBuilder builder = new StringBuilder();
        Object o;
        for (int i = 0; i < row.getArity(); i++) {
            if (i > 0) {
                builder.append(DEFAULT_FIELD_DELIMITER);
            }
            if ((o = row.getField(i)) != null) {
                builder.append(o);
            }
        }
        String str = builder.toString();
        stream.write(str.getBytes(StandardCharsets.UTF_8));
        stream.write(DEFAULT_LINE_DELIMITER.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public EncodingFormat<BulkWriter.Factory<RowData>> createEncodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        return new EncodingFormat<BulkWriter.Factory<RowData>>() {
            @Override
            public BulkWriter.Factory<RowData> createRuntimeEncoder(
                    DynamicTableSink.Context context, DataType consumedDataType) {
                return out ->
                        new CsvBulkWriter(
                                consumedDataType.getChildren().toArray(new DataType[0]), out);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    private static class CsvBulkWriter implements BulkWriter<RowData> {

        private final DataType[] types;
        private final OutputStream stream;

        private CsvBulkWriter(DataType[] types, OutputStream stream) {
            this.types = types;
            this.stream = stream;
        }

        @Override
        public void addElement(RowData element) throws IOException {
            writeCsvToStream(types, element, stream);
        }

        @Override
        public void flush() {}

        @Override
        public void finish() {}
    }
}
