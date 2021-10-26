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

package org.apache.flink.formats.avro;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FileSystemFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.PartitionPathUtils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.formats.avro.AvroFormatOptions.AVRO_OUTPUT_CODEC;

/** Avro format factory for file system. */
@Internal
public class AvroFileSystemFormatFactory implements FileSystemFormatFactory {

    public static final String IDENTIFIER = "avro";

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
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(AVRO_OUTPUT_CODEC);
        return options;
    }

    @Override
    public InputFormat<RowData, ?> createReader(ReaderContext context) {
        //noinspection unchecked
        return new RowDataAvroInputFormat(
                context.getPaths(),
                context.getFormatRowType(),
                context.getSchema().toRowDataType(),
                context.getProjectFields(),
                context.getPartitionKeys(),
                context.getDefaultPartName(),
                context.getPushedDownLimit());
    }

    /**
     * InputFormat that reads avro record into {@link RowData}.
     *
     * <p>This extends {@link AvroInputFormat}, but {@link RowData} is not a avro record, so now we
     * remove generic information. TODO {@link AvroInputFormat} support type conversion.
     */
    private static class RowDataAvroInputFormat extends AvroInputFormat {

        private static final long serialVersionUID = 1L;

        private final DataType schemaRowDataType;
        private final int[] selectFields;
        private final List<String> partitionKeys;
        private final String defaultPartValue;
        private final long limit;
        private final RowType formatRowType;

        private transient long emitted;
        // reuse object for per record
        private transient GenericRowData rowData;
        private transient IndexedRecord record;
        private transient AvroToRowDataConverters.AvroToRowDataConverter converter;

        public RowDataAvroInputFormat(
                Path[] filePaths,
                RowType formatRowType,
                DataType schemaRowDataType,
                int[] selectFields,
                List<String> partitionKeys,
                String defaultPartValue,
                long limit) {
            super(filePaths[0], GenericRecord.class);
            super.setFilePaths(filePaths);
            this.formatRowType = formatRowType;
            this.schemaRowDataType = schemaRowDataType;
            this.partitionKeys = partitionKeys;
            this.defaultPartValue = defaultPartValue;
            this.selectFields = selectFields;
            this.limit = limit;
            this.emitted = 0;
        }

        @Override
        public void open(FileInputSplit split) throws IOException {
            super.open(split);
            Schema schema = AvroSchemaConverter.convertToSchema(formatRowType);
            record = new GenericData.Record(schema);

            String[] fieldNames =
                    ((RowType) schemaRowDataType.getLogicalType())
                            .getFieldNames()
                            .toArray(new String[0]);
            DataType[] fieldDataTypes = schemaRowDataType.getChildren().toArray(new DataType[0]);
            rowData =
                    PartitionPathUtils.fillPartitionValueForRecord(
                            fieldNames,
                            fieldDataTypes,
                            selectFields,
                            partitionKeys,
                            currentSplit.getPath(),
                            defaultPartValue);

            RowType producedRowType =
                    RowType.of(
                            Arrays.stream(selectFields)
                                    .mapToObj(i -> fieldDataTypes[i].getLogicalType())
                                    .toArray(LogicalType[]::new),
                            Arrays.stream(selectFields)
                                    .mapToObj(i -> fieldNames[i])
                                    .toArray(String[]::new));
            this.converter =
                    AvroToRowDataConverters.createRowConverter(producedRowType, formatRowType);
        }

        @Override
        public boolean reachedEnd() throws IOException {
            return emitted >= limit || super.reachedEnd();
        }

        @Override
        public Object nextRecord(Object reuse) throws IOException {
            @SuppressWarnings("unchecked")
            IndexedRecord r = (IndexedRecord) super.nextRecord(record);
            if (r == null) {
                return null;
            }
            converter.convert(r, rowData);
            emitted++;
            return rowData;
        }
    }
}
