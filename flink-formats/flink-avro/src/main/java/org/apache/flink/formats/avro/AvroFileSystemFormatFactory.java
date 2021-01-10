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

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FileSystemFormatFactory;
import org.apache.flink.table.types.DataType;
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
import java.util.stream.Collectors;

import static org.apache.flink.formats.avro.AvroFileFormatFactory.AVRO_OUTPUT_CODEC;

/** Avro format factory for file system. */
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
        String[] fieldNames = context.getSchema().getFieldNames();
        List<String> projectFields =
                Arrays.stream(context.getProjectFields())
                        .mapToObj(idx -> fieldNames[idx])
                        .collect(Collectors.toList());
        List<String> csvFields =
                Arrays.stream(fieldNames)
                        .filter(field -> !context.getPartitionKeys().contains(field))
                        .collect(Collectors.toList());

        int[] selectFieldToProjectField =
                context.getFormatProjectFields().stream()
                        .mapToInt(projectFields::indexOf)
                        .toArray();
        int[] selectFieldToFormatField =
                context.getFormatProjectFields().stream().mapToInt(csvFields::indexOf).toArray();

        //noinspection unchecked
        return new RowDataAvroInputFormat(
                context.getPaths(),
                context.getFormatRowType(),
                context.getSchema().getFieldDataTypes(),
                context.getSchema().getFieldNames(),
                context.getProjectFields(),
                context.getPartitionKeys(),
                context.getDefaultPartName(),
                context.getPushedDownLimit(),
                selectFieldToProjectField,
                selectFieldToFormatField);
    }

    /**
     * InputFormat that reads avro record into {@link RowData}.
     *
     * <p>This extends {@link AvroInputFormat}, but {@link RowData} is not a avro record, so now we
     * remove generic information. TODO {@link AvroInputFormat} support type conversion.
     */
    private static class RowDataAvroInputFormat extends AvroInputFormat {

        private static final long serialVersionUID = 1L;

        private final DataType[] fieldTypes;
        private final String[] fieldNames;
        private final int[] selectFields;
        private final List<String> partitionKeys;
        private final String defaultPartValue;
        private final long limit;
        private final int[] selectFieldToProjectField;
        private final int[] selectFieldToFormatField;
        private final RowType formatRowType;

        private transient long emitted;
        // reuse object for per record
        private transient GenericRowData rowData;
        private transient IndexedRecord record;
        private transient AvroToRowDataConverters.AvroToRowDataConverter converter;

        public RowDataAvroInputFormat(
                Path[] filePaths,
                RowType formatRowType,
                DataType[] fieldTypes,
                String[] fieldNames,
                int[] selectFields,
                List<String> partitionKeys,
                String defaultPartValue,
                long limit,
                int[] selectFieldToProjectField,
                int[] selectFieldToFormatField) {
            super(filePaths[0], GenericRecord.class);
            super.setFilePaths(filePaths);
            this.formatRowType = formatRowType;
            this.fieldTypes = fieldTypes;
            this.fieldNames = fieldNames;
            this.partitionKeys = partitionKeys;
            this.defaultPartValue = defaultPartValue;
            this.selectFields = selectFields;
            this.limit = limit;
            this.emitted = 0;
            this.selectFieldToProjectField = selectFieldToProjectField;
            this.selectFieldToFormatField = selectFieldToFormatField;
        }

        @Override
        public void open(FileInputSplit split) throws IOException {
            super.open(split);
            Schema schema = AvroSchemaConverter.convertToSchema(formatRowType);
            record = new GenericData.Record(schema);
            rowData =
                    PartitionPathUtils.fillPartitionValueForRecord(
                            fieldNames,
                            fieldTypes,
                            selectFields,
                            partitionKeys,
                            currentSplit.getPath(),
                            defaultPartValue);
            this.converter = AvroToRowDataConverters.createRowConverter(formatRowType);
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
            GenericRowData row = (GenericRowData) converter.convert(r);

            for (int i = 0; i < selectFieldToFormatField.length; i++) {
                rowData.setField(
                        selectFieldToProjectField[i], row.getField(selectFieldToFormatField[i]));
            }
            emitted++;
            return rowData;
        }
    }
}
