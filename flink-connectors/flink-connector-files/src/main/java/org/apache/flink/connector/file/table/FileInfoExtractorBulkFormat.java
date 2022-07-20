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

package org.apache.flink.connector.file.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.RecordMapperWrapperRecordIterator;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.PartitionPathUtils;

import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This {@link BulkFormat} is a wrapper that attaches file information columns to the output
 * records.
 */
class FileInfoExtractorBulkFormat implements BulkFormat<RowData, FileSourceSplit> {

    private final BulkFormat<RowData, FileSourceSplit> wrapped;
    private final TypeInformation<RowData> producedType;

    private final List<FileSystemTableSource.FileInfoAccessor> metadataColumnsFunctions;
    private final List<Map.Entry<String, DataType>> partitionColumnTypes;
    private final int[] extendedRowIndexMapping;

    private final String defaultPartName;

    public FileInfoExtractorBulkFormat(
            BulkFormat<RowData, FileSourceSplit> wrapped,
            DataType producedDataType,
            TypeInformation<RowData> producedTypeInformation,
            Map<String, FileSystemTableSource.FileInfoAccessor> metadataColumns,
            List<String> partitionColumns,
            String defaultPartName) {
        this.wrapped = wrapped;
        this.producedType = producedTypeInformation;
        this.defaultPartName = defaultPartName;

        // Compute index mapping for the extended row and the functions to compute metadata
        List<DataTypes.Field> producedRowField = DataType.getFields(producedDataType);
        List<String> producedRowFieldNames =
                producedRowField.stream()
                        .map(DataTypes.Field::getName)
                        .collect(Collectors.toList());
        List<String> mutableRowFieldNames =
                producedRowFieldNames.stream()
                        .filter(
                                key ->
                                        !metadataColumns.containsKey(key)
                                                && !partitionColumns.contains(key))
                        .collect(Collectors.toList());
        List<String> metadataFieldNames = new ArrayList<>(metadataColumns.keySet());

        List<String> fixedRowFieldNames =
                Stream.concat(metadataFieldNames.stream(), partitionColumns.stream())
                        .collect(Collectors.toList());

        this.partitionColumnTypes =
                partitionColumns.stream()
                        .map(
                                fieldName ->
                                        new SimpleImmutableEntry<>(
                                                fieldName,
                                                producedRowField
                                                        .get(
                                                                producedRowFieldNames.indexOf(
                                                                        fieldName))
                                                        .getDataType()))
                        .collect(Collectors.toList());

        this.extendedRowIndexMapping =
                EnrichedRowData.computeIndexMapping(
                        producedRowFieldNames, mutableRowFieldNames, fixedRowFieldNames);
        this.metadataColumnsFunctions =
                metadataFieldNames.stream().map(metadataColumns::get).collect(Collectors.toList());
    }

    @Override
    public Reader<RowData> createReader(Configuration config, FileSourceSplit split)
            throws IOException {
        return wrapReader(wrapped.createReader(config, split), split);
    }

    @Override
    public Reader<RowData> restoreReader(Configuration config, FileSourceSplit split)
            throws IOException {
        return wrapReader(wrapped.restoreReader(config, split), split);
    }

    @Override
    public boolean isSplittable() {
        return wrapped.isSplittable();
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedType;
    }

    private Reader<RowData> wrapReader(Reader<RowData> superReader, FileSourceSplit split) {
        // Fill the metadata + partition columns row
        final GenericRowData fileInfoRowData =
                new GenericRowData(metadataColumnsFunctions.size() + partitionColumnTypes.size());
        int fileInfoRowIndex = 0;
        for (; fileInfoRowIndex < metadataColumnsFunctions.size(); fileInfoRowIndex++) {
            fileInfoRowData.setField(
                    fileInfoRowIndex,
                    metadataColumnsFunctions.get(fileInfoRowIndex).getValue(split));
        }
        if (!partitionColumnTypes.isEmpty()) {
            final LinkedHashMap<String, String> partitionSpec =
                    PartitionPathUtils.extractPartitionSpecFromPath(split.path());
            for (int partitionFieldIndex = 0;
                    fileInfoRowIndex < fileInfoRowData.getArity();
                    fileInfoRowIndex++, partitionFieldIndex++) {
                final String fieldName = partitionColumnTypes.get(partitionFieldIndex).getKey();
                final DataType fieldType = partitionColumnTypes.get(partitionFieldIndex).getValue();
                if (!partitionSpec.containsKey(fieldName)) {
                    throw new RuntimeException(
                            "Cannot find the partition value from path for partition: "
                                    + fieldName);
                }

                String valueStr = partitionSpec.get(fieldName);
                valueStr = valueStr.equals(defaultPartName) ? null : valueStr;
                fileInfoRowData.setField(
                        fileInfoRowIndex,
                        PartitionPathUtils.convertStringToInternalValue(valueStr, fieldType));
            }
        }

        // This row is going to be reused for every record
        final EnrichedRowData producedRowData =
                new EnrichedRowData(fileInfoRowData, this.extendedRowIndexMapping);

        return RecordMapperWrapperRecordIterator.wrapReader(
                superReader,
                physicalRowData -> {
                    producedRowData.replaceMutableRow(physicalRowData);
                    return producedRowData;
                });
    }
}
