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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.DataType;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** A helper class to build the fixed and mutable row index mapping. */
public class FileInfoExtractor implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<PartitionColumn> partitionColumns;
    private final int[] extendedRowIndexMapping;

    public FileInfoExtractor(
            DataType producedDataType,
            List<String> metadataColumns,
            List<String> partitionColumns) {

        // Compute index mapping for the extended row and the functions to compute metadata
        List<DataTypes.Field> producedRowField = DataType.getFields(producedDataType);
        List<String> producedRowFieldNames =
                producedRowField.stream()
                        .map(DataTypes.Field::getName)
                        .collect(Collectors.toList());

        // Filter out partition columns not in producedDataType
        final List<String> partitionKeysToExtract =
                DataType.getFieldNames(producedDataType).stream()
                        .filter(partitionColumns::contains)
                        .collect(Collectors.toList());

        List<String> mutableRowFieldNames =
                producedRowFieldNames.stream()
                        .filter(
                                key ->
                                        !metadataColumns.contains(key)
                                                && !partitionKeysToExtract.contains(key))
                        .collect(Collectors.toList());

        List<String> fixedRowFieldNames =
                Stream.concat(metadataColumns.stream(), partitionKeysToExtract.stream())
                        .collect(Collectors.toList());
        this.partitionColumns =
                partitionKeysToExtract.stream()
                        .map(
                                name ->
                                        new PartitionColumn(
                                                name,
                                                producedRowField
                                                        .get(producedRowFieldNames.indexOf(name))
                                                        .getDataType()))
                        .collect(Collectors.toList());

        this.extendedRowIndexMapping =
                EnrichedRowData.computeIndexMapping(
                        producedRowFieldNames, mutableRowFieldNames, fixedRowFieldNames);
    }

    public List<PartitionColumn> getPartitionColumns() {
        return partitionColumns;
    }

    public int[] getExtendedRowIndexMapping() {
        return extendedRowIndexMapping;
    }

    /** Info of the partition column. */
    public static class PartitionColumn implements Serializable {
        private static final long serialVersionUID = 1L;
        public final String fieldName;
        public final DataFormatConverters.DataFormatConverter converter;
        public final DataType dataType;

        public PartitionColumn(String fieldName, DataType dataType) {
            this.fieldName = fieldName;
            this.dataType = dataType;
            this.converter = DataFormatConverters.getConverterForDataType(dataType);
        }
    }
}
