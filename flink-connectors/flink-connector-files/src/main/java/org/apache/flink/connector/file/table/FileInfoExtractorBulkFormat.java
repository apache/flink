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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This {@link BulkFormat} is a wrapper that attaches file information columns to the output
 * records.
 */
public class FileInfoExtractorBulkFormat<SplitT extends FileSourceSplit>
        implements BulkFormat<RowData, SplitT> {

    private final BulkFormat<RowData, SplitT> wrapped;
    private final PartitionFieldExtractor<SplitT> partitionFieldExtractor;
    private final FileInfoExtractor fileInfoExtractor;
    public final List<FileSystemTableSource.FileInfoAccessor> metadataColumnsFunctions;
    public final TypeInformation<RowData> producedType;

    public FileInfoExtractorBulkFormat(
            BulkFormat<RowData, SplitT> wrapped,
            DataType producedDataType,
            TypeInformation<RowData> producedTypeInformation,
            Map<String, FileSystemTableSource.FileInfoAccessor> metadataColumns,
            List<String> partitionColumns,
            PartitionFieldExtractor<SplitT> partitionFieldExtractor) {
        List<String> metadataFieldNames = new ArrayList<>(metadataColumns.keySet());
        this.wrapped = wrapped;
        this.partitionFieldExtractor = partitionFieldExtractor;
        this.producedType = producedTypeInformation;
        this.fileInfoExtractor =
                new FileInfoExtractor(producedDataType, metadataFieldNames, partitionColumns);
        this.metadataColumnsFunctions =
                metadataFieldNames.stream().map(metadataColumns::get).collect(Collectors.toList());
    }

    @Override
    public Reader<RowData> createReader(Configuration config, SplitT split) throws IOException {
        return wrapReader(wrapped.createReader(config, split), split);
    }

    @Override
    public Reader<RowData> restoreReader(Configuration config, SplitT split) throws IOException {
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

    private Reader<RowData> wrapReader(Reader<RowData> superReader, SplitT split) {
        // Fill the metadata + partition columns row
        List<FileInfoExtractor.PartitionColumn> partitionColumns =
                fileInfoExtractor.getPartitionColumns();
        final GenericRowData fileInfoRowData =
                new GenericRowData(metadataColumnsFunctions.size() + partitionColumns.size());
        int fileInfoRowIndex = 0;
        for (; fileInfoRowIndex < metadataColumnsFunctions.size(); fileInfoRowIndex++) {
            fileInfoRowData.setField(
                    fileInfoRowIndex,
                    metadataColumnsFunctions.get(fileInfoRowIndex).getValue(split));
        }
        if (!partitionColumns.isEmpty()) {
            for (int partitionFieldIndex = 0;
                    fileInfoRowIndex < fileInfoRowData.getArity();
                    fileInfoRowIndex++, partitionFieldIndex++) {
                FileInfoExtractor.PartitionColumn partition =
                        partitionColumns.get(partitionFieldIndex);

                Object partitionValue =
                        partitionFieldExtractor.extract(
                                split, partition.fieldName, partition.dataType.getLogicalType());

                fileInfoRowData.setField(
                        fileInfoRowIndex, partition.converter.toInternal(partitionValue));
            }
        }

        // This row is going to be reused for every record
        final EnrichedRowData producedRowData =
                new EnrichedRowData(
                        fileInfoRowData, fileInfoExtractor.getExtendedRowIndexMapping());

        return RecordMapperWrapperRecordIterator.wrapReader(
                superReader,
                physicalRowData -> {
                    producedRowData.replaceMutableRow(physicalRowData);
                    return producedRowData;
                });
    }
}
