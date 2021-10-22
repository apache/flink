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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.RecordMapperWrapperRecordIterator;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.EnrichedRowData;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This {@link BulkFormat} is a wrapper that attaches file information columns to the output
 * records.
 */
class FileInfoExtractorBulkFormat implements BulkFormat<RowData, FileSourceSplit> {

    private final BulkFormat<RowData, FileSourceSplit> wrapped;
    private final TypeInformation<RowData> producedType;

    private final List<FileSystemTableSource.FileInfoAccessor> metadataColumnsFunctions;
    private final int[] extendedRowIndexMapping;

    public FileInfoExtractorBulkFormat(
            BulkFormat<RowData, FileSourceSplit> wrapped,
            DataType fullDataType,
            TypeInformation<RowData> producedType,
            Map<String, FileSystemTableSource.FileInfoAccessor> metadataColumns) {
        this.wrapped = wrapped;
        this.producedType = producedType;

        // Compute index mapping for the extended row and the functions to compute metadata
        List<String> completeRowFields = DataType.getFieldNames(fullDataType);
        List<String> mutableRowFields =
                completeRowFields.stream()
                        .filter(key -> !metadataColumns.containsKey(key))
                        .collect(Collectors.toList());
        List<String> fixedRowFields = new ArrayList<>(metadataColumns.keySet());
        this.extendedRowIndexMapping =
                EnrichedRowData.computeIndexMapping(
                        completeRowFields, mutableRowFields, fixedRowFields);
        this.metadataColumnsFunctions =
                fixedRowFields.stream().map(metadataColumns::get).collect(Collectors.toList());
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
        // Fill the metadata row
        final GenericRowData metadataRowData = new GenericRowData(metadataColumnsFunctions.size());
        for (int i = 0; i < metadataColumnsFunctions.size(); i++) {
            metadataRowData.setField(i, metadataColumnsFunctions.get(i).getValue(split));
        }

        // This row is going to be reused for every record
        final EnrichedRowData enrichedRowData =
                new EnrichedRowData(metadataRowData, this.extendedRowIndexMapping);

        return new ReaderWrapper(superReader, enrichedRowData);
    }

    private static final class ReaderWrapper implements Reader<RowData> {

        private final Reader<RowData> wrappedReader;
        private final EnrichedRowData enrichedRowData;

        private ReaderWrapper(Reader<RowData> wrappedReader, EnrichedRowData enrichedRowData) {
            this.wrappedReader = wrappedReader;
            this.enrichedRowData = enrichedRowData;
        }

        @Nullable
        @Override
        public RecordIterator<RowData> readBatch() throws IOException {
            RecordIterator<RowData> iterator = wrappedReader.readBatch();
            if (iterator == null) {
                return null;
            }
            return new RecordMapperWrapperRecordIterator<>(
                    iterator,
                    physicalRowData -> {
                        enrichedRowData.replaceMutableRow(physicalRowData);
                        return enrichedRowData;
                    });
        }

        @Override
        public void close() throws IOException {
            this.wrappedReader.close();
        }
    }
}
