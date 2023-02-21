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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.ProjectedRowData;

import java.io.IOException;

/**
 * This {@link BulkFormat} is a wrapper that performs projections for formats that don't support
 * projections.
 */
class ProjectingBulkFormat implements BulkFormat<RowData, FileSourceSplit> {

    private final BulkFormat<RowData, FileSourceSplit> wrapped;
    private final TypeInformation<RowData> producedType;

    private final int[] projections;

    public ProjectingBulkFormat(
            BulkFormat<RowData, FileSourceSplit> wrapped,
            int[] projections,
            TypeInformation<RowData> producedType) {
        this.wrapped = wrapped;
        this.projections = projections;
        this.producedType = producedType;
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
        // This row is going to be reused for every record
        final ProjectedRowData producedRowData = ProjectedRowData.from(this.projections);

        return RecordMapperWrapperRecordIterator.wrapReader(
                superReader,
                physicalRowData -> {
                    producedRowData.replaceRow(physicalRowData);
                    return producedRowData;
                });
    }
}
