/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.csv;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ContainerNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;

/** Utilities for using CSV format in PyFlink. */
public class PythonCsvUtils {

    /** Util for creating a {@link CsvReaderFormat}. */
    public static CsvReaderFormat<Object> createCsvReaderFormat(
            CsvSchema schema, DataType dataType) {
        Preconditions.checkArgument(dataType.getLogicalType() instanceof RowType);

        return new CsvReaderFormat<>(
                CsvMapper::new,
                ignored -> schema,
                JsonNode.class,
                new CsvToRowDataConverters(false)
                        .createRowConverter(
                                LogicalTypeUtils.toRowType(dataType.getLogicalType()), true),
                InternalTypeInfo.of(dataType.getLogicalType()),
                false);
    }

    /**
     * Util for creating a {@link
     * RowDataToCsvConverters.RowDataToCsvConverter.RowDataToCsvFormatConverterContext}.
     */
    public static RowDataToCsvConverters.RowDataToCsvConverter.RowDataToCsvFormatConverterContext
            createRowDataToCsvFormatConverterContext(CsvMapper mapper, ContainerNode<?> container) {
        return new RowDataToCsvConverters.RowDataToCsvConverter.RowDataToCsvFormatConverterContext(
                mapper, container);
    }

    /**
     * Util for creating a {@link BulkWriter.Factory} that wraps {@link CsvBulkWriter#forSchema}.
     */
    public static BulkWriter.Factory<RowData> createCsvBulkWriterFactory(
            CsvSchema schema, DataType physicalDataType) {

        return CsvFileFormatFactory.createCsvBulkWriterFactory(
                schema, (RowType) physicalDataType.getLogicalType());
    }
}
