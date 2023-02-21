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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.util.function.Function;

/** Testing utils for tests related to {@link AbstractAvroBulkFormat}. */
public class AvroBulkFormatTestUtils {

    public static final RowType ROW_TYPE =
            RowType.of(
                    false,
                    new LogicalType[] {
                        DataTypes.STRING().getLogicalType(), DataTypes.STRING().getLogicalType()
                    },
                    new String[] {"a", "b"});

    /** {@link AbstractAvroBulkFormat} for tests. */
    public static class TestingAvroBulkFormat
            extends AbstractAvroBulkFormat<GenericRecord, RowData, FileSourceSplit> {

        protected TestingAvroBulkFormat() {
            super(AvroSchemaConverter.convertToSchema(ROW_TYPE));
        }

        @Override
        protected GenericRecord createReusedAvroRecord() {
            return new GenericData.Record(readerSchema);
        }

        @Override
        protected Function<GenericRecord, RowData> createConverter() {
            AvroToRowDataConverters.AvroToRowDataConverter converter =
                    AvroToRowDataConverters.createRowConverter(ROW_TYPE);
            return record -> record == null ? null : (RowData) converter.convert(record);
        }

        @Override
        public TypeInformation<RowData> getProducedType() {
            return InternalTypeInfo.of(ROW_TYPE);
        }
    }
}
