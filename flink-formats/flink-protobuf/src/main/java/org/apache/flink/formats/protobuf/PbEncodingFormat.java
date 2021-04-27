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

package org.apache.flink.formats.protobuf;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.protobuf.serialize.PbRowDataSerializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

/** {@link EncodingFormat} for protobuf encoding. */
public class PbEncodingFormat implements EncodingFormat<SerializationSchema<RowData>> {
    private final PbFormatConfig pbFormatConfig;

    public PbEncodingFormat(PbFormatConfig pbFormatConfig) {
        this.pbFormatConfig = pbFormatConfig;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SerializationSchema<RowData> createRuntimeEncoder(
            DynamicTableSink.Context context, DataType consumedDataType) {
        RowType rowType = (RowType) consumedDataType.getLogicalType();
        return new PbRowDataSerializationSchema(rowType, pbFormatConfig);
    }
}
