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

package org.apache.flink.connector.upserttest.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.upserttest.sink.UpsertTestSink;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import java.io.File;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link DynamicTableSink} that describes how to create a {@link UpsertTestSink} from a logical
 * description.
 */
@Internal
class UpsertTestDynamicTableSink implements DynamicTableSink {

    private final DataType physicalRowDataType;
    private final EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat;
    private final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat;
    private final String outputFilePath;

    UpsertTestDynamicTableSink(
            DataType physicalRowDataType,
            EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat,
            EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat,
            String outputFilePath) {
        this.physicalRowDataType = checkNotNull(physicalRowDataType);
        this.keyEncodingFormat = checkNotNull(keyEncodingFormat);
        this.valueEncodingFormat = checkNotNull(valueEncodingFormat);
        this.outputFilePath = checkNotNull(outputFilePath);
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        for (RowKind kind : requestedMode.getContainedKinds()) {
            if (kind != RowKind.UPDATE_BEFORE) {
                builder.addContainedKind(kind);
            }
        }
        return builder.build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final File outputFile = new File(outputFilePath);
        final SerializationSchema<RowData> keySerialization =
                keyEncodingFormat.createRuntimeEncoder(context, physicalRowDataType);
        final SerializationSchema<RowData> valueSerialization =
                valueEncodingFormat.createRuntimeEncoder(context, physicalRowDataType);

        final UpsertTestSink<RowData> sink =
                UpsertTestSink.<RowData>builder()
                        .setOutputFile(outputFile)
                        .setKeySerializationSchema(keySerialization)
                        .setValueSerializationSchema(valueSerialization)
                        .build();
        return SinkV2Provider.of(sink, 1);
    }

    @Override
    public DynamicTableSink copy() {
        return new UpsertTestDynamicTableSink(
                physicalRowDataType, keyEncodingFormat, valueEncodingFormat, outputFilePath);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UpsertTestDynamicTableSink that = (UpsertTestDynamicTableSink) o;
        return Objects.equals(physicalRowDataType, that.physicalRowDataType)
                && Objects.equals(keyEncodingFormat, that.keyEncodingFormat)
                && Objects.equals(valueEncodingFormat, that.valueEncodingFormat)
                && Objects.equals(outputFilePath, that.outputFilePath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                physicalRowDataType, keyEncodingFormat, valueEncodingFormat, outputFilePath);
    }

    @Override
    public String asSummaryString() {
        return "UpsertTestSink";
    }
}
