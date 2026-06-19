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
import org.apache.flink.table.data.utils.ProjectedRowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import java.io.File;
import java.util.Arrays;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link DynamicTableSink} that describes how to create a {@link UpsertTestSink} from a logical
 * description.
 */
@Internal
class UpsertTestDynamicTableSink implements DynamicTableSink {

    private final DataType physicalRowDataType;
    private final DataType primaryKeyDataType;
    private final int[] primaryKeyIndexes;
    private final EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat;
    private final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat;
    private final String outputFilePath;

    UpsertTestDynamicTableSink(
            DataType physicalRowDataType,
            DataType primaryKeyDataType,
            int[] primaryKeyIndexes,
            EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat,
            EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat,
            String outputFilePath) {
        this.physicalRowDataType = checkNotNull(physicalRowDataType);
        this.primaryKeyIndexes = checkNotNull(primaryKeyIndexes);
        this.primaryKeyDataType = checkNotNull(primaryKeyDataType);
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
        final SerializationSchema<RowData> keySerialization = createKeySerializationSchema(context);
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
                physicalRowDataType,
                primaryKeyDataType,
                primaryKeyIndexes,
                keyEncodingFormat,
                valueEncodingFormat,
                outputFilePath);
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
                && Objects.equals(primaryKeyDataType, that.primaryKeyDataType)
                && Arrays.equals(primaryKeyIndexes, that.primaryKeyIndexes)
                && Objects.equals(keyEncodingFormat, that.keyEncodingFormat)
                && Objects.equals(valueEncodingFormat, that.valueEncodingFormat)
                && Objects.equals(outputFilePath, that.outputFilePath);
    }

    @Override
    public int hashCode() {
        return 31
                        * Objects.hash(
                                physicalRowDataType,
                                primaryKeyDataType,
                                keyEncodingFormat,
                                valueEncodingFormat,
                                outputFilePath)
                + Arrays.hashCode(primaryKeyIndexes);
    }

    @Override
    public String asSummaryString() {
        return "UpsertTestSink";
    }

    private SerializationSchema<RowData> createKeySerializationSchema(Context context) {
        final SerializationSchema<RowData> serializationSchema =
                keyEncodingFormat.createRuntimeEncoder(context, primaryKeyDataType);
        if (primaryKeyIndexes.length > 0) {
            return new UpsertKeySerializationSchema(serializationSchema, primaryKeyIndexes);
        }
        return serializationSchema;
    }

    /**
     * {@code SerializationSchema} that extracts and serializes the primary key fields from given
     * {@link RowData}.
     */
    private static class UpsertKeySerializationSchema implements SerializationSchema<RowData> {

        private final SerializationSchema<RowData> serializationSchema;

        private final int[] primaryKeyIndexes;

        private UpsertKeySerializationSchema(
                SerializationSchema<RowData> serializationSchema, int[] primaryKeyIndexes) {
            this.serializationSchema = serializationSchema;
            this.primaryKeyIndexes = primaryKeyIndexes;
        }

        @Override
        public void open(InitializationContext context) throws Exception {
            serializationSchema.open(context);
        }

        @Override
        public byte[] serialize(RowData element) {
            RowData primaryKeyRowData =
                    ProjectedRowData.from(primaryKeyIndexes).replaceRow(element);
            return serializationSchema.serialize(primaryKeyRowData);
        }
    }
}
