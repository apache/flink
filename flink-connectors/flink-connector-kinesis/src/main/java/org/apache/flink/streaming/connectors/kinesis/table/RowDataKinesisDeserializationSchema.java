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

package org.apache.flink.streaming.connectors.kinesis.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A {@link KinesisDeserializationSchema} adaptor for {@link RowData} records that delegates
 * physical data deserialization to an inner {@link DeserializationSchema} and appends requested
 * metadata to the end of the deserialized {@link RowData} record.
 */
@Internal
public final class RowDataKinesisDeserializationSchema
        implements KinesisDeserializationSchema<RowData> {

    /** Internal type for enumerating available metadata. */
    protected enum Metadata {
        Timestamp("timestamp", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull()),
        SequenceNumber("sequence-number", DataTypes.VARCHAR(128).notNull()),
        ShardId("shard-id", DataTypes.VARCHAR(128).notNull());

        private final String fieldName;
        private final DataType dataType;

        Metadata(String fieldName, DataType dataType) {
            this.fieldName = fieldName;
            this.dataType = dataType;
        }

        public String getFieldName() {
            return this.fieldName;
        }

        public DataType getDataType() {
            return this.dataType;
        }

        public static Metadata of(String fieldName) {
            return Arrays.stream(Metadata.values())
                    .filter(m -> Objects.equals(m.fieldName, fieldName))
                    .findFirst()
                    .orElseThrow(
                            () -> {
                                String msg =
                                        "Cannot find Metadata instance for field name '"
                                                + fieldName
                                                + "'";
                                return new IllegalArgumentException(msg);
                            });
        }
    }

    private static final long serialVersionUID = 5551095193778230749L;

    /** A {@link DeserializationSchema} to deserialize the physical part of the row. */
    private final DeserializationSchema<RowData> physicalDeserializer;

    /** The type of the produced {@link RowData} records (physical data with appended metadata]. */
    private final TypeInformation<RowData> producedTypeInfo;

    /** Metadata fields to be appended to the physical {@link RowData} in the produced records. */
    private final List<Metadata> requestedMetadataFields;

    public RowDataKinesisDeserializationSchema(
            DeserializationSchema<RowData> physicalDeserializer,
            TypeInformation<RowData> producedTypeInfo,
            List<Metadata> requestedMetadataFields) {
        this.physicalDeserializer = Preconditions.checkNotNull(physicalDeserializer);
        this.producedTypeInfo = Preconditions.checkNotNull(producedTypeInfo);
        this.requestedMetadataFields = Preconditions.checkNotNull(requestedMetadataFields);
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        physicalDeserializer.open(context);
    }

    @Override
    public RowData deserialize(
            byte[] recordValue,
            String partitionKey,
            String seqNum,
            long approxArrivalTimestamp,
            String stream,
            String shardId)
            throws IOException {

        RowData physicalRow = physicalDeserializer.deserialize(recordValue);
        GenericRowData metadataRow = new GenericRowData(requestedMetadataFields.size());

        for (int i = 0; i < metadataRow.getArity(); i++) {
            Metadata metadataField = requestedMetadataFields.get(i);
            if (metadataField == Metadata.Timestamp) {
                metadataRow.setField(i, TimestampData.fromEpochMillis(approxArrivalTimestamp));
            } else if (metadataField == Metadata.SequenceNumber) {
                metadataRow.setField(i, StringData.fromString(seqNum));
            } else if (metadataField == Metadata.ShardId) {
                metadataRow.setField(i, StringData.fromString(shardId));
            } else {
                String msg = String.format("Unsupported metadata key %s", metadataField);
                throw new RuntimeException(msg); // should never happen
            }
        }

        JoinedRowData joinedRowData = new JoinedRowData(physicalRow, metadataRow);
        joinedRowData.setRowKind(physicalRow.getRowKind());
        return joinedRowData;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }
}
