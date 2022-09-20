/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.pulsar.table.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;
import org.apache.flink.connector.pulsar.sink.writer.context.PulsarSinkContext;
import org.apache.flink.connector.pulsar.sink.writer.message.PulsarMessage;
import org.apache.flink.connector.pulsar.sink.writer.message.PulsarMessageBuilder;
import org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import org.apache.pulsar.client.api.Schema;

import javax.annotation.Nullable;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link PulsarSerializationSchema} implementation for Pulsar SQL sink connector. It is
 * responsible for retrieving fields from Flink row and serialize into Pulsar message key or body,
 * and set necessary metadata fields as required.
 */
public class PulsarTableSerializationSchema implements PulsarSerializationSchema<RowData> {

    private static final long serialVersionUID = 7314442107082067836L;

    @Nullable private final SerializationSchema<RowData> keySerialization;

    private final RowData.FieldGetter[] keyFieldGetters;

    private final SerializationSchema<RowData> valueSerialization;

    private final RowData.FieldGetter[] valueFieldGetters;

    private final PulsarWritableMetadata writableMetadata;

    private final boolean upsertMode;

    public PulsarTableSerializationSchema(
            @Nullable SerializationSchema<RowData> keySerialization,
            RowData.FieldGetter[] keyFieldGetters,
            SerializationSchema<RowData> valueSerialization,
            RowData.FieldGetter[] valueFieldGetters,
            PulsarWritableMetadata writableMetadata,
            boolean upsertMode) {
        this.keySerialization = keySerialization;
        this.keyFieldGetters = checkNotNull(keyFieldGetters);
        this.valueSerialization = checkNotNull(valueSerialization);
        this.valueFieldGetters = checkNotNull(valueFieldGetters);
        this.writableMetadata = checkNotNull(writableMetadata);
        this.upsertMode = upsertMode;
    }

    @Override
    public void open(
            SerializationSchema.InitializationContext initializationContext,
            PulsarSinkContext sinkContext,
            SinkConfiguration sinkConfiguration)
            throws Exception {
        if (keySerialization != null) {
            keySerialization.open(initializationContext);
        }
        valueSerialization.open(initializationContext);
    }

    @Override
    public PulsarMessage<?> serialize(RowData consumedRow, PulsarSinkContext sinkContext) {

        PulsarMessageBuilder<byte[]> messageBuilder = new PulsarMessageBuilder<>();

        final RowKind kind = consumedRow.getRowKind();
        final byte[] serializedData;
        if (upsertMode) {
            if (kind == RowKind.DELETE || kind == RowKind.UPDATE_BEFORE) {
                // use null message as the tombstone message
                serializedData = null;
            } else {
                final RowData valueRow = createProjectedRow(consumedRow, kind, valueFieldGetters);
                valueRow.setRowKind(RowKind.INSERT);
                serializedData = valueSerialization.serialize(valueRow);
            }
        } else {
            final RowData valueRow = createProjectedRow(consumedRow, kind, valueFieldGetters);
            serializedData = valueSerialization.serialize(valueRow);
        }

        // apply metadata
        writableMetadata.applyWritableMetadataInMessage(consumedRow, messageBuilder);

        // get key row data
        if (keySerialization != null) {
            final RowData keyRow = createProjectedRow(consumedRow, RowKind.INSERT, keyFieldGetters);
            messageBuilder.keyBytes(keySerialization.serialize(keyRow));
        }

        messageBuilder.value(Schema.BYTES, serializedData);
        return messageBuilder.build();
    }

    private static RowData createProjectedRow(
            RowData consumedRow, RowKind kind, RowData.FieldGetter[] fieldGetters) {
        final int arity = fieldGetters.length;
        final GenericRowData genericRowData = new GenericRowData(kind, arity);
        for (int fieldPos = 0; fieldPos < arity; fieldPos++) {
            genericRowData.setField(fieldPos, fieldGetters[fieldPos].getFieldOrNull(consumedRow));
        }
        return genericRowData;
    }

    /** A class to read fields from Flink row and map to a Pulsar metadata. */
    public interface MetadataConverter extends Serializable {
        Object read(RowData consumedRow, int pos);
    }
}
