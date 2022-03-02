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
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import org.apache.pulsar.client.api.Schema;

import java.io.Serializable;
import java.util.Base64;
import java.util.Map;

// TODO the PulsarSerializationSchema should be serializable ? ResultTypeQueriable ?
public class PulsarDynamicTableSerializationSchema implements PulsarSerializationSchema<RowData> {

    private static final long serialVersionUID = 7314442107082067836L;

    private final SerializationSchema<RowData> keySerialization;

    private final RowData.FieldGetter[] keyFieldGetters;

    private final SerializationSchema<RowData> valueSerialization;

    private final RowData.FieldGetter[] valueFieldGetters;

    /**
     * Contains the position for each value of {@link PulsarDynamicTableSink.WritableMetadata} in
     * the consumed row or -1 if this metadata key is not used.
     */
    private final int[] metadataPositions;

    // TODO what does this do
    private DataType valueDataType;

    private volatile Schema<RowData> schema;

    public PulsarDynamicTableSerializationSchema(
            SerializationSchema<RowData> keySerialization,
            RowData.FieldGetter[] keyFieldGetters,
            SerializationSchema<RowData> valueSerialization,
            RowData.FieldGetter[] valueFieldGetters,
            int[] metadataPositions,
            DataType valueDataType,
            long delayMilliseconds) {
        // TODO this does not make sense, FLink SQL Key can come from message Body
        this.keySerialization = keySerialization;
        this.keyFieldGetters = keyFieldGetters;
        this.valueSerialization = valueSerialization;
        this.valueFieldGetters = valueFieldGetters;
        this.metadataPositions = metadataPositions;
        // TODO valueDataType is not needed
        this.valueDataType = valueDataType;
    }

    @Override
    public void open(
            SerializationSchema.InitializationContext initializationContext,
            PulsarSinkContext sinkContext,
            SinkConfiguration sinkConfiguration)
            throws Exception {
        valueSerialization.open(initializationContext);
    }

    // TODO this implementation is just a skeletion
    @Override
    public PulsarMessage<?> serialize(RowData consumedRow, PulsarSinkContext sinkContext) {

        PulsarMessageBuilder<byte[]> messageBuilder = new PulsarMessageBuilder<>();


        // TODO we probably don't need the projectedRow
        final RowKind kind = consumedRow.getRowKind();
        final RowData valueRow = createProjectedRow(consumedRow, kind, valueFieldGetters);

        // TODO metadata are appended in the properties. TODO is this a good practice ?
        Map<String, String> properties =
                readMetadata(consumedRow, PulsarDynamicTableSink.WritableMetadata.PROPERTIES);
        if (properties != null) {
            messageBuilder.properties(properties);
        }
        final Long eventTime =
                readMetadata(consumedRow, PulsarDynamicTableSink.WritableMetadata.EVENT_TIME);
        if (eventTime != null && eventTime >= 0) {
            messageBuilder.eventTime(eventTime);
        }


        // TODO serialize key
        if (keySerialization != null) {
            final RowData keyRow = createProjectedRow(consumedRow, RowKind.INSERT, keyFieldGetters);
            // TODO here the keyBytes needs to be taken care of.
            final byte[] keyBytes = keySerialization.serialize(keyRow);
            // We can't simply encode it, but we need to encode it as well.
            messageBuilder.key(keyBytes);
        }

        // TODO serialize value.
        byte[] serializedData = valueSerialization.serialize(valueRow);
        messageBuilder.value(Schema.BYTES, serializedData);
        return messageBuilder.build();
    }

    // TODO why do we need this ?

    @SuppressWarnings("unchecked")
    private <T> T readMetadata(
            RowData consumedRow, PulsarDynamicTableSink.WritableMetadata metadata) {
        final int pos = metadataPositions[metadata.ordinal()];
        if (pos < 0) {
            return null;
        }
        return (T) metadata.converter.read(consumedRow, pos);
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

    // --------------------------------------------------------------------------------------------

    public interface MetadataConverter extends Serializable {
        Object read(RowData consumedRow, int pos);
    }
}
