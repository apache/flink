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

package org.apache.flink.connector.pulsar.table.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import org.apache.pulsar.client.api.Message;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;

import javax.annotation.Nullable;

/** A specific {@link PulsarDeserializationSchema} for {@link PulsarDynamicTableSource}. */
class DynamicPulsarDeserializationSchema implements PulsarDeserializationSchema<RowData> {

    private static final long serialVersionUID = -5072225094380530611L;

    @Nullable private final DeserializationSchema<RowData> keyDeserialization;

    private final DeserializationSchema<RowData> valueDeserialization;

    // TODO metadata support can be abstract to another class
    private final boolean hasMetadata;

    // TODO what does this do ?
    private final OutputProjectionCollector outputCollector;

    // TODO what does this do ?
    private final TypeInformation<RowData> producedTypeInfo;

    // TODO upsert support can be abstracted to another class
    private final boolean upsertMode;

    // TODO what does this do ?
    private static final ThreadLocal<SimpleCollector<RowData>> tlsCollector =
            new ThreadLocal<SimpleCollector<RowData>>() {
                @Override
                public SimpleCollector initialValue() {
                    return new SimpleCollector();
                }
            };

    DynamicPulsarDeserializationSchema(
            int physicalArity,
            @Nullable DeserializationSchema<RowData> keyDeserialization,
            int[] keyProjection,
            DeserializationSchema<RowData> valueDeserialization,
            int[] valueProjection,
            boolean hasMetadata,
            MetadataConverter[] metadataConverters,
            TypeInformation<RowData> producedTypeInfo,
            boolean upsertMode) {
        // TODO extract this to a method ?
        if (upsertMode) {
            Preconditions.checkArgument(
                    keyDeserialization != null && keyProjection.length > 0,
                    "Key must be set in upsert mode for deserialization schema.");
        }
        this.keyDeserialization = ThreadSafeDeserializationSchema.of(keyDeserialization);
        this.valueDeserialization = ThreadSafeDeserializationSchema.of(valueDeserialization);
        this.hasMetadata = hasMetadata;
        this.outputCollector =
                new OutputProjectionCollector(
                        physicalArity,
                        keyProjection,
                        valueProjection,
                        metadataConverters,
                        upsertMode);
        this.producedTypeInfo = producedTypeInfo;
        this.upsertMode = upsertMode;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        if (keyDeserialization != null) {
            keyDeserialization.open(context);
        }
        valueDeserialization.open(context);
    }

    @Override
    public RowData deserialize(Message<RowData> message) throws IOException {
        final SimpleCollector<RowData> collector = tlsCollector.get();
        deserialize(message, collector);
        return collector.takeRecord();
    }

    @Override
    public void deserialize(Message<byte[]> message, Collector<RowData> collector)
            throws IOException {
        // shortcut in case no output projection is required,
        // also not for a cartesian product with the keys
        if (keyDeserialization == null && !hasMetadata) {
            valueDeserialization.deserialize(message.getData(), collector);
            return;
        }
        BufferingCollector keyCollector = new BufferingCollector();

        // buffer key(s)
        if (keyDeserialization != null) {
            // TODO why it has to be from the keyBytes ?
            keyDeserialization.deserialize(message.getKeyBytes(), keyCollector);
        }

        // TODO assemble this data to a more stable data format
        // TODO key is a list of row data as well ? Key schema is useful in upsert mode
        // project output while emitting values
        outputCollector.inputMessage = message;
        // TODO why add the physicalKeyRows to the keyCollector Why there is a list of row data?
        outputCollector.physicalKeyRows = keyCollector.buffer;
        outputCollector.outputCollector = collector;
        if ((message.getData() == null || message.getData().length == 0) && upsertMode) {
            // collect tombstone messages in upsert mode by hand
            outputCollector.collect(null);
        } else {
            valueDeserialization.deserialize(message.getData(), outputCollector);
        }
        keyCollector.buffer.clear();
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }

    @Override
    public Schema<RowData> getSchema() {
        return new FlinkSchema<>(Schema.BYTES.getSchemaInfo(), null, valueDeserialization);
    }

    // --------------------------------------------------------------------------------------------

    interface MetadataConverter extends Serializable {
        Object read(Message<?> message);
    }

    // --------------------------------------------------------------------------------------------

    private static final class BufferingCollector implements Collector<RowData>, Serializable {

        private static final long serialVersionUID = 1L;

        private final List<RowData> buffer = new ArrayList<>();

        @Override
        public void collect(RowData record) {
            buffer.add(record);
        }

        @Override
        public void close() {
            // nothing to do
        }
    }

    private static class SimpleCollector<T> implements Collector<T> {
        private T record;

        @Override
        public void collect(T record) {
            this.record = record;
        }

        @Override
        public void close() {
            // Nothing to do.
        }

        private T getRecord() {
            return record;
        }

        private T takeRecord() {
            T result = record;
            reset();
            return result;
        }

        private void reset() {
            record = null;
        }
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Emits a row with key, value, and metadata fields.
     *
     * <p>The collector is able to handle the following kinds of keys:
     *
     * <ul>
     *   <li>No key is used.
     *   <li>A key is used.
     *   <li>The deserialization schema emits multiple keys.
     *   <li>Keys and values have overlapping fields.
     *   <li>Keys are used and value is null.
     * </ul>
     */
    private static final class OutputProjectionCollector
            implements Collector<RowData>, Serializable {

        private static final long serialVersionUID = 1L;

        private final int physicalArity;

        private final int[] keyProjection;

        private final int[] valueProjection;

        private final MetadataConverter[] metadataConverters;

        private final boolean upsertMode;

        private transient Message<?> inputMessage;

        private transient List<RowData> physicalKeyRows;

        private transient Collector<RowData> outputCollector;

        OutputProjectionCollector(
                int physicalArity,
                int[] keyProjection,
                int[] valueProjection,
                MetadataConverter[] metadataConverters,
                boolean upsertMode) {
            this.physicalArity = physicalArity;
            this.keyProjection = keyProjection;
            this.valueProjection = valueProjection;
            this.metadataConverters = metadataConverters;
            this.upsertMode = upsertMode;
        }

        @Override
        public void collect(RowData physicalValueRow) {
            // no key defined
            if (keyProjection.length == 0) {
                emitRow(null, (GenericRowData) physicalValueRow);
                return;
            }

            // TODO what does it mean to emit a value for each key
            // otherwise emit a value for each key
            for (RowData physicalKeyRow : physicalKeyRows) {
                emitRow((GenericRowData) physicalKeyRow, (GenericRowData) physicalValueRow);
            }
        }

        @Override
        public void close() {
            // nothing to do
        }

        private void emitRow(
                @Nullable GenericRowData physicalKeyRow,
                @Nullable GenericRowData physicalValueRow) {
            final RowKind rowKind;
            if (physicalValueRow == null) {
                if (upsertMode) {
                    rowKind = RowKind.DELETE;
                } else {
                    throw new DeserializationException(
                            "Invalid null value received in non-upsert mode. Could not to set row kind for output record.");
                }
            } else {
                rowKind = physicalValueRow.getRowKind();
            }

            final int metadataArity = metadataConverters.length;
            // TODO need to combine physicalArity and metadataArity
            // TODO metadata arity
            final GenericRowData producedRow =
                    new GenericRowData(rowKind, physicalArity + metadataArity);

            if (physicalValueRow != null) {
                for (int valuePos = 0; valuePos < valueProjection.length; valuePos++) {
                    producedRow.setField(
                            valueProjection[valuePos], physicalValueRow.getField(valuePos));
                }
            }

            // TODO put both key and value fields in the produced row
            for (int keyPos = 0; keyPos < keyProjection.length; keyPos++) {
                assert physicalKeyRow != null;
                producedRow.setField(keyProjection[keyPos], physicalKeyRow.getField(keyPos));
            }

            // TODO put the metadata in the produced row as well
            for (int metadataPos = 0; metadataPos < metadataArity; metadataPos++) {
                producedRow.setField(
                        physicalArity + metadataPos,
                        metadataConverters[metadataPos].read(inputMessage));
            }

            outputCollector.collect(producedRow);
        }
    }
}
