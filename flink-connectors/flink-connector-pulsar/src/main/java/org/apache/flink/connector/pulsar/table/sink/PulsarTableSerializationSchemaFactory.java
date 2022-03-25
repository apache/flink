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
import org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchema;
import org.apache.flink.connector.pulsar.table.sink.impl.PulsarWritableMetadata;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.DataTypeUtils;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Contains needed field mapping and encoding format information to construct a {@link
 * org.apache.flink.connector.pulsar.table.sink.PulsarTableSerializationSchema} instance.
 */
public class PulsarTableSerializationSchemaFactory {

    protected final DataType physicalDataType;

    protected final EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat;

    protected final int[] keyProjection;

    protected final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat;

    protected final int[] valueProjection;

    /** Metadata that is appended at the end of a physical sink row. */
    protected List<String> writableMetadataKeys;

    public PulsarTableSerializationSchemaFactory(
            DataType physicalDataType,
            EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat,
            int[] keyProjection,
            EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat,
            int[] valueProjection) {
        // TODO add null check
        this.physicalDataType = physicalDataType;
        this.keyEncodingFormat = keyEncodingFormat;
        this.keyProjection = keyProjection;
        this.valueEncodingFormat = valueEncodingFormat;
        this.valueProjection = valueProjection;
        this.writableMetadataKeys = Collections.emptyList();
    }

    public PulsarSerializationSchema<RowData> createPulsarSerializationSchema(
            DynamicTableSink.Context context) {

        final SerializationSchema<RowData> keySerialization =
                createSerialization(context, keyEncodingFormat, keyProjection, null);

        final SerializationSchema<RowData> valueSerialization =
                createSerialization(context, valueEncodingFormat, valueProjection, null);

        // TODO check Kafka implementation
        final List<LogicalType> physicalChildren = physicalDataType.getLogicalType().getChildren();

        final RowData.FieldGetter[] keyFieldGetters =
                getFieldGetters(physicalChildren, keyProjection);
        final RowData.FieldGetter[] valueFieldGetters =
                getFieldGetters(physicalChildren, valueProjection);

        // determine the positions of metadata in the consumed row
        // TODO abstract this to a different
        final PulsarWritableMetadata writableMetadata =
                new PulsarWritableMetadata(writableMetadataKeys, physicalChildren.size());

        return new PulsarTableSerializationSchema(
                keySerialization,
                keyFieldGetters,
                valueSerialization,
                valueFieldGetters,
                writableMetadata);
    }

    private @Nullable SerializationSchema<RowData> createSerialization(
            DynamicTableSink.Context context,
            @Nullable EncodingFormat<SerializationSchema<RowData>> format,
            int[] projection,
            @Nullable String prefix) {
        if (format == null) {
            return null;
        }
        DataType physicalFormatDataType = Projection.of(projection).project(this.physicalDataType);
        if (prefix != null) {
            physicalFormatDataType = DataTypeUtils.stripRowPrefix(physicalFormatDataType, prefix);
        }
        return format.createRuntimeEncoder(context, physicalFormatDataType);
    }

    private RowData.FieldGetter[] getFieldGetters(
            List<LogicalType> physicalChildren, int[] projection) {
        return Arrays.stream(projection)
                .mapToObj(
                        targetField ->
                                RowData.createFieldGetter(
                                        physicalChildren.get(targetField), targetField))
                .toArray(RowData.FieldGetter[]::new);
    }

    public void setWritableMetadataKeys(List<String> writableMetadataKeys) {
        this.writableMetadataKeys = writableMetadataKeys;
    }
}
