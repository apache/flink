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
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A factory class which contains required fields mapping and encoding format information to
 * construct a {@link PulsarTableSerializationSchema} instance.
 */
public class PulsarTableSerializationSchemaFactory {

    private final DataType physicalDataType;

    @Nullable private final EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat;

    private final int[] keyProjection;

    private final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat;

    private final int[] valueProjection;

    /** Metadata that is appended at the end of a physical sink row. */
    private List<String> writableMetadataKeys;

    private final boolean upsertMode;

    public PulsarTableSerializationSchemaFactory(
            DataType physicalDataType,
            @Nullable EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat,
            int[] keyProjection,
            EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat,
            int[] valueProjection,
            boolean upsertMode) {
        this.physicalDataType = checkNotNull(physicalDataType);
        this.keyEncodingFormat = keyEncodingFormat;
        this.keyProjection = checkNotNull(keyProjection);
        this.valueEncodingFormat = checkNotNull(valueEncodingFormat);
        this.valueProjection = checkNotNull(valueProjection);
        this.writableMetadataKeys = Collections.emptyList();
        this.upsertMode = upsertMode;
    }

    public PulsarSerializationSchema<RowData> createPulsarSerializationSchema(
            DynamicTableSink.Context context) {

        final SerializationSchema<RowData> keySerialization =
                createSerialization(context, keyEncodingFormat, keyProjection, null);

        final SerializationSchema<RowData> valueSerialization =
                createSerialization(context, valueEncodingFormat, valueProjection, null);
        final List<LogicalType> physicalChildren = physicalDataType.getLogicalType().getChildren();

        final RowData.FieldGetter[] keyFieldGetters =
                getFieldGetters(physicalChildren, keyProjection);
        final RowData.FieldGetter[] valueFieldGetters =
                getFieldGetters(physicalChildren, valueProjection);

        final PulsarWritableMetadata writableMetadata =
                new PulsarWritableMetadata(writableMetadataKeys, physicalChildren.size());

        return new PulsarTableSerializationSchema(
                keySerialization,
                keyFieldGetters,
                valueSerialization,
                valueFieldGetters,
                writableMetadata,
                upsertMode);
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PulsarTableSerializationSchemaFactory that = (PulsarTableSerializationSchemaFactory) o;
        return Objects.equals(physicalDataType, that.physicalDataType)
                && Objects.equals(keyEncodingFormat, that.keyEncodingFormat)
                && Arrays.equals(keyProjection, that.keyProjection)
                && Objects.equals(valueEncodingFormat, that.valueEncodingFormat)
                && Arrays.equals(valueProjection, that.valueProjection)
                && Objects.equals(writableMetadataKeys, that.writableMetadataKeys)
                && Objects.equals(upsertMode, that.upsertMode);
    }

    @Override
    public int hashCode() {
        int result =
                Objects.hash(
                        physicalDataType,
                        keyEncodingFormat,
                        valueEncodingFormat,
                        writableMetadataKeys,
                        upsertMode);
        result = 31 * result + Arrays.hashCode(keyProjection);
        result = 31 * result + Arrays.hashCode(valueProjection);
        return result;
    }
}
