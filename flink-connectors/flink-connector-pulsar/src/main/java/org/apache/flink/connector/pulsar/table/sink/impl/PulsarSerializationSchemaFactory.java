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
package org.apache.flink.connector.pulsar.table.sink.impl;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchema;
import org.apache.flink.connector.pulsar.table.sink.PulsarDynamicTableSerializationSchema;
import org.apache.flink.connector.pulsar.table.source.impl.PulsarAppendMetadataSupport;
import org.apache.flink.connector.pulsar.table.source.impl.PulsarUpsertSupport;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.DataTypeUtils;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

public class PulsarSerializationSchemaFactory {

    protected final DataType physicalDataType;

    protected final EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat;

    protected final int[] keyProjection;

    protected final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat;

    protected final int[] valueProjection;

    /** Metadata that is appended at the end of a physical sink row. */
    protected List<String> writableMetadataKeys;

    public PulsarSerializationSchemaFactory(
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

//        final PulsarUpsertSupport upsertSupport = new PulsarUpsertSupport(false);
//        final PulsarAppendMetadataSupport appendMetadataSupport =
//                new PulsarAppendMetadataSupport(writableMetadataKeys);

        // TODO check Kafka implementation
        final List<LogicalType> physicalChildren = physicalDataType.getLogicalType().getChildren();

        final RowData.FieldGetter[] keyFieldGetters =
                getFieldGetters(physicalChildren, keyProjection);
        final RowData.FieldGetter[] valueFieldGetters =
                getFieldGetters(physicalChildren, valueProjection);

        // determine the positions of metadata in the consumed row
        // TODO abstract this to a different
        final int[] metadataPositions =
                Stream.of(WritableMetadata.values())
                        .mapToInt(
                                m -> {
                                    final int pos = writableMetadataKeys.indexOf(m.key);
                                    if (pos < 0) {
                                        return -1;
                                    }
                                    return physicalChildren.size() + pos;
                                })
                        .toArray();

        // check if metadata is used at all
        final boolean hasMetadata = writableMetadataKeys.size() > 0;

        // TODO retrieve message delay configuration
        final long delayMilliseconds = 0;

        return new PulsarDynamicTableSerializationSchema(
                keySerialization,
                keyFieldGetters,
                valueSerialization,
                valueFieldGetters,
                metadataPositions,
                DataTypeUtils.projectRow(physicalDataType, valueProjection),
                delayMilliseconds);
    }

    private @Nullable SerializationSchema<RowData> createSerialization(
            DynamicTableSink.Context context,
            @Nullable EncodingFormat<SerializationSchema<RowData>> format,
            int[] projection,
            @Nullable String prefix) {
        if (format == null) {
            return null;
        }
        DataType physicalFormatDataType =
                DataTypeUtils.projectRow(this.physicalDataType, projection);
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


    // TODO put here for now, need to exam the detail logic
    enum WritableMetadata {

        PROPERTIES(
                "properties",
                // key and value of the map are nullable to make handling easier in queries
                DataTypes.MAP(DataTypes.STRING().nullable(), DataTypes.STRING().nullable()).nullable(),
                (row, pos) -> {
                    if (row.isNullAt(pos)) {
                        return null;
                    }
                    final MapData map = row.getMap(pos);
                    final ArrayData keyArray = map.keyArray();
                    final ArrayData valueArray = map.valueArray();

                    final Properties properties = new Properties();
                    for (int i = 0; i < keyArray.size(); i++) {
                        if (!keyArray.isNullAt(i) && !valueArray.isNullAt(i)) {
                            final String key = keyArray.getString(i).toString();
                            final String value = valueArray.getString(i).toString();
                            properties.put(key, value);
                        }
                    }
                    return properties;
                }
        ),

        EVENT_TIME(
                "eventTime",
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).nullable(),
                (row, pos) -> {
                    if (row.isNullAt(pos)) {
                        return null;
                    }
                    return row.getTimestamp(pos, 3).getMillisecond();
                });
        final String key;

        final DataType dataType;

        final PulsarDynamicTableSerializationSchema.MetadataConverter converter;

        WritableMetadata(String key, DataType dataType,
                         PulsarDynamicTableSerializationSchema.MetadataConverter converter) {
            this.key = key;
            this.dataType = dataType;
            this.converter = converter;
        }
    }
}
