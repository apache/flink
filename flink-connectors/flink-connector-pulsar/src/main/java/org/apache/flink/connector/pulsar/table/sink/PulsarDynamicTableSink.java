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

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.pulsar.sink.PulsarSink;
import org.apache.flink.connector.pulsar.sink.PulsarSinkBuilder;
import org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchema;
import org.apache.flink.connector.pulsar.table.sink.impl.PulsarSerializationSchemaFactory;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

/** pulsar dynamic table sink. */
public class PulsarDynamicTableSink implements DynamicTableSink, SupportsWritingMetadata {

    // --------------------------------------------------------------------------------------------
    // Format attributes
    // --------------------------------------------------------------------------------------------

    /** Data type to configure the formats. */
    protected final DataType physicalDataType;

    protected final PulsarSerializationSchemaFactory serializationSchemaFactory;

    protected final ChangelogMode changelogMode;

    // TODO

    protected final List<String> topics;

    /** Parallelism of the physical Pulsar producer. * */
    protected final @Nullable Integer parallelism;

    /** Sink commit semantic. */
    protected final DeliveryGuarantee deliveryGuarantee;

    /** Properties for the pulsar producer. */
    protected final Properties properties;

    public PulsarDynamicTableSink(
            DataType physicalDataType,
            PulsarSerializationSchemaFactory serializationSchemaFactory,
            ChangelogMode changelogMode,
            List<String> topics,
            Properties properties,
            DeliveryGuarantee deliveryGuarantee,
            @Nullable Integer parallelism) {
        this.physicalDataType =
                Preconditions.checkNotNull(
                        physicalDataType, "Consumed data type must not be null.");
        this.serializationSchemaFactory = serializationSchemaFactory;
        this.changelogMode = changelogMode;
        this.topics = topics;
        // Mutable attributes
        this.properties = Preconditions.checkNotNull(properties, "Properties must not be null.");
        this.deliveryGuarantee =
                Preconditions.checkNotNull(deliveryGuarantee, "Semantic must not be null.");
        this.parallelism = parallelism;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return this.changelogMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {

        final PulsarSerializationSchema<RowData> pulsarSerializationSchema =
                serializationSchemaFactory.createPulsarSerializationSchema(context);

        final PulsarSinkBuilder<RowData> pulsarSinkBuilder = PulsarSink.builder();
        pulsarSinkBuilder.setSerializationSchema(pulsarSerializationSchema);
        pulsarSinkBuilder.setProperties(properties);
        pulsarSinkBuilder.setDeliveryGuarantee(deliveryGuarantee);
        pulsarSinkBuilder.setTopics(topics);

        return SinkV2Provider.of(pulsarSinkBuilder.build());
    }

    @Override
    public DynamicTableSink copy() {
        // TODO
        return null;
    }

    @Override
    public boolean equals(Object o) {
        // TODO
        return false;
    }

    @Override
    public int hashCode() {
        // TODO
        return 0;
    }

    @Override
    public String asSummaryString() {
        return "Pulsar dynamic table sink";
    }

    @Override
    public Map<String, DataType> listWritableMetadata() {
        final Map<String, DataType> metadataMap = new LinkedHashMap<>();
        Stream.of(WritableMetadata.values())
                .forEachOrdered(m -> metadataMap.put(m.key, m.dataType));
        return metadataMap;
    }

    @Override
    public void applyWritableMetadata(List<String> metadataKeys, DataType consumedDataType) {
        this.serializationSchemaFactory.setWritableMetadataKeys(metadataKeys);
    }

    enum WritableMetadata {
        PROPERTIES(
                "properties",
                // key and value of the map are nullable to make handling easier in queries
                DataTypes.MAP(DataTypes.STRING().nullable(), DataTypes.STRING().nullable())
                        .nullable(),
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
                }),

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

        WritableMetadata(
                String key,
                DataType dataType,
                PulsarDynamicTableSerializationSchema.MetadataConverter converter) {
            this.key = key;
            this.dataType = dataType;
            this.converter = converter;
        }
    }
}
