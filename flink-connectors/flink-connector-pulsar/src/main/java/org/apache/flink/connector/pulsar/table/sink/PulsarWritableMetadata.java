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

import org.apache.flink.connector.pulsar.sink.writer.message.PulsarMessageBuilder;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import org.bouncycastle.util.Arrays;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

/** A class used to manage metadata that is non virtual for Pulsar SQL sink connector. */
public class PulsarWritableMetadata implements Serializable {

    private static final long serialVersionUID = 8117156158379846715L;

    private final List<String> writableMetadataKeys;

    private final int physicalChildrenSize;

    /**
     * Contains the position for each value of {@link WritableMetadata} in the consumed row or -1 if
     * this metadata key is not used.
     */
    private int[] metadataPositions;

    public PulsarWritableMetadata(List<String> writableMetadataKeys, int physicalChildrenSize) {
        this.writableMetadataKeys = writableMetadataKeys;
        this.physicalChildrenSize = physicalChildrenSize;
        this.metadataPositions = getMetadataPositions();
    }

    public void applyWritableMetadataInMessage(
            RowData consumedRow, PulsarMessageBuilder<byte[]> messageBuilder) {
        Map<String, String> properties = readMetadata(consumedRow, WritableMetadata.PROPERTIES);
        if (properties != null) {
            messageBuilder.properties(properties);
        }
        final Long eventTime = readMetadata(consumedRow, WritableMetadata.EVENT_TIME);
        if (eventTime != null && eventTime > 0) {
            messageBuilder.eventTime(eventTime);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T readMetadata(RowData consumedRow, WritableMetadata metadata) {
        if (Arrays.isNullOrEmpty(metadataPositions)) {
            return null;
        }
        final int pos = metadataPositions[metadata.ordinal()];
        if (pos < 0) {
            return null;
        }
        return (T) metadata.converter.read(consumedRow, pos);
    }

    private int[] getMetadataPositions() {
        return Stream.of(WritableMetadata.values())
                .mapToInt(
                        m -> {
                            final int pos = writableMetadataKeys.indexOf(m.key);
                            if (pos < 0) {
                                return -1;
                            }
                            return physicalChildrenSize + pos;
                        })
                .toArray();
    }

    /** A list of writable metadata used by Pulsar SQL sink connector. */
    public enum WritableMetadata {
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
                "event_time",
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).nullable(),
                (row, pos) -> {
                    if (row.isNullAt(pos)) {
                        return null;
                    }
                    return row.getTimestamp(pos, 3).getMillisecond();
                });
        public final String key;

        public final DataType dataType;

        public final PulsarTableSerializationSchema.MetadataConverter converter;

        WritableMetadata(
                String key,
                DataType dataType,
                PulsarTableSerializationSchema.MetadataConverter converter) {
            this.key = key;
            this.dataType = dataType;
            this.converter = converter;
        }
    }
}
