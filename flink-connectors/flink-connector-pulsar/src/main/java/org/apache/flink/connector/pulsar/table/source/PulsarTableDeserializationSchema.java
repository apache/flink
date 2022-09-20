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

import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.pulsar.source.config.SourceConfiguration;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import org.apache.pulsar.client.api.Message;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A specific {@link PulsarDeserializationSchema} for {@link PulsarTableSource}.
 *
 * <p>Both Flink's key decoding format and value decoding format are wrapped in this class. It is
 * responsible for getting metadata fields from a physical pulsar message body, and the final
 * projection mapping from Pulsar message fields to Flink row.
 *
 * <p>After retrieving key and value bytes and convert them into a list of {@link RowData}, it then
 * delegates metadata appending, key and value {@link RowData} combining to a {@link
 * PulsarRowDataConverter} instance.
 */
public class PulsarTableDeserializationSchema implements PulsarDeserializationSchema<RowData> {
    private static final long serialVersionUID = -3298784447432136216L;

    private final TypeInformation<RowData> producedTypeInfo;

    @Nullable private final DeserializationSchema<RowData> keyDeserialization;

    private final DeserializationSchema<RowData> valueDeserialization;

    private final PulsarRowDataConverter rowDataConverter;

    private final boolean upsertMode;

    public PulsarTableDeserializationSchema(
            @Nullable DeserializationSchema<RowData> keyDeserialization,
            DeserializationSchema<RowData> valueDeserialization,
            TypeInformation<RowData> producedTypeInfo,
            PulsarRowDataConverter rowDataConverter,
            boolean upsertMode) {
        this.keyDeserialization = keyDeserialization;
        this.valueDeserialization = checkNotNull(valueDeserialization);
        this.rowDataConverter = checkNotNull(rowDataConverter);
        this.producedTypeInfo = checkNotNull(producedTypeInfo);
        this.upsertMode = upsertMode;
    }

    @Override
    public void open(
            DeserializationSchema.InitializationContext context, SourceConfiguration configuration)
            throws Exception {
        if (keyDeserialization != null) {
            keyDeserialization.open(context);
        }
        valueDeserialization.open(context);
    }

    @Override
    public void deserialize(Message<byte[]> message, Collector<RowData> collector)
            throws IOException {

        // Get the key row data
        List<RowData> keyRowData = new ArrayList<>();
        if (keyDeserialization != null) {
            keyDeserialization.deserialize(message.getKeyBytes(), new ListCollector<>(keyRowData));
        }

        // Get the value row data
        List<RowData> valueRowData = new ArrayList<>();

        if (upsertMode && message.getData().length == 0) {
            checkNotNull(keyDeserialization, "upsert mode must specify a key format");
            rowDataConverter.projectToRowWithNullValueRow(message, keyRowData, collector);
            return;
        }

        valueDeserialization.deserialize(message.getData(), new ListCollector<>(valueRowData));

        rowDataConverter.projectToProducedRowAndCollect(
                message, keyRowData, valueRowData, collector);
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }
}
