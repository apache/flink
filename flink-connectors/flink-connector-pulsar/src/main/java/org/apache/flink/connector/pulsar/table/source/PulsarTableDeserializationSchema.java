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
import org.apache.flink.connector.pulsar.table.source.impl.PulsarProjectProducedRowSupport;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A specific {@link PulsarDeserializationSchema} for {@link PulsarTableSource}.
 *
 * <p>Both Flink's key decoding format and value decoding format are wrapped in this class. It is
 * responsible for getting metadata fields from a physical pulsar message body, and the final
 * projection mapping from Pulsar message fields to Flink row.
 *
 * <p>After retrieving key and value bytes and convert them into a list of {@link RowData}, it then
 * delegates metadata appending, key and value {@link RowData} combining to a {@link
 * PulsarProjectProducedRowSupport} instance.
 */
public class PulsarTableDeserializationSchema implements PulsarDeserializationSchema<RowData> {

    private static final long serialVersionUID = -3298784447432136216L;

    private final TypeInformation<RowData> producedTypeInfo;

    @Nullable private final DeserializationSchema<RowData> keyDeserialization;

    private final DeserializationSchema<RowData> valueDeserialization;

    private final PulsarProjectProducedRowSupport projectionSupport;

    public PulsarTableDeserializationSchema(
            @Nullable DeserializationSchema<RowData> keyDeserialization,
            DeserializationSchema<RowData> valueDeserialization,
            TypeInformation<RowData> producedTypeInfo,
            PulsarProjectProducedRowSupport projectionSupport) {
        this.keyDeserialization = keyDeserialization;
        this.valueDeserialization = valueDeserialization;
        this.projectionSupport = projectionSupport;
        this.producedTypeInfo = producedTypeInfo;
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
    public void deserialize(Message<?> message, Collector<RowData> collector) throws IOException {
        // Get the value row data
        List<RowData> valueRowData = new ArrayList<>();
        valueDeserialization.deserialize(message.getData(), new ListCollector<>(valueRowData));

        // Get the key row data
        List<RowData> keyRowData = new ArrayList<>();
        if (keyDeserialization != null) {
            keyDeserialization.deserialize(message.getKeyBytes(), new ListCollector<>(keyRowData));
        }

        projectionSupport.projectToProducedRowAndCollect(
                message, keyRowData, valueRowData, collector);
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }

    @Override
    public Schema<?> schema() {
        return Schema.BYTES;
    }
}
