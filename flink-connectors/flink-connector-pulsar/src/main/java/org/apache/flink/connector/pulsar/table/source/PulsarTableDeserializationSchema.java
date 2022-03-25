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
 * <p>Both Flink's key decoding format and value decoding format are wrapped in this class. It * is
 * also responsible for getting metadata fields from a physical pulsar message body, * and the final
 * projection mapping from Pulsar message fields to Flink row.
 *
 * <p>TODO add more description
 */
public class PulsarTableDeserializationSchema implements PulsarDeserializationSchema<RowData> {

    private static final long serialVersionUID = -3298784447432136216L;

    // TODO what does this do ?
    private final TypeInformation<RowData> producedTypeInfo;

    // TODO upsert support can be abstracted to another class

    @Nullable private final DeserializationSchema<RowData> messageKeyDeserialization;

    private final DeserializationSchema<RowData> messageBodyDeserialization;

    private final PulsarProjectProducedRowSupport projectionSupport;

    public PulsarTableDeserializationSchema(
            @Nullable DeserializationSchema<RowData> keyDeserialization,
            DeserializationSchema<RowData> valueDeserialization,
            TypeInformation<RowData> producedTypeInfo,
            PulsarProjectProducedRowSupport projectionSupport) {

        // TODO do we need to deserialization to be thread safe ? I don't think so.
        // TODO rename these variables to use full name
        this.messageKeyDeserialization = keyDeserialization;
        this.messageBodyDeserialization = valueDeserialization;
        this.projectionSupport = projectionSupport;
        this.producedTypeInfo = producedTypeInfo;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        if (messageKeyDeserialization != null) {
            messageKeyDeserialization.open(context);
        }
        messageBodyDeserialization.open(context);
    }

    // TODO here the message schema should always be bytes.
    @Override
    public void deserialize(Message<?> message, Collector<RowData> collector) throws IOException {
        // shortcut in case no output projection is required,
        // also not for a cartesian product with the keys
        // TODO we don't need the shortcut ?
        // always get the value row data
        List<RowData> valueRowData = new ArrayList<>();
        messageBodyDeserialization.deserialize(
                message.getData(), new ListCollector<>(valueRowData));

        // buffer key(s)
        List<RowData> keyRowData = new ArrayList<>();
        // TODO when will this be null ?
        if (messageKeyDeserialization != null) {
            // TODO why it has to be from the keyBytes ?
            messageKeyDeserialization.deserialize(
                    message.getKeyBytes(), new ListCollector<>(keyRowData));
        }

        // TODO keyRowData could be empty
        projectionSupport.projectToProducedRowAndCollect(
                message, keyRowData, valueRowData, collector);
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }

    // TODO should always use bytes schema
    @Override
    public Schema<?> schema() {
        return Schema.BYTES;
    }
}
