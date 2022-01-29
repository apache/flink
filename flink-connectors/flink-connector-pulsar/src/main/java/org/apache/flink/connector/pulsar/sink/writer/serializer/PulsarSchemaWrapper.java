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

package org.apache.flink.connector.pulsar.sink.writer.serializer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.pulsar.common.schema.PulsarSchema;
import org.apache.flink.connector.pulsar.sink.writer.context.PulsarSinkContext;
import org.apache.flink.connector.pulsar.sink.writer.message.RawMessage;

import org.apache.pulsar.client.api.Schema;

/** Wrap the Pulsar's Schema into PulsarSerializationSchema. */
@Internal
public class PulsarSchemaWrapper<IN> implements PulsarSerializationSchema<IN> {
    private static final long serialVersionUID = -2567052498398184194L;

    private static final byte[] EMPTY_BYTES = new byte[0];
    private final PulsarSchema<IN> pulsarSchema;

    public PulsarSchemaWrapper(PulsarSchema<IN> pulsarSchema) {
        this.pulsarSchema = pulsarSchema;
    }

    @Override
    public RawMessage<byte[]> serialize(IN element, PulsarSinkContext sinkContext) {
        RawMessage<byte[]> message;

        if (sinkContext.isEnableSchemaEvolution()) {
            // We don't need to serialize incoming records in schema evolution.
            message = new RawMessage<>(EMPTY_BYTES);
        } else {
            Schema<IN> schema = this.pulsarSchema.getPulsarSchema();
            byte[] bytes = schema.encode(element);
            message = new RawMessage<>(bytes);
        }

        Long eventTime = sinkContext.timestamp();
        if (eventTime != null) {
            message.setEventTime(eventTime);
        }

        return message;
    }

    @Override
    public Schema<IN> schema() {
        return pulsarSchema.getPulsarSchema();
    }
}
