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
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema.InitializationContext;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;
import org.apache.flink.connector.pulsar.sink.writer.context.PulsarSinkContext;
import org.apache.flink.connector.pulsar.sink.writer.message.PulsarMessage;
import org.apache.flink.connector.pulsar.sink.writer.message.PulsarMessageBuilder;

import org.apache.pulsar.client.api.Schema;

/** Wrap the Flink's SerializationSchema into PulsarSerializationSchema. */
@Internal
public class PulsarSerializationSchemaWrapper<IN> implements PulsarSerializationSchema<IN> {
    private static final long serialVersionUID = 4948155843623161119L;

    private final SerializationSchema<IN> serializationSchema;

    public PulsarSerializationSchemaWrapper(SerializationSchema<IN> serializationSchema) {
        this.serializationSchema = serializationSchema;
    }

    @Override
    public void open(
            InitializationContext initializationContext,
            PulsarSinkContext sinkContext,
            SinkConfiguration sinkConfiguration)
            throws Exception {
        serializationSchema.open(initializationContext);
    }

    @Override
    public PulsarMessage<?> serialize(IN element, PulsarSinkContext sinkContext) {
        PulsarMessageBuilder<byte[]> builder = new PulsarMessageBuilder<>();
        byte[] value = serializationSchema.serialize(element);
        builder.value(Schema.BYTES, value);

        return builder.build();
    }
}
