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

package org.apache.flink.connector.pulsar.source.reader.deserializer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;

/**
 * The base deserialization schema implementation, user can implement their own {@code
 * PulsarDeserializationSchema} by extends this class. Or you can just given custom {@link
 * PulsarSchemaFactory}, {@link PulsarMessageDeserializer} and {@link TypeInformation}.
 *
 * @param <M> The message type for pulsar.
 * @param <T> The result type (source type) for flink.
 */
@PublicEvolving
public class PulsarDeserializationSchemaBase<M, T> implements PulsarDeserializationSchema<M, T> {
    private static final long serialVersionUID = 7278149192690933421L;

    private final PulsarSchemaFactory<M> schemaFactory;

    private final PulsarMessageDeserializer<M, T> messageDeserializer;

    private final TypeInformation<T> typeInformation;

    public PulsarDeserializationSchemaBase(
            PulsarSchemaFactory<M> schemaFactory,
            PulsarMessageDeserializer<M, T> pulsarDeserializer,
            TypeInformation<T> typeInformation) {
        this.schemaFactory = schemaFactory;
        this.messageDeserializer = pulsarDeserializer;
        this.typeInformation = typeInformation;
    }

    @Override
    public void deserialize(Message<M> message, Collector<T> out) throws Exception {
        Iterable<T> iterable = messageDeserializer.deserialize(message);
        // This null asserting is required for avoiding NP.
        if (iterable != null) {
            iterable.forEach(out::collect);
        }
    }

    @Override
    public Schema<M> createPulsarSchema() {
        return schemaFactory.create();
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return typeInformation;
    }
}
