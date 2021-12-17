/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.pulsar.source.reader.deserializer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema.InitializationContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.pulsar.common.schema.PulsarSchema;
import org.apache.flink.util.Collector;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;

import static org.apache.flink.connector.pulsar.common.schema.PulsarSchemaUtils.createTypeInformation;

/**
 * The deserialization schema wrapper for pulsar original {@link Schema}. Pulsar would deserialize
 * the message and pass it to flink with a auto generate or given {@link TypeInformation}.
 *
 * @param <T> The output type of the message.
 */
@Internal
class PulsarSchemaWrapper<T> implements PulsarDeserializationSchema<T> {
    private static final long serialVersionUID = -4864701207257059158L;

    /** The serializable pulsar schema, it wrap the schema with type class. */
    private final PulsarSchema<T> pulsarSchema;

    @SuppressWarnings("java:S2065")
    private transient Schema<T> schema;

    public PulsarSchemaWrapper(PulsarSchema<T> pulsarSchema) {
        this.pulsarSchema = pulsarSchema;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        if (schema == null) {
            this.schema = pulsarSchema.getPulsarSchema();
        }
    }

    @Override
    public void deserialize(Message<byte[]> message, Collector<T> out) throws Exception {
        byte[] bytes = message.getData();
        T instance = schema.decode(bytes);

        out.collect(instance);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        SchemaInfo info = pulsarSchema.getSchemaInfo();
        return createTypeInformation(info);
    }
}
