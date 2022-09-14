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

package org.apache.flink.streaming.connectors.kafka.internals;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

/**
 * A simple wrapper for using the SerializationSchema with the KeyedSerializationSchema interface.
 *
 * @param <T> The type to serialize
 */
@Internal
public class KeyedSerializationSchemaWrapper<T> implements KeyedSerializationSchema<T> {

    private static final long serialVersionUID = 1351665280744549933L;

    private final SerializationSchema<T> serializationSchema;

    public KeyedSerializationSchemaWrapper(SerializationSchema<T> serializationSchema) {
        this.serializationSchema = serializationSchema;
    }

    public SerializationSchema<T> getSerializationSchema() {
        return serializationSchema;
    }

    @Override
    public byte[] serializeKey(T element) {
        return null;
    }

    @Override
    public byte[] serializeValue(T element) {
        return serializationSchema.serialize(element);
    }

    @Override
    public String getTargetTopic(T element) {
        return null; // we are never overriding the topic
    }
}
