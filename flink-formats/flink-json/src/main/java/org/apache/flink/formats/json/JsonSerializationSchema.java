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

package org.apache.flink.formats.json;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.util.function.SerializableSupplier;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

/** SerializationSchema that serializes an object to a JSON String. */
@PublicEvolving
public class JsonSerializationSchema<T> implements SerializationSchema<T> {

    private static final long serialVersionUID = 1L;

    private final SerializableSupplier<ObjectMapper> mapperFactory;

    protected transient ObjectMapper mapper;

    public JsonSerializationSchema() {
        this(() -> new ObjectMapper());
    }

    public JsonSerializationSchema(SerializableSupplier<ObjectMapper> mapperFactory) {
        this.mapperFactory = mapperFactory;
    }

    @Override
    public void open(InitializationContext context) {
        mapper = mapperFactory.get();
    }

    @Override
    public byte[] serialize(T element) {
        try {
            return mapper.writeValueAsBytes(element);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(
                    String.format("Could not serialize value '%s'.", element), e);
        }
    }
}
