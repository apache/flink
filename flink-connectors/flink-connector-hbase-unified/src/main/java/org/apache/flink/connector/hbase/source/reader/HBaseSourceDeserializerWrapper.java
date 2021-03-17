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

package org.apache.flink.connector.hbase.source.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.hbase.source.HBaseSource;

import java.io.IOException;

/**
 * This class wraps a {@link DeserializationSchema} so it can be used in an {@link HBaseSource} as a
 * {@link HBaseSourceDeserializer}.
 *
 * @see HBaseSourceDeserializer#valueOnly(DeserializationSchema) valueOnly(DeserializationSchema).
 */
@Internal
class HBaseSourceDeserializerWrapper<T> implements HBaseSourceDeserializer<T> {
    private final DeserializationSchema<T> deserializationSchema;

    HBaseSourceDeserializerWrapper(DeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public T deserialize(HBaseSourceEvent event) throws IOException {
        return deserializationSchema.deserialize(event.getPayload());
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return deserializationSchema.getProducedType();
    }
}
