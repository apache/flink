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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.connector.hbase.source.HBaseSource;

import java.io.IOException;
import java.io.Serializable;

/**
 * The deserialization interface that needs to be implemented for constructing an {@link
 * HBaseSource}.
 *
 * <p>A minimal implementation can be seen in the following example.
 *
 * <pre>{@code
 * static class HBaseStringDeserializer implements HBaseSourceDeserializer<String> {
 *     @Override
 *     public String deserialize(HBaseSourceEvent event) {
 *         return new String(event.getPayload(), HBaseEvent.DEFAULT_CHARSET);
 *     }
 * }
 * }</pre>
 *
 * {@link #getProducedType()} should be overridden if the type information cannot be extracted this
 * way.
 */
@FunctionalInterface
public interface HBaseSourceDeserializer<T> extends Serializable, ResultTypeQueryable<T> {

    /**
     * This method wraps a {@link DeserializationSchema} in an HBaseSourceDeserializer. It will only
     * have access to the payload of the {@link HBaseSourceEvent}.
     */
    static <T> HBaseSourceDeserializer<T> valueOnly(
            DeserializationSchema<T> deserializationSchema) {
        return new HBaseSourceDeserializerWrapper<>(deserializationSchema);
    }

    T deserialize(HBaseSourceEvent event) throws IOException;

    @Override
    default TypeInformation<T> getProducedType() {
        return TypeExtractor.createTypeInfo(
                HBaseSourceDeserializer.class, getClass(), 0, null, null);
    }
}
