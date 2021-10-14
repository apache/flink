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

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TemporaryClassLoaderContext;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

class KafkaSerializerWrapper<IN> implements SerializationSchema<IN> {
    private final Class<? extends Serializer<? super IN>> serializerClass;
    private final Map<String, String> config;
    private final Function<? super IN, String> topicSelector;

    private transient Serializer<? super IN> serializer;

    KafkaSerializerWrapper(
            Class<? extends Serializer<? super IN>> serializerClass,
            Map<String, String> config,
            Function<? super IN, String> topicSelector) {
        this.serializerClass = checkNotNull(serializerClass);
        this.config = checkNotNull(config);
        this.topicSelector = checkNotNull(topicSelector);
    }

    KafkaSerializerWrapper(
            Class<? extends Serializer<? super IN>> serializerClass,
            Function<? super IN, String> topicSelector) {
        this(serializerClass, Collections.emptyMap(), topicSelector);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void open(InitializationContext context) throws Exception {
        final ClassLoader userCodeClassLoader = context.getUserCodeClassLoader().asClassLoader();
        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(userCodeClassLoader)) {
            serializer =
                    InstantiationUtil.instantiate(
                            serializerClass.getName(),
                            Serializer.class,
                            getClass().getClassLoader());
            if (serializer instanceof Configurable) {
                ((Configurable) serializer).configure(config);
            }
        } catch (Exception e) {
            throw new IOException("Failed to instantiate the serializer of class " + serializer, e);
        }
    }

    @Override
    public byte[] serialize(IN element) {
        checkState(serializer != null, "Call open() once before trying to serialize elements.");
        return serializer.serialize(topicSelector.apply(element), element);
    }
}
