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

package org.apache.flink.streaming.util.serialization;

import org.apache.flink.annotation.Public;

import java.io.Serializable;

/**
 * The serialization schema describes how to turn a data object into a different serialized
 * representation. Most data sinks (for example Apache Kafka) require the data to be handed to them
 * in a specific format (for example as byte strings).
 *
 * @param <T> The type to be serialized.
 * @deprecated Use {@link org.apache.flink.api.common.serialization.SerializationSchema} instead.
 */
@Public
@Deprecated
public interface SerializationSchema<T>
        extends org.apache.flink.api.common.serialization.SerializationSchema<T>, Serializable {

    /**
     * Serializes the incoming element to a specified type.
     *
     * @param element The incoming element to be serialized
     * @return The serialized element.
     */
    @Override
    byte[] serialize(T element);
}
