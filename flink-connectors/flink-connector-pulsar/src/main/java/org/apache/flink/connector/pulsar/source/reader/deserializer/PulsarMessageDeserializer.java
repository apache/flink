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

import org.apache.pulsar.client.api.Message;

import java.io.Serializable;

/**
 * The message deserializer for extracting the desired instance from <code>Message<M></code>.
 *
 * @param <M> The type of the class in pulsar message's {@link Message#getValue()}.
 * @param <T> The type of the class for sinking to flink downstream operator.
 */
@PublicEvolving
@FunctionalInterface
public interface PulsarMessageDeserializer<M, T> extends Serializable {

    /**
     * Convert the message to a sequence of results, the size could zero, one or many.
     *
     * @param message The message decoded from pulsar client.
     */
    Iterable<T> deserialize(Message<M> message) throws Exception;
}
