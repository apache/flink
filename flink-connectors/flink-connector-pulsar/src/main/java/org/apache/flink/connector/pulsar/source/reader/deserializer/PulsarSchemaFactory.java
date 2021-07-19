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

import org.apache.pulsar.client.api.Schema;

import java.io.Serializable;

/**
 * Since pulsar's default schema implementation is not serializable, we introduce the interface for
 * allowing user defining a lambda based schema factory.
 *
 * <p>The following example shows a lambda based schema factory which returns a <code>Schema.STRING
 * </code>
 *
 * <pre>{@code () -> Schema.STRING}</pre>
 *
 * @param <T> The message type.
 */
@PublicEvolving
@FunctionalInterface
public interface PulsarSchemaFactory<T> extends Serializable {

    /** Return a schema instance, this instance could be reused since its thread-safe. */
    Schema<T> create();
}
