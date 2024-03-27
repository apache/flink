/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.protobuf.registry.confluent.utils;

import org.apache.flink.formats.protobuf.registry.confluent.SchemaRegistryConfig;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

/**
 * A test {@link SchemaRegistryConfig} that passes the given {@link
 * io.confluent.kafka.schemaregistry.client.SchemaRegistryClient}. This lets us connect to a {@link
 * io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry}.
 */
public final class TestSchemaRegistryConfig implements SchemaRegistryConfig {

    private final int schemaId;

    private final SchemaRegistryClient client;

    public TestSchemaRegistryConfig(int schemaId, SchemaRegistryClient client) {
        this.schemaId = schemaId;
        this.client = client;
    }

    @Override
    public int getSchemaId() {
        return schemaId;
    }

    @Override
    public SchemaRegistryClient createClient() {
        return client;
    }
}
