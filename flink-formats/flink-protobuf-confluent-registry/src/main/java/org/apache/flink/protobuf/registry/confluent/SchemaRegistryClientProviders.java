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

package org.apache.flink.protobuf.registry.confluent;

import org.apache.flink.annotation.VisibleForTesting;

import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Providers for {@link SchemaRegistryClient}. */
public class SchemaRegistryClientProviders {
    public static class CachedSchemaRegistryClientProvider implements SchemaRegistryClientProvider {

        private final String url;
        private final int identityMapCapacity;
        private final Map<String, String> properties;

        public CachedSchemaRegistryClientProvider(
                String url, int identityMapCapacity, Map<String, String> properties) {
            this.url = url;
            this.identityMapCapacity = identityMapCapacity;
            this.properties = properties;
        }

        @Override
        public SchemaRegistryClient createSchemaRegistryClient() {
            List<SchemaProvider> providers = new ArrayList<>();
            providers.add(new ProtobufSchemaProvider());
            return new CachedSchemaRegistryClient(url, identityMapCapacity, providers, properties);
        }
    }

    @VisibleForTesting
    public static class MockSchemaRegistryClientProvider implements SchemaRegistryClientProvider {
        private final MockSchemaRegistryClient client;

        public MockSchemaRegistryClientProvider(MockSchemaRegistryClient client) {
            this.client = client;
        }

        @Override
        public SchemaRegistryClient createSchemaRegistryClient() {
            return client;
        }
    }
}
