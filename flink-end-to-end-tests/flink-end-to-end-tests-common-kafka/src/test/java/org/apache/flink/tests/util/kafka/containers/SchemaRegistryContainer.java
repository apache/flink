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

package org.apache.flink.tests.util.kafka.containers;

import org.testcontainers.containers.GenericContainer;

/**
 * A container over an Confluent Schema Registry. It runs the schema registry on port 8082 in the
 * docker network so that it does not overlap with Flink cluster.
 */
public class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> {

    public SchemaRegistryContainer(String version) {
        super("confluentinc/cp-schema-registry:" + version);
        withExposedPorts(8082);
    }

    public SchemaRegistryContainer withKafka(String kafkaBootstrapServer) {
        withEnv(
                "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                "PLAINTEXT://" + kafkaBootstrapServer);
        return this;
    }

    @Override
    protected void configure() {
        withEnv("SCHEMA_REGISTRY_HOST_NAME", getNetworkAliases().get(0));
        withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8082");
    }

    public String getSchemaRegistryUrl() {
        return "http://" + getContainerIpAddress() + ":" + getMappedPort(8082);
    }
}
