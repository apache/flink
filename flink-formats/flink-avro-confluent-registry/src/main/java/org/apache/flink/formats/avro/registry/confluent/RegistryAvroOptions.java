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

package org.apache.flink.formats.avro.registry.confluent;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Options for Schema Registry Avro format. */
public class RegistryAvroOptions {
    private RegistryAvroOptions() {}

    public static final ConfigOption<String> SCHEMA_REGISTRY_URL =
            ConfigOptions.key("schema-registry.url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Required URL to connect to schema registry service");

    public static final ConfigOption<String> SCHEMA_REGISTRY_SUBJECT =
            ConfigOptions.key("schema-registry.subject")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Subject name to write to the Schema Registry service, required for sink");
}
