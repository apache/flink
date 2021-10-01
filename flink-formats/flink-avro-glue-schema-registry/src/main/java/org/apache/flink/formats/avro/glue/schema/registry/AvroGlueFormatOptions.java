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

package org.apache.flink.formats.avro.glue.schema.registry;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import software.amazon.awssdk.services.glue.model.Compatibility;

import java.time.Duration;

/** Options for AWS Glue Schema Registry Avro format. */
@PublicEvolving
public class AvroGlueFormatOptions {
    public static final ConfigOption<String> AWS_REGION =
            ConfigOptions.key("aws.region")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("AWS region");

    public static final ConfigOption<String> AWS_ENDPOINT =
            ConfigOptions.key("aws.endpoint").stringType().noDefaultValue();

    public static final ConfigOption<Integer> CACHE_SIZE =
            ConfigOptions.key("cache.size")
                    .intType()
                    .defaultValue(200)
                    .withDescription("Cache maximum size in *items*.  Defaults to 200");

    public static final ConfigOption<Long> CACHE_TTL_MS =
            ConfigOptions.key("cache.ttlMs")
                    .longType()
                    .defaultValue(Duration.ofDays(1L).toMillis())
                    .withDescription("Cache TTL in milliseconds.  Defaults to 1 day");

    public static final ConfigOption<String> REGISTRY_NAME =
            ConfigOptions.key("registry.name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Registry name");

    public static final ConfigOption<Boolean> SCHEMA_AUTO_REGISTRATION =
            ConfigOptions.key("schema.autoRegistration")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether auto-registration is enabled.  Defaults to true.");

    public static final ConfigOption<Compatibility> SCHEMA_COMPATIBILITY =
            ConfigOptions.key("schema.compatibility")
                    .enumType(Compatibility.class)
                    .defaultValue(AWSSchemaRegistryConstants.DEFAULT_COMPATIBILITY_SETTING);

    public static final ConfigOption<AWSSchemaRegistryConstants.COMPRESSION> SCHEMA_COMPRESSION =
            ConfigOptions.key("schema.compression")
                    .enumType(AWSSchemaRegistryConstants.COMPRESSION.class)
                    .defaultValue(AWSSchemaRegistryConstants.COMPRESSION.NONE)
                    .withDescription("Compression type");

    public static final ConfigOption<String> SCHEMA_NAME =
            ConfigOptions.key("schema.name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The Schema name under which to register the schema used by this format during serialization.");

    public static final ConfigOption<AvroRecordType> SCHEMA_TYPE =
            ConfigOptions.key("schema.type")
                    .enumType(AvroRecordType.class)
                    .defaultValue(AvroRecordType.GENERIC_RECORD)
                    .withDescription("Record type");

    private AvroGlueFormatOptions() {}
}
