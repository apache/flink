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

import java.time.Duration;

import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants.COMPRESSION;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import software.amazon.awssdk.services.glue.model.Compatibility;

public class GlueSchemaRegistryAvroOptions {
    private GlueSchemaRegistryAvroOptions() {
    }
    public static final String PREFIX = "schema-registry.";

    public static final ConfigOption<String> AWS_REGION = ConfigOptions.key(PREFIX + AWSSchemaRegistryConstants.AWS_REGION)
            .stringType().noDefaultValue().withDescription("AWS region");

    public static final ConfigOption<String> REGISTRY_NAME = ConfigOptions.key(PREFIX + AWSSchemaRegistryConstants.REGISTRY_NAME)
            .stringType().noDefaultValue().withDescription("Registry name");

    public static final ConfigOption<String> RECORD_TYPE = ConfigOptions
            .key(PREFIX + AWSSchemaRegistryConstants.AVRO_RECORD_TYPE).stringType()
            .defaultValue(AvroRecordType.GENERIC_RECORD.getName()).withDescription("Record type");

    public static final ConfigOption<String> SCHEMA_REGISTRY_SUBJECT = ConfigOptions.key(PREFIX + "subject").stringType()
            .noDefaultValue().withDescription(
                    "The Schema name under which to register the schema used by this format during serialization.");

    public static final ConfigOption<COMPRESSION> COMPRESSION_TYPE = ConfigOptions
            .key(PREFIX + AWSSchemaRegistryConstants.COMPRESSION_TYPE).enumType(COMPRESSION.class).defaultValue(COMPRESSION.NONE)
            .withDescription("Compression type");

    public static final ConfigOption<String> ENDPOINT = ConfigOptions.key(PREFIX + AWSSchemaRegistryConstants.AWS_ENDPOINT)
            .stringType().noDefaultValue();

    public static final ConfigOption<Compatibility> COMPATIBILITY = ConfigOptions
            .key(PREFIX + AWSSchemaRegistryConstants.COMPATIBILITY_SETTING).enumType(Compatibility.class)
            .defaultValue(AWSSchemaRegistryConstants.DEFAULT_COMPATIBILITY_SETTING);

    public static final ConfigOption<Boolean> AUTO_REGISTRATION = ConfigOptions
            .key(PREFIX + AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING).booleanType().defaultValue(true)
            .withDescription("Whether auto-registration is enabled.  Defaults to true.");

    public static final ConfigOption<Integer> CACHE_SIZE = ConfigOptions
            .key(PREFIX + AWSSchemaRegistryConstants.CACHE_SIZE).intType().defaultValue(200)
            .withDescription("Cache maximum size in *items*.  Defaults to 200");

    public static final ConfigOption<Long> CACHE_TTL_MS = ConfigOptions
            .key(PREFIX + AWSSchemaRegistryConstants.CACHE_TIME_TO_LIVE_MILLIS).longType()
            .defaultValue(Duration.ofDays(1l).toMillis())
            .withDescription("Cache TTL in milliseconds.  Defaults to 1 day");
}
