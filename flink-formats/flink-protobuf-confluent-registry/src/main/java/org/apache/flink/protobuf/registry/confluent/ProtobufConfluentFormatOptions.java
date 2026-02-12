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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.Map;

/** Options for Confluent protobuf format. */
@PublicEvolving
public class ProtobufConfluentFormatOptions {

    public static final ConfigOption<String> URL =
            ConfigOptions.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withFallbackKeys("schema-registry.url")
                    .withDescription(
                            "The URL of the Confluent Schema Registry to fetch/register schemas.");

    public static final ConfigOption<Integer> REGISTRY_CLIENT_CACHE_CAPACITY =
            ConfigOptions.key("schema-registry.client.cache.capacity")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "The number of schemas to cache in the Confluent Schema Registry client.");

    public static final ConfigOption<Boolean> USE_DEFAULT_PROTO_INCLUDES =
            ConfigOptions.key("use-default-proto-includes")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to include the following set of proto descriptors when calling protoc to generate the Java classes that are used for (de)-serialization: "
                                    + "confluent/meta.proto, confluent/types/decimal.proto, "
                                    + "/google/protobuf/any.proto, /google/protobuf/api.proto, "
                                    + "/google/protobuf/descriptor.proto, /google/protobuf/duration.proto, "
                                    + "/google/protobuf/empty.proto, /google/protobuf/field_mask.proto, "
                                    + "/google/protobuf/source_context.proto, /google/protobuf/struct.proto, "
                                    + "/google/protobuf/timestamp.proto, /google/protobuf/type.proto, "
                                    + "/google/protobuf/wrappers.proto");

    public static final ConfigOption<String> CUSTOM_PROTO_INCLUDES =
            ConfigOptions.key("custom-proto-includes")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "A comma-separated list of Java resource URLs that should be included when calling protoc to generate the Java classes that are used for (de)-serialization.");

    // --------------------------------------------------------------------------------------------
    // Serialization options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<String> SUBJECT =
            ConfigOptions.key("serializer.subject")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The Confluent Schema Registry subject under which to register the "
                                    + "schema used by this format during serialization.");

    public static final ConfigOption<String> MESSAGE_NAME =
            ConfigOptions.key("serializer.generated-message-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The name of the protobuf message type that will be generated at "
                                    + "runtime, and used during serialization.");

    public static final ConfigOption<String> PACKAGE_NAME =
            ConfigOptions.key("serializer.generated-package-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The package name of the protobuf descriptor that will be generated at "
                                    + "runtime, and used during serialization.");

    public static final ConfigOption<String> WRITE_NULL_STRING_LITERAL =
            ConfigOptions.key("serializer.write-null-string-literal")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "When serializing to protobuf data, this is the optional config to specify the string literal in protobuf's array/map in case of null values."
                                    + "By default empty string is used.");

    // --------------------------------------------------------------------------------------------
    // De-serialization options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS =
            ConfigOptions.key("deserializer.ignore-parse-errors")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional read flag to skip rows with parse errors instead of failing; false by default.");

    public static final ConfigOption<Boolean> READ_DEFAULT_VALUES =
            ConfigOptions.key("deserializer.read-default-values")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional flag to read as default values instead of null when some field does not exist in deserialization; default to false."
                                    + "If proto syntax is proto3, users need to set this to true when using protobuf versions lower than 3.15 as older versions "
                                    + "do not support checking for field presence which can cause runtime compilation issues. Additionally, primitive types "
                                    + "will be set to default values instead of null as field presence cannot be checked for them. Please be aware that setting this"
                                    + " to true will cause the deserialization performance to be much slower depending on schema complexity and message size");

    // --------------------------------------------------------------------------------------------
    // Commonly used options maintained by Flink for convenience
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<String> SSL_KEYSTORE_LOCATION =
            ConfigOptions.key("ssl.keystore.location")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Location / File of SSL keystore");

    public static final ConfigOption<String> SSL_KEYSTORE_PASSWORD =
            ConfigOptions.key("ssl.keystore.password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Password for SSL keystore");

    public static final ConfigOption<String> SSL_TRUSTSTORE_LOCATION =
            ConfigOptions.key("ssl.truststore.location")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Location / File of SSL truststore");

    public static final ConfigOption<String> SSL_TRUSTSTORE_PASSWORD =
            ConfigOptions.key("ssl.truststore.password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Password for SSL truststore");

    public static final ConfigOption<String> BASIC_AUTH_CREDENTIALS_SOURCE =
            ConfigOptions.key("basic-auth.credentials-source")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Basic auth credentials source for Schema Registry");

    public static final ConfigOption<String> BASIC_AUTH_USER_INFO =
            ConfigOptions.key("basic-auth.user-info")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Basic auth user info for schema registry");

    public static final ConfigOption<String> BEARER_AUTH_CREDENTIALS_SOURCE =
            ConfigOptions.key("bearer-auth.credentials-source")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Bearer auth credentials source for Schema Registry");

    public static final ConfigOption<String> BEARER_AUTH_TOKEN =
            ConfigOptions.key("bearer-auth.token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Bearer auth token for Schema Registry");

    // --------------------------------------------------------------------------------------------
    // Fallback properties
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<Map<String, String>> PROPERTIES =
            ConfigOptions.key("properties")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "Properties map that is forwarded to the underlying Schema Registry. "
                                    + "This is useful for options that are not officially exposed "
                                    + "via Flink config options. However, note that Flink options "
                                    + "have higher precedence.");

    private ProtobufConfluentFormatOptions() {}
}
