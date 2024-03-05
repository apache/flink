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

package org.apache.flink.formats.avro.registry.apicurio;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import io.apicurio.registry.serde.SerdeConfig;

import java.util.Map;

/** Options for Schema Registry Avro format. */
@PublicEvolving
public class AvroApicurioFormatOptions {

    public static final ConfigOption<String> URL =
            ConfigOptions.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withFallbackKeys("schema-registry.url")
                    .withDescription(
                            "The URL of the Apicurio Schema Registry to fetch/register schemas.");

    public static final ConfigOption<Boolean> ENABLE_HEADERS =
            ConfigOptions.key(SerdeConfig.ENABLE_HEADERS)
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional flag to indicate that the artifact identifier is in the headers rather than the payload;\n"
                                    + "false by default.");

    public static final ConfigOption<String> HEADERS_HANDLER =
            ConfigOptions.key(SerdeConfig.HEADERS_HANDLER)
                    .stringType()
                    .defaultValue(SerdeConfig.HEADERS_HANDLER_DEFAULT)
                    .withDescription(
                            "Used by serializers and deserializers. Fully-qualified Java classname that implements\n"
                                    + "HeadersHandler and writes/reads the artifact identifier to/from the Kafka message headers.");
    public static final ConfigOption<String> ID_HANDLER =
            ConfigOptions.key(SerdeConfig.ID_HANDLER)
                    .stringType()
                    .defaultValue(SerdeConfig.ID_HANDLER_DEFAULT)
                    .withDescription(
                            "    Used by serializers and deserializers. Fully-qualified Java classname of a class that implements\n"
                                    + "IdHandler and writes/reads the artifact identifier to/from the message payload.\n"
                                    + "Only used if "
                                    + SerdeConfig.ENABLE_HEADERS
                                    + " is set to false.");

    public static final ConfigOption<Boolean> ENABLE_CONFLUENT_ID_HANDLER =
            ConfigOptions.key(SerdeConfig.ENABLE_CONFLUENT_ID_HANDLER)
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Used by serializers and deserializers. Shortcut for enabling the\n "
                                    + "legacy Confluent-compatible implementation of IdHandler.\n"
                                    + "Only used if "
                                    + SerdeConfig.ENABLE_HEADERS
                                    + " is set to false.");

    /*
    TODO
     Artifact resolver strategy - default or provided ?
      encoding
      Avro datum provider
      Avro encoding
     */
    public static final ConfigOption<String> SUBJECT =
            ConfigOptions.key("subject")
                    .stringType()
                    .noDefaultValue()
                    .withFallbackKeys("schema-registry.subject")
                    .withDescription(
                            "The Apicurio Schema Registry subject under which to register the "
                                    + "schema used by this format during serialization. By default, "
                                    + "'kafka' and 'upsert-kafka' connectors use '<topic_name>-value' "
                                    + "or '<topic_name>-key' as the default subject name if this format "
                                    + "is used as the value or key format. But for other connectors (e.g. 'filesystem'), "
                                    + "the subject option is required when used as sink.");

    public static final ConfigOption<String> SCHEMA =
            ConfigOptions.key("schema")
                    .stringType()
                    .noDefaultValue()
                    .withFallbackKeys("schema-registry.schema")
                    .withDescription(
                            "The schema registered or to be registered in the Apicurio Schema Registry. "
                                    + "If no schema is provided Flink converts the table schema to avro schema. "
                                    + "The schema provided must match the table schema ('avro-apicurio') or "
                                    + "the Debezium schema which is a nullable record type including "
                                    + "fields 'before', 'after', 'op' ('debezium-avro-apicurio').");

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
    public static final ConfigOption<String> SSL_KEYSTORE_TYPE =
            ConfigOptions.key("ssl.keystore.type")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Type for SSL truststore");

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

    public static final ConfigOption<String> SSL_TRUSTSTORE_TYPE =
            ConfigOptions.key("ssl.truststore.type")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Type for SSL truststore");

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
    // Apicurio token security settings
    // --------------------------------------------------------------------------------------------
    public static final ConfigOption<String> AUTH_TOKEN_ENDPOINT =
            ConfigOptions.key("auth.token-endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Auth token endpoint");

    public static final ConfigOption<String> AUTH_CLIENT_ID =
            ConfigOptions.key("auth.client-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Auth client id");
    public static final ConfigOption<String> AUTH_CLIENT_SECRET =
            ConfigOptions.key("auth.client-secret")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Auth client secret");

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

    private AvroApicurioFormatOptions() {}
}
