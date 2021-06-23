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
                    .withDescription(
                            "The URL of the Confluent Schema Registry to fetch/register schemas.");

    public static final ConfigOption<String> SCHEMA_REGISTRY_SUBJECT =
            ConfigOptions.key("schema-registry.subject")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The Confluent Schema Registry subject under which to register the "
                                    + "schema used by this format during serialization. By default, "
                                    + "'kafka' and 'upsert-kafka' connectors use '<topic_name>-value' "
                                    + "or '<topic_name>-key' as the default subject name if this format "
                                    + "is used as the value or key format. But for other connectors (e.g. 'filesystem'), "
                                    + "the subject option is required when used as sink.");

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
}
