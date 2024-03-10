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

package org.apache.flink.formats.protobuf.registry.confluent;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Options for Schema Registry Avro format. */
public class RegistryFormatOptions {

    // --------------------------------------------------------------------------------------------
    // PRIVATE
    // --------------------------------------------------------------------------------------------

    public static final String PRIVATE_PREFIX = "confluent.";

    public static final ConfigOption<String> URL =
            ConfigOptions.key(PRIVATE_PREFIX + "url")
                    .stringType()
                    .noDefaultValue()
                    .withFallbackKeys("schema-registry.url")
                    .withFallbackKeys("url")
                    .withDescription(
                            "The URL of the Confluent Schema Registry to fetch/register schemas.");

    public static final ConfigOption<Integer> SCHEMA_ID =
            ConfigOptions.key(PRIVATE_PREFIX + "schema-id")
                    .intType()
                    .noDefaultValue()
                    .withFallbackKeys("schema-id")
                    .withDescription(
                            "The id of the corresponding Avro schema in Schema Registry."
                                    + " This is important for serialization schema. It ensures the"
                                    + " serialization schema writes records in that particular version."
                                    + " That way one can control e.g. records namespaces.");

    public static final ConfigOption<Integer> SCHEMA_CACHE_SIZE =
            ConfigOptions.key(PRIVATE_PREFIX + "schema-cache-size")
                    .intType()
                    .defaultValue(20)
                    .withFallbackKeys("schema-cache-size")
                    .withDescription("Maximum number of cached schemas.");

    public static final ConfigOption<String> LOGICAL_CLUSTER_ID =
            ConfigOptions.key(PRIVATE_PREFIX + "logical-cluster-id")
                    .stringType()
                    .noDefaultValue()
                    .withFallbackKeys("logical-cluster-id")
                    .withDescription("The kafka logical cluster id in CC");

    public static final ConfigOption<CredentialsSource> CREDENTIALS_SOURCE =
            ConfigOptions.key(PRIVATE_PREFIX + "credentials-source")
                    .enumType(CredentialsSource.class)
                    .defaultValue(CredentialsSource.DPAT)
                    .withFallbackKeys("credentials-source")
                    .withDescription("Where to get the credentials from");

    public static final ConfigOption<String> BASIC_AUTH_USER_INFO =
            ConfigOptions.key(PRIVATE_PREFIX + "basic-auth.user-info")
                    .stringType()
                    .noDefaultValue()
                    .withFallbackKeys("basic-auth.user-info")
                    .withDescription("Basic auth user info for schema registry");

    /** Where to get the credentials from. */
    public enum CredentialsSource {
        KEYS,
        DPAT
    }

    private RegistryFormatOptions() {}
}
