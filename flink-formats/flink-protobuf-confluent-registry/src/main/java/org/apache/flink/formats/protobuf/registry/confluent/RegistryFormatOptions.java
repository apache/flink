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

import java.util.Map;

/** Options for Schema Registry proto format. */
public class RegistryFormatOptions {

    public static final ConfigOption<String> URL =
            ConfigOptions.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withFallbackKeys("schema-registry.url")
                    .withFallbackKeys("url")
                    .withDescription(
                            "The URL of the Confluent Schema Registry to fetch/register schemas.");

    public static final ConfigOption<Integer> SCHEMA_ID =
            ConfigOptions.key("schema-id")
                    .intType()
                    .noDefaultValue()
                    .withFallbackKeys("schema-id")
                    .withDescription(
                            "The id of the corresponding protobuf schema in Schema Registry."
                                    + " This is important for serialization schema. It ensures the"
                                    + " serialization schema writes records in that particular version."
                                    + " That way one can control e.g. records namespaces.");

    public static final ConfigOption<Integer> SCHEMA_CACHE_SIZE =
            ConfigOptions.key( "schema-cache-size")
                    .intType()
                    .defaultValue(20)
                    .withFallbackKeys("schema-cache-size")
                    .withDescription("Maximum number of cached schemas.");

    public static final ConfigOption<String> BASIC_AUTH_USER_INFO =
            ConfigOptions.key("basic-auth.user-info")
                    .stringType()
                    .noDefaultValue()
                    .withFallbackKeys("basic-auth.user-info")
                    .withDescription("Basic auth user info for schema registry");

    public static final ConfigOption<Map<String, String>> PROPERTIES =
            ConfigOptions.key("properties")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "Properties map that is forwarded to the underlying Schema Registry. "
                                    + "This is useful for options that are not officially exposed "
                                    + "via Flink config options. However, note that Flink options "
                                    + "have higher precedence.");

    private RegistryFormatOptions() {}
}
