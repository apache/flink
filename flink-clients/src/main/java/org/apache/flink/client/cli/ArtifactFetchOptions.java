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

package org.apache.flink.client.cli;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.List;
import java.util.Map;

import static org.apache.flink.configuration.ConfigOptions.key;

/** Artifact Fetch options. */
@PublicEvolving
public class ArtifactFetchOptions {

    public static final ConfigOption<String> BASE_DIR =
            ConfigOptions.key("user.artifacts.base-dir")
                    .stringType()
                    .defaultValue("/opt/flink/artifacts")
                    .withDescription("The base dir to put the application job artifacts.");

    public static final ConfigOption<List<String>> ARTIFACT_LIST =
            key("user.artifacts.artifact-list")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "A semicolon-separated list of the additional artifacts to fetch for the job before setting up the application cluster."
                                    + " All given elements have to be valid URIs. Example: s3://sandbox-bucket/format.jar;http://sandbox-server:1234/udf.jar");

    public static final ConfigOption<Boolean> RAW_HTTP_ENABLED =
            ConfigOptions.key("user.artifacts.raw-http-enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Enables artifact fetching from raw HTTP endpoints.");

    public static final ConfigOption<Map<String, String>> HTTP_HEADERS =
            ConfigOptions.key("user.artifacts.http-headers")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "Custom HTTP header(s) for the HTTP artifact fetcher. The header(s) will be applied when getting the application job artifacts."
                                    + " Expected format: headerKey1:headerValue1,headerKey2:headerValue2.");
}
