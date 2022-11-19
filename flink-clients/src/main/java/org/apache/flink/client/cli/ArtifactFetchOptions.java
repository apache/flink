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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.Map;

/** Artifact Fetch options. */
public class ArtifactFetchOptions {

    public static final ConfigOption<String> USER_ARTIFACTS_BASE_DIR =
            ConfigOptions.key("user.artifacts.base.dir")
                    .stringType()
                    .defaultValue("/opt/flink/artifacts")
                    .withDescription("The base dir to put the application job artifacts.");

    public static final ConfigOption<Map<String, String>> USER_ARTIFACT_HTTP_HEADER =
            ConfigOptions.key("user.artifacts.http.header")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "Custom HTTP header for HttpArtifactFetcher. The header will be applied when getting the application job artifacts. "
                                    + "Expected format: headerKey1:headerValue1,headerKey2:headerValue2.");
}
