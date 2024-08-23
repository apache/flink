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

package org.apache.flink.fs.gs.utils;

import org.apache.flink.fs.gs.TestUtils;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import com.google.auth.oauth2.GoogleCredentials;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Test construction of Storage instance in GSFileSystemFactory. */
@ExtendWith(ParameterizedTestExtension.class)
class ConfigUtilsStorageTest {

    /* The test case description. */
    private @Parameter String description;

    /* The value to use for the GOOGLE_APPLICATION_CREDENTIALS environment variable. */
    @Parameter(value = 1)
    @Nullable
    private String envGoogleApplicationCredentials;

    /* The Hadoop config. */
    @Parameter(value = 2)
    private org.apache.hadoop.conf.Configuration hadoopConfig;

    /* The expected credentials file to use. */
    @Parameter(value = 3)
    @Nullable
    private String expectedCredentialsFilePath;

    @Parameters(name = "description={0}")
    private static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[][] {
                    {
                        "no GAC in env, no credentials in hadoop conf",
                        null,
                        TestUtils.hadoopConfigFromMap(new HashMap<>()),
                        null,
                    },
                    {
                        "GAC in env, no credentials in hadoop conf",
                        "/opt/file.json",
                        TestUtils.hadoopConfigFromMap(new HashMap<>()),
                        "/opt/file.json",
                    },
                    {
                        "no GAC in env, credentials in hadoop conf",
                        null,
                        TestUtils.hadoopConfigFromMap(
                                new HashMap<String, String>() {
                                    {
                                        put(
                                                "google.cloud.auth.service.account.json.keyfile",
                                                "/opt/file.json");
                                    }
                                }),
                        "/opt/file.json",
                    },
                    {
                        "GAC in env, credentials in hadoop conf, GAC should take precedence",
                        "/opt/file1.json",
                        TestUtils.hadoopConfigFromMap(
                                new HashMap<String, String>() {
                                    {
                                        put(
                                                "google.cloud.auth.service.account.json.keyfile",
                                                "/opt/file2.json");
                                    }
                                }),
                        "/opt/file1.json",
                    },
                    {
                        "GAC in env, no credentials in hadoop conf, service accounts disabled",
                        "/opt/file.json",
                        TestUtils.hadoopConfigFromMap(
                                new HashMap<String, String>() {
                                    {
                                        put("google.cloud.auth.service.account.enable", "false");
                                    }
                                }),
                        null,
                    },
                    {
                        "no GAC in env, credentials in hadoop conf, service accounts disabled",
                        null,
                        TestUtils.hadoopConfigFromMap(
                                new HashMap<String, String>() {
                                    {
                                        put("google.cloud.auth.service.account.enable", "false");
                                        put(
                                                "google.cloud.auth.service.account.json.keyfile",
                                                "/opt/file.json");
                                    }
                                }),
                        null,
                    },
                });
    }

    @TestTemplate
    void shouldProperlyCreateStorageCredentials() {

        // populate this if we store credentials in the testing context
        Optional<GoogleCredentials> expectedCredentials = Optional.empty();

        // construct the testing config context
        HashMap<String, String> envs = new HashMap<>();
        if (envGoogleApplicationCredentials != null) {
            envs.put("GOOGLE_APPLICATION_CREDENTIALS", envGoogleApplicationCredentials);
        }
        HashMap<String, GoogleCredentials> credentials = new HashMap<>();
        if (expectedCredentialsFilePath != null) {
            expectedCredentials = Optional.of(GoogleCredentials.newBuilder().build());
            credentials.put(expectedCredentialsFilePath, expectedCredentials.get());
        }
        TestingConfigContext configContext =
                new TestingConfigContext(envs, new HashMap<>(), credentials);

        // load the storage credentials
        Optional<GoogleCredentials> loadedCredentials =
                ConfigUtils.getStorageCredentials(hadoopConfig, configContext);

        assertThat(loadedCredentials).isEqualTo(expectedCredentials);
    }
}
