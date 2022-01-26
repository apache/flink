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

import com.google.cloud.storage.StorageOptions;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/** Test construction of Storage instance in GSFileSystemFactory. */
@RunWith(Parameterized.class)
public class ConfigUtilsStorageTest {

    /* The test case description. */
    @Parameterized.Parameter(value = 0)
    public String description;

    /* The value to use for the GOOGLE_APPLICATION_CREDENTIALS environment variable. */
    @Parameterized.Parameter(value = 1)
    public @Nullable String envGoogleApplicationCredentials;

    /* The Hadoop config. */
    @Parameterized.Parameter(value = 2)
    public org.apache.hadoop.conf.Configuration hadoopConfig;

    /* The expected credentials file to use. */
    @Parameterized.Parameter(value = 3)
    public @Nullable String expectedCredentialsFile;

    @Parameterized.Parameters(name = "description={0}")
    public static Collection<Object[]> data() {
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

    @Test
    public void shouldProperlyCreateStorageOptions() {

        final String[] actualCredentialsPath = {null};

        StorageOptions storageOptions =
                ConfigUtils.getStorageOptions(
                        hadoopConfig,
                        new ConfigUtils.ConfigContext() {
                            @Override
                            public Optional<String> getenv(String name) {
                                if ("GOOGLE_APPLICATION_CREDENTIALS".equals(name)) {
                                    return Optional.ofNullable(envGoogleApplicationCredentials);
                                }
                                return Optional.empty();
                            }

                            @Override
                            public void addHadoopResourcesFromDir(
                                    Configuration config, String configDir) {
                                throw new UnsupportedOperationException();
                            }

                            @Override
                            public void setStorageCredentialsFromFile(
                                    StorageOptions.Builder storageOptionsBuilder,
                                    String credentialsPath) {
                                actualCredentialsPath[0] = credentialsPath;
                            }
                        });

        assertNotNull(storageOptions);
        assertEquals(expectedCredentialsFile, actualCredentialsPath[0]);
    }
}
