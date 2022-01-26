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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.fs.gs.TestUtils;

import org.apache.flink.shaded.guava30.com.google.common.collect.MapDifference;
import org.apache.flink.shaded.guava30.com.google.common.collect.Maps;

import com.google.cloud.storage.StorageOptions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test construction of Hadoop config in GSFileSystemFactory. */
@RunWith(Parameterized.class)
public class ConfigUtilsHadoopTest {

    /* The test case description. */
    @Parameterized.Parameter(value = 0)
    public String description;

    /* The value to use for the HADOOP_CONF_DIR environment variable. */
    @Parameterized.Parameter(value = 1)
    public @Nullable String envHadoopConfDir;

    /* The value to use for the Flink config. */
    @Parameterized.Parameter(value = 2)
    public Configuration flinkConfig;

    /* The additional Hadoop resources to add to the hadoop config when hadoop conf dir is present. */
    @Parameterized.Parameter(value = 3)
    public org.apache.hadoop.conf.Configuration additionalHadoopConfig;

    /* The expected Hadoop configuration. */
    @Parameterized.Parameter(value = 4)
    public org.apache.hadoop.conf.Configuration expectedHadoopConfig;

    @Parameterized.Parameters(name = "description={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[][] {
                    {
                        "no env hadoop conf dir, no flink hadoop conf dir, no flink hadoop options, no hadoop options in conf dir",
                        null,
                        Configuration.fromMap(new HashMap<>()),
                        TestUtils.hadoopConfigFromMap(new HashMap<>()),
                        TestUtils.hadoopConfigFromMap(new HashMap<>()),
                    },
                    {
                        "no env hadoop conf dir, no flink hadoop conf dir, no flink hadoop options, hadoop options in conf dir",
                        null,
                        Configuration.fromMap(new HashMap<>()),
                        TestUtils.hadoopConfigFromMap(
                                new HashMap<String, String>() {
                                    {
                                        put("fs.gs.project.id", "project-id");
                                    }
                                }),
                        TestUtils.hadoopConfigFromMap(new HashMap<>()),
                    },
                    {
                        "env hadoop conf dir, no flink hadoop conf dir, no flink hadoop options, hadoop options in conf dir",
                        "/hadoop/conf",
                        Configuration.fromMap(new HashMap<>()),
                        TestUtils.hadoopConfigFromMap(
                                new HashMap<String, String>() {
                                    {
                                        put("fs.gs.project.id", "project-id");
                                    }
                                }),
                        TestUtils.hadoopConfigFromMap(
                                new HashMap<String, String>() {
                                    {
                                        put("fs.gs.project.id", "project-id");
                                    }
                                }),
                    },
                    {
                        "no env hadoop conf dir, flink hadoop conf dir, no flink hadoop options, hadoop options in conf dir",
                        null,
                        Configuration.fromMap(
                                new HashMap<String, String>() {
                                    {
                                        put("env.hadoop.conf.dir", "/hadoop/conf");
                                    }
                                }),
                        TestUtils.hadoopConfigFromMap(
                                new HashMap<String, String>() {
                                    {
                                        put("fs.gs.project.id", "project-id");
                                    }
                                }),
                        TestUtils.hadoopConfigFromMap(
                                new HashMap<String, String>() {
                                    {
                                        put("fs.gs.project.id", "project-id");
                                    }
                                }),
                    },
                    {
                        "env hadoop conf dir, flink hadoop conf dir, no flink hadoop options, hadoop options in conf dir",
                        "/hadoop/conf1",
                        Configuration.fromMap(
                                new HashMap<String, String>() {
                                    {
                                        put("env.hadoop.conf.dir", "/hadoop/conf2");
                                    }
                                }),
                        TestUtils.hadoopConfigFromMap(
                                new HashMap<String, String>() {
                                    {
                                        put("fs.gs.project.id", "project-id");
                                    }
                                }),
                        TestUtils.hadoopConfigFromMap(
                                new HashMap<String, String>() {
                                    {
                                        put("fs.gs.project.id", "project-id");
                                    }
                                }),
                    },
                    {
                        "env hadoop conf dir, no flink hadoop conf dir, flink hadoop options, hadoop options in conf dir",
                        "/hadoop/conf",
                        Configuration.fromMap(
                                new HashMap<String, String>() {
                                    {
                                        put("gs.block.size", "10000");
                                    }
                                }),
                        TestUtils.hadoopConfigFromMap(
                                new HashMap<String, String>() {
                                    {
                                        put("fs.gs.project.id", "project-id");
                                    }
                                }),
                        TestUtils.hadoopConfigFromMap(
                                new HashMap<String, String>() {
                                    {
                                        put("fs.gs.block.size", "10000");
                                        put("fs.gs.project.id", "project-id");
                                    }
                                }),
                    },
                    {
                        "env hadoop conf dir, no flink hadoop conf dir, flink hadoop options, hadoop options in conf dir, hadoop options overlap",
                        "/hadoop/conf",
                        Configuration.fromMap(
                                new HashMap<String, String>() {
                                    {
                                        put("gs.project.id", "project-id-1");
                                    }
                                }),
                        TestUtils.hadoopConfigFromMap(
                                new HashMap<String, String>() {
                                    {
                                        put("fs.gs.project.id", "project-id-2");
                                    }
                                }),
                        TestUtils.hadoopConfigFromMap(
                                new HashMap<String, String>() {
                                    {
                                        put("fs.gs.project.id", "project-id-1");
                                    }
                                }),
                    },
                    {
                        "env hadoop conf dir, no flink hadoop conf dir, no flink hadoop options, hadoop enabled auth options in conf dir",
                        "/hadoop/conf",
                        Configuration.fromMap(new HashMap<>()),
                        TestUtils.hadoopConfigFromMap(
                                new HashMap<String, String>() {
                                    {
                                        put("google.cloud.auth.service.account.enable", "true");
                                        put(
                                                "google.cloud.auth.service.account.json.keyfile",
                                                "/opt/file.json");
                                    }
                                }),
                        TestUtils.hadoopConfigFromMap(
                                new HashMap<String, String>() {
                                    {
                                        put("google.cloud.auth.service.account.enable", "true");
                                        put(
                                                "google.cloud.auth.service.account.json.keyfile",
                                                "/opt/file.json");
                                    }
                                }),
                    },
                    {
                        "env hadoop conf dir, no flink hadoop conf dir, no flink hadoop options, hadoop disabled auth options in conf dir",
                        "/hadoop/conf",
                        Configuration.fromMap(new HashMap<>()),
                        TestUtils.hadoopConfigFromMap(
                                new HashMap<String, String>() {
                                    {
                                        put("google.cloud.auth.service.account.enable", "false");
                                        put(
                                                "google.cloud.auth.service.account.json.keyfile",
                                                "/opt/file.json");
                                    }
                                }),
                        TestUtils.hadoopConfigFromMap(
                                new HashMap<String, String>() {
                                    {
                                        put("google.cloud.auth.service.account.enable", "false");
                                        put(
                                                "google.cloud.auth.service.account.json.keyfile",
                                                "/opt/file.json");
                                    }
                                }),
                    },
                });
    }

    private @Nullable String expectedHadoopConfigDir;

    @Before
    public void before() {

        // determine which hadoop conf dir we expect to receive
        expectedHadoopConfigDir = flinkConfig.get(CoreOptions.FLINK_HADOOP_CONF_DIR);
        if (expectedHadoopConfigDir == null) {
            expectedHadoopConfigDir = envHadoopConfDir;
        }
    }

    @Test
    public void shouldProperlyCreateHadoopConfig() {
        final String[] actualHadoopConfigDir = {null};

        // get the hadoop configuration
        org.apache.hadoop.conf.Configuration hadoopConfig =
                ConfigUtils.getHadoopConfiguration(
                        flinkConfig,
                        new ConfigUtils.ConfigContext() {
                            @Override
                            public Optional<String> getenv(String name) {
                                if ("HADOOP_CONF_DIR".equals(name)) {
                                    return Optional.ofNullable(envHadoopConfDir);
                                }
                                return Optional.empty();
                            }

                            @Override
                            public void addHadoopResourcesFromDir(
                                    org.apache.hadoop.conf.Configuration config, String configDir) {
                                actualHadoopConfigDir[0] = configDir;
                                config.addResource(additionalHadoopConfig);
                            }

                            @Override
                            public void setStorageCredentialsFromFile(
                                    StorageOptions.Builder storageOptionsBuilder,
                                    String credentialsPath) {
                                throw new UnsupportedOperationException();
                            }
                        });

        assertEquals(expectedHadoopConfigDir, actualHadoopConfigDir[0]);

        Map<String, String> expectedHadoopConfigMap =
                TestUtils.hadoopConfigToMap(expectedHadoopConfig);
        Map<String, String> hadoopConfigMap = TestUtils.hadoopConfigToMap(hadoopConfig);
        MapDifference<String, String> difference =
                Maps.difference(expectedHadoopConfigMap, hadoopConfigMap);

        assertEquals(Collections.EMPTY_MAP, difference.entriesDiffering());
        assertEquals(Collections.EMPTY_MAP, difference.entriesOnlyOnLeft());
        assertEquals(Collections.EMPTY_MAP, difference.entriesOnlyOnRight());
    }
}
