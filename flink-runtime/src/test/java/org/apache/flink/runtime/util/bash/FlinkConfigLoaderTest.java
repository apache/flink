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

package org.apache.flink.runtime.util.bash;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.testutils.junit.utils.TempDirUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FlinkConfigLoader}. */
class FlinkConfigLoaderTest {

    private static final String TEST_CONFIG_KEY = "test.key";
    private static final String TEST_CONFIG_VALUE = "test_value";

    @TempDir private java.nio.file.Path confDir;

    @BeforeEach
    void setUp() throws IOException {
        File flinkConfFile = TempDirUtils.newFile(confDir.toAbsolutePath(), "flink-conf.yaml");
        FileWriter fw = new FileWriter(flinkConfFile);
        fw.write(TEST_CONFIG_KEY + ": " + TEST_CONFIG_VALUE + "\n");
        fw.close();
    }

    @Test
    void testLoadConfigurationConfigDirLongOpt() throws Exception {
        String[] args = {"--configDir", confDir.toFile().getAbsolutePath()};
        Configuration configuration = FlinkConfigLoader.loadConfiguration(args);
        verifyConfiguration(configuration, TEST_CONFIG_KEY, TEST_CONFIG_VALUE);
    }

    @Test
    void testLoadConfigurationConfigDirShortOpt() throws Exception {
        String[] args = {"-c", confDir.toFile().getAbsolutePath()};
        Configuration configuration = FlinkConfigLoader.loadConfiguration(args);
        verifyConfiguration(configuration, TEST_CONFIG_KEY, TEST_CONFIG_VALUE);
    }

    @Test
    void testLoadConfigurationDynamicPropertyWithSpace() throws Exception {
        String[] args = {"--configDir", confDir.toFile().getAbsolutePath(), "-D", "key=value"};
        Configuration configuration = FlinkConfigLoader.loadConfiguration(args);
        verifyConfiguration(configuration, "key", "value");
    }

    @Test
    void testLoadConfigurationDynamicPropertyWithoutSpace() throws Exception {
        String[] args = {"--configDir", confDir.toFile().getAbsolutePath(), "-Dkey=value"};
        Configuration configuration = FlinkConfigLoader.loadConfiguration(args);
        verifyConfiguration(configuration, "key", "value");
    }

    @Test
    void testLoadConfigurationIgnoreUnknownToken() throws Exception {
        String[] args = {
            "unknown",
            "-u",
            "--configDir",
            confDir.toFile().getAbsolutePath(),
            "--unknown",
            "-Dkey=value"
        };
        Configuration configuration = FlinkConfigLoader.loadConfiguration(args);
        verifyConfiguration(configuration, TEST_CONFIG_KEY, TEST_CONFIG_VALUE);
        verifyConfiguration(configuration, "key", "value");
    }

    private void verifyConfiguration(Configuration config, String key, String expectedValue) {
        ConfigOption<String> option = key(key).stringType().noDefaultValue();
        assertThat(config.get(option)).isEqualTo(expectedValue);
    }
}
