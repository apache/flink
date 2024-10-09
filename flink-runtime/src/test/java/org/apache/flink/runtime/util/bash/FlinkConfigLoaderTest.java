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

import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationFileMigrationUtils;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.testutils.junit.utils.TempDirUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FlinkConfigLoader}. */
public class FlinkConfigLoaderTest {

    private static final String TEST_CONFIG_KEY = "test.key";
    private static final String TEST_CONFIG_VALUE = "test_value";

    @TempDir private java.nio.file.Path confDir;

    @BeforeEach
    void setUp() throws IOException {
        File flinkConfFile =
                TempDirUtils.newFile(
                        confDir.toAbsolutePath(), GlobalConfiguration.FLINK_CONF_FILENAME);
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
    void testloadAndModifyConfigurationConfigDirLongOpt() throws Exception {
        String[] args = {"--configDir", confDir.toFile().getAbsolutePath()};
        List<String> list = FlinkConfigLoader.loadAndModifyConfiguration(args);
        assertThat(list).containsExactly("test:", "  key: " + TEST_CONFIG_VALUE);
    }

    @Test
    void testLoadConfigurationConfigDirShortOpt() throws Exception {
        String[] args = {"-c", confDir.toFile().getAbsolutePath()};
        Configuration configuration = FlinkConfigLoader.loadConfiguration(args);
        verifyConfiguration(configuration, TEST_CONFIG_KEY, TEST_CONFIG_VALUE);
    }

    @Test
    void testloadAndModifyConfigurationConfigDirShortOpt() throws Exception {
        String[] args = {"-c", confDir.toFile().getAbsolutePath()};
        List<String> list = FlinkConfigLoader.loadAndModifyConfiguration(args);
        assertThat(list).containsExactly("test:", "  key: " + TEST_CONFIG_VALUE);
    }

    @Test
    void testLoadConfigurationDynamicPropertyWithSpace() throws Exception {
        String[] args = {"--configDir", confDir.toFile().getAbsolutePath(), "-D", "key=value"};
        Configuration configuration = FlinkConfigLoader.loadConfiguration(args);
        verifyConfiguration(configuration, "key", "value");
    }

    @Test
    void testloadAndModifyConfigurationDynamicPropertyWithSpace() throws Exception {
        String[] args = {"--configDir", confDir.toFile().getAbsolutePath(), "-D", "key=value"};
        List<String> list = FlinkConfigLoader.loadAndModifyConfiguration(args);
        assertThat(list).containsExactly("test:", "  key: " + TEST_CONFIG_VALUE, "key: value");
    }

    @Test
    void testLoadConfigurationDynamicPropertyWithoutSpace() throws Exception {
        String[] args = {"--configDir", confDir.toFile().getAbsolutePath(), "-Dkey=value"};
        Configuration configuration = FlinkConfigLoader.loadConfiguration(args);
        verifyConfiguration(configuration, "key", "value");
    }

    @Test
    void testloadAndModifyConfigurationDynamicPropertyWithoutSpace() throws Exception {
        String[] args = {"--configDir", confDir.toFile().getAbsolutePath(), "-Dkey=value"};
        List<String> list = FlinkConfigLoader.loadAndModifyConfiguration(args);
        assertThat(list).containsExactly("test:", "  key: " + TEST_CONFIG_VALUE, "key: value");
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

    @Test
    void testloadAndModifyConfigurationIgnoreUnknownToken() throws Exception {
        String[] args = {
            "unknown",
            "-u",
            "--configDir",
            confDir.toFile().getAbsolutePath(),
            "--unknown",
            "-Dkey=value"
        };
        List<String> list = FlinkConfigLoader.loadAndModifyConfiguration(args);
        assertThat(list).containsExactly("test:", "  key: " + TEST_CONFIG_VALUE, "key: value");
    }

    @Test
    void testloadAndModifyConfigurationRemoveKeysMatched() throws Exception {
        String key = "key";

        String[] args = {
            "--configDir",
            confDir.toFile().getAbsolutePath(),
            String.format("-D%s=value", key),
            "--removeKey",
            key
        };
        List<String> list = FlinkConfigLoader.loadAndModifyConfiguration(args);
        assertThat(list).containsExactly("test:", "  key: " + TEST_CONFIG_VALUE);
    }

    @Test
    void testloadAndModifyConfigurationRemoveKeysNotMatched() throws Exception {
        String key = "key";
        String value = "value";
        String removeKey = "removeKey";

        String[] args = {
            "--configDir",
            confDir.toFile().getAbsolutePath(),
            String.format("-D%s=%s", key, value),
            "--removeKey",
            removeKey
        };
        List<String> list = FlinkConfigLoader.loadAndModifyConfiguration(args);
        assertThat(list)
                .containsExactly("test:", "  key: " + TEST_CONFIG_VALUE, key + ": " + value);
    }

    @Test
    void testloadAndModifyConfigurationRemoveKeyValuesMatched() throws Exception {
        String removeKey = "removeKey";
        String removeValue = "removeValue";

        String[] args = {
            "--configDir",
            confDir.toFile().getAbsolutePath(),
            String.format("-D%s=%s", removeKey, removeValue),
            "--removeKeyValue",
            String.format("%s=%s", removeKey, removeValue)
        };
        List<String> list = FlinkConfigLoader.loadAndModifyConfiguration(args);
        assertThat(list).containsExactly("test:", "  key: " + TEST_CONFIG_VALUE);
    }

    @Test
    void testloadAndModifyConfigurationRemoveKeyValuesNotMatched() throws Exception {
        String removeKey = "removeKey";
        String removeValue = "removeValue";
        String nonExistentValue = "nonExistentValue";

        String[] args = {
            "--configDir",
            confDir.toFile().getAbsolutePath(),
            String.format("-D%s=%s", removeKey, removeValue),
            "--removeKeyValue",
            String.format("%s=%s", removeKey, nonExistentValue)
        };
        List<String> list = FlinkConfigLoader.loadAndModifyConfiguration(args);
        assertThat(list)
                .containsExactlyInAnyOrder(
                        "test:", "  key: " + TEST_CONFIG_VALUE, removeKey + ": " + removeValue);
    }

    @Test
    void testloadAndModifyConfigurationReplaceKeyValuesMatched() throws Exception {
        String newValue = "newValue";

        String[] args = {
            "--configDir",
            confDir.toFile().getAbsolutePath(),
            "--replaceKeyValue",
            String.format("%s,%s,%s", TEST_CONFIG_KEY, TEST_CONFIG_VALUE, newValue)
        };
        List<String> list = FlinkConfigLoader.loadAndModifyConfiguration(args);
        assertThat(list).containsExactly("test:", "  key: " + newValue);
    }

    @Test
    void testloadAndModifyConfigurationReplaceKeyValuesNotMatched() throws Exception {
        String nonExistentValue = "nonExistentValue";
        String newValue = "newValue";

        String[] args = {
            "--configDir",
            confDir.toFile().getAbsolutePath(),
            "--replaceKeyValue",
            String.format("%s,%s,%s", TEST_CONFIG_KEY, nonExistentValue, newValue)
        };
        List<String> list = FlinkConfigLoader.loadAndModifyConfiguration(args);
        assertThat(list).containsExactly("test:", "  key: " + TEST_CONFIG_VALUE);
    }

    @Test
    void testloadAndModifyConfigurationWithFlatten() throws Exception {
        String[] args = {"-c", confDir.toFile().getAbsolutePath(), "-flatten"};
        List<String> list = FlinkConfigLoader.loadAndModifyConfiguration(args);
        assertThat(list).containsExactly(TEST_CONFIG_KEY + ": " + TEST_CONFIG_VALUE);
    }

    @Test
    void testMigrateLegacyConfigToStandardYaml() throws Exception {
        File file =
                new File(
                        confDir.toFile(),
                        ConfigurationFileMigrationUtils.LEGACY_FLINK_CONF_FILENAME);
        try (FileWriter fw = new FileWriter(file)) {
            fw.write(TEST_CONFIG_KEY + ": " + TEST_CONFIG_VALUE + "\n");
            fw.write(
                    "pipeline.cached-files"
                            + ": "
                            + "name:file1,path:'file:///tmp/file1';name:file2,path:'hdfs:///tmp/file2'"
                            + "\n");
        }
        Map<String, String> configuration =
                ConfigurationFileMigrationUtils.loadLegacyYAMLResource(file);

        File newConfigFolder = TempDirUtils.newFolder(confDir);
        try (FileWriter fw =
                new FileWriter(
                        TempDirUtils.newFile(
                                newConfigFolder.toPath(),
                                GlobalConfiguration.FLINK_CONF_FILENAME))) {
            String[] args = {"-c", confDir.toFile().getAbsolutePath()};
            for (String line : FlinkConfigLoader.migrateLegacyConfigurationToStandardYaml(args)) {
                fw.write(line + "\n");
            }
        }

        Configuration standardYamlConfig =
                GlobalConfiguration.loadConfiguration(newConfigFolder.toString());
        assertThat(configuration.getOrDefault(TEST_CONFIG_KEY, null))
                .isEqualTo(standardYamlConfig.getString(TEST_CONFIG_KEY, null));

        List<String> cachedFiles =
                ConfigurationUtils.convertToList(
                        configuration.get(PipelineOptions.CACHED_FILES.key()), String.class);
        assertThat(DistributedCache.parseCachedFilesFromString(cachedFiles))
                .isEqualTo(
                        DistributedCache.parseCachedFilesFromString(
                                standardYamlConfig.get(PipelineOptions.CACHED_FILES)));
    }

    private void verifyConfiguration(Configuration config, String key, String expectedValue) {
        ConfigOption<String> option = key(key).stringType().noDefaultValue();
        assertThat(config.get(option)).isEqualTo(expectedValue);
    }
}
