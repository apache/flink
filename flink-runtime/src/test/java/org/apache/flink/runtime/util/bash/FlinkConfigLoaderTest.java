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
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.testutils.junit.utils.TempDirUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FlinkConfigLoader}. */
@ExtendWith(ParameterizedTestExtension.class)
public class FlinkConfigLoaderTest {

    @Parameter public boolean standardYaml;

    @Parameters(name = "standardYaml: {0}")
    public static Collection<Boolean> parameters() {
        return Arrays.asList(true, false);
    }

    private static final String TEST_CONFIG_KEY = "test.key";
    private static final String TEST_CONFIG_VALUE = "test_value";

    @TempDir private java.nio.file.Path confDir;

    @BeforeEach
    void setUp() throws IOException {
        File flinkConfFile;
        if (standardYaml) {
            flinkConfFile =
                    TempDirUtils.newFile(
                            confDir.toAbsolutePath(), GlobalConfiguration.FLINK_CONF_FILENAME);
        } else {
            flinkConfFile =
                    TempDirUtils.newFile(
                            confDir.toAbsolutePath(),
                            GlobalConfiguration.LEGACY_FLINK_CONF_FILENAME);
        }
        FileWriter fw = new FileWriter(flinkConfFile);
        fw.write(TEST_CONFIG_KEY + ": " + TEST_CONFIG_VALUE + "\n");
        fw.close();
    }

    @AfterAll
    static void after() {
        // clean the standard yaml flag to avoid impact to other cases.
        GlobalConfiguration.setStandardYaml(true);
    }

    @TestTemplate
    void testLoadConfigurationConfigDirLongOpt() throws Exception {
        String[] args = {"--configDir", confDir.toFile().getAbsolutePath()};
        Configuration configuration = FlinkConfigLoader.loadConfiguration(args);
        verifyConfiguration(configuration, TEST_CONFIG_KEY, TEST_CONFIG_VALUE);
    }

    @TestTemplate
    void testloadAndModifyConfigurationConfigDirLongOpt() throws Exception {
        String[] args = {"--configDir", confDir.toFile().getAbsolutePath()};
        List<String> list = FlinkConfigLoader.loadAndModifyConfiguration(args);
        if (standardYaml) {
            assertThat(list).containsExactly("test:", "  key: " + TEST_CONFIG_VALUE);
        } else {
            assertThat(list).containsExactly(TEST_CONFIG_KEY + ": " + TEST_CONFIG_VALUE);
        }
    }

    @TestTemplate
    void testLoadConfigurationConfigDirShortOpt() throws Exception {
        String[] args = {"-c", confDir.toFile().getAbsolutePath()};
        Configuration configuration = FlinkConfigLoader.loadConfiguration(args);
        verifyConfiguration(configuration, TEST_CONFIG_KEY, TEST_CONFIG_VALUE);
    }

    @TestTemplate
    void testloadAndModifyConfigurationConfigDirShortOpt() throws Exception {
        String[] args = {"-c", confDir.toFile().getAbsolutePath()};
        List<String> list = FlinkConfigLoader.loadAndModifyConfiguration(args);
        if (standardYaml) {
            assertThat(list).containsExactly("test:", "  key: " + TEST_CONFIG_VALUE);
        } else {
            assertThat(list).containsExactly(TEST_CONFIG_KEY + ": " + TEST_CONFIG_VALUE);
        }
    }

    @TestTemplate
    void testLoadConfigurationDynamicPropertyWithSpace() throws Exception {
        String[] args = {"--configDir", confDir.toFile().getAbsolutePath(), "-D", "key=value"};
        Configuration configuration = FlinkConfigLoader.loadConfiguration(args);
        verifyConfiguration(configuration, "key", "value");
    }

    @TestTemplate
    void testloadAndModifyConfigurationDynamicPropertyWithSpace() throws Exception {
        String[] args = {"--configDir", confDir.toFile().getAbsolutePath(), "-D", "key=value"};
        List<String> list = FlinkConfigLoader.loadAndModifyConfiguration(args);
        if (standardYaml) {
            assertThat(list).containsExactly("test:", "  key: " + TEST_CONFIG_VALUE, "key: value");
        } else {
            assertThat(list)
                    .containsExactlyInAnyOrder(
                            TEST_CONFIG_KEY + ": " + TEST_CONFIG_VALUE, "key: value");
        }
    }

    @TestTemplate
    void testLoadConfigurationDynamicPropertyWithoutSpace() throws Exception {
        String[] args = {"--configDir", confDir.toFile().getAbsolutePath(), "-Dkey=value"};
        Configuration configuration = FlinkConfigLoader.loadConfiguration(args);
        verifyConfiguration(configuration, "key", "value");
    }

    @TestTemplate
    void testloadAndModifyConfigurationDynamicPropertyWithoutSpace() throws Exception {
        String[] args = {"--configDir", confDir.toFile().getAbsolutePath(), "-Dkey=value"};
        List<String> list = FlinkConfigLoader.loadAndModifyConfiguration(args);
        if (standardYaml) {
            assertThat(list).containsExactly("test:", "  key: " + TEST_CONFIG_VALUE, "key: value");
        } else {
            assertThat(list)
                    .containsExactlyInAnyOrder(
                            TEST_CONFIG_KEY + ": " + TEST_CONFIG_VALUE, "key: value");
        }
    }

    @TestTemplate
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

    @TestTemplate
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
        if (standardYaml) {
            assertThat(list).containsExactly("test:", "  key: " + TEST_CONFIG_VALUE, "key: value");
        } else {
            assertThat(list)
                    .containsExactlyInAnyOrder(
                            TEST_CONFIG_KEY + ": " + TEST_CONFIG_VALUE, "key: value");
        }
    }

    @TestTemplate
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
        if (standardYaml) {
            assertThat(list).containsExactly("test:", "  key: " + TEST_CONFIG_VALUE);
        } else {
            assertThat(list).containsExactlyInAnyOrder(TEST_CONFIG_KEY + ": " + TEST_CONFIG_VALUE);
        }
    }

    @TestTemplate
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
        if (standardYaml) {
            assertThat(list)
                    .containsExactly("test:", "  key: " + TEST_CONFIG_VALUE, key + ": " + value);
        } else {
            assertThat(list)
                    .containsExactlyInAnyOrder(
                            TEST_CONFIG_KEY + ": " + TEST_CONFIG_VALUE, key + ": " + value);
        }
    }

    @TestTemplate
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
        if (standardYaml) {
            assertThat(list).containsExactly("test:", "  key: " + TEST_CONFIG_VALUE);
        } else {
            assertThat(list).containsExactlyInAnyOrder(TEST_CONFIG_KEY + ": " + TEST_CONFIG_VALUE);
        }
    }

    @TestTemplate
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
        if (standardYaml) {
            assertThat(list)
                    .containsExactlyInAnyOrder(
                            "test:", "  key: " + TEST_CONFIG_VALUE, removeKey + ": " + removeValue);
        } else {
            assertThat(list)
                    .containsExactlyInAnyOrder(
                            TEST_CONFIG_KEY + ": " + TEST_CONFIG_VALUE,
                            removeKey + ": " + removeValue);
        }
    }

    @TestTemplate
    void testloadAndModifyConfigurationReplaceKeyValuesMatched() throws Exception {
        String newValue = "newValue";

        String[] args = {
            "--configDir",
            confDir.toFile().getAbsolutePath(),
            "--replaceKeyValue",
            String.format("%s,%s,%s", TEST_CONFIG_KEY, TEST_CONFIG_VALUE, newValue)
        };
        List<String> list = FlinkConfigLoader.loadAndModifyConfiguration(args);
        if (standardYaml) {
            assertThat(list).containsExactly("test:", "  key: " + newValue);
        } else {
            assertThat(list).containsExactlyInAnyOrder(TEST_CONFIG_KEY + ": " + newValue);
        }
    }

    @TestTemplate
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
        if (standardYaml) {
            assertThat(list).containsExactly("test:", "  key: " + TEST_CONFIG_VALUE);
        } else {
            assertThat(list).containsExactlyInAnyOrder(TEST_CONFIG_KEY + ": " + TEST_CONFIG_VALUE);
        }
    }

    @TestTemplate
    void testloadAndModifyConfigurationWithFlatten() throws Exception {
        String[] args = {"-c", confDir.toFile().getAbsolutePath(), "-flatten"};
        List<String> list = FlinkConfigLoader.loadAndModifyConfiguration(args);
        assertThat(list).containsExactly(TEST_CONFIG_KEY + ": " + TEST_CONFIG_VALUE);
    }

    @Test
    void testMigrateLegacyConfigToStandardYaml() throws Exception {
        try (FileWriter fw =
                new FileWriter(
                        new File(
                                confDir.toFile(),
                                GlobalConfiguration.LEGACY_FLINK_CONF_FILENAME))) {
            fw.write(TEST_CONFIG_KEY + ": " + TEST_CONFIG_VALUE + "\n");
            fw.write(
                    "pipeline.cached-files"
                            + ": "
                            + "name:file1,path:'file:///tmp/file1';name:file2,path:'hdfs:///tmp/file2'"
                            + "\n");
            fw.write(
                    "pipeline.default-kryo-serializers"
                            + ": "
                            + "class:org.example.ExampleClass,serializer:org.example.ExampleSerializer1;"
                            + " class:org.example.ExampleClass2,serializer:org.example.ExampleSerializer2"
                            + "\n");
        }
        Configuration configuration = GlobalConfiguration.loadConfiguration(confDir.toString());

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
        assertThat(configuration.getString(TEST_CONFIG_KEY, null))
                .isEqualTo(standardYamlConfig.getString(TEST_CONFIG_KEY, null));

        assertThat(
                        configuration.get(PipelineOptions.KRYO_DEFAULT_SERIALIZERS).stream()
                                .map(ConfigurationUtils::parseStringToMap)
                                .collect(Collectors.toList()))
                .isEqualTo(
                        standardYamlConfig.get(PipelineOptions.KRYO_DEFAULT_SERIALIZERS).stream()
                                .map(ConfigurationUtils::parseStringToMap)
                                .collect(Collectors.toList()));

        assertThat(
                        DistributedCache.parseCachedFilesFromString(
                                configuration.get(PipelineOptions.CACHED_FILES)))
                .isEqualTo(
                        DistributedCache.parseCachedFilesFromString(
                                standardYamlConfig.get(PipelineOptions.CACHED_FILES)));
    }

    private void verifyConfiguration(Configuration config, String key, String expectedValue) {
        ConfigOption<String> option = key(key).stringType().noDefaultValue();
        assertThat(config.get(option)).isEqualTo(expectedValue);
    }
}
