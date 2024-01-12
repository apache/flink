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

package org.apache.flink.runtime.clusterframework;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.OperatingSystem;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BootstrapTools}. */
class BootstrapToolsTest {

    @TempDir private static java.nio.file.Path temporaryFolder;

    @Test
    void testSubstituteConfigKey() {
        String deprecatedKey1 = "deprecated-key";
        String deprecatedKey2 = "another-out_of-date_key";
        String deprecatedKey3 = "yet-one-more";

        String designatedKey1 = "newkey1";
        String designatedKey2 = "newKey2";
        String designatedKey3 = "newKey3";

        String value1 = "value1";
        String value2Designated = "designated-value2";
        String value2Deprecated = "deprecated-value2";

        // config contains only deprecated key 1, and for key 2 both deprecated and designated
        Configuration cfg = new Configuration();
        cfg.setString(deprecatedKey1, value1);
        cfg.setString(deprecatedKey2, value2Deprecated);
        cfg.setString(designatedKey2, value2Designated);

        BootstrapTools.substituteDeprecatedConfigKey(cfg, deprecatedKey1, designatedKey1);
        BootstrapTools.substituteDeprecatedConfigKey(cfg, deprecatedKey2, designatedKey2);
        BootstrapTools.substituteDeprecatedConfigKey(cfg, deprecatedKey3, designatedKey3);

        // value 1 should be set to designated
        assertThat(cfg.getString(designatedKey1, null)).isEqualTo(value1);

        // value 2 should not have been set, since it had a value already
        assertThat(cfg.getString(designatedKey2, null)).isEqualTo(value2Designated);

        // nothing should be in there for key 3
        assertThat(cfg.getString(designatedKey3, null)).isNull();
        assertThat(cfg.getString(deprecatedKey3, null)).isNull();
    }

    @Test
    void testSubstituteConfigKeyPrefix() {
        String deprecatedPrefix1 = "deprecated-prefix";
        String deprecatedPrefix2 = "-prefix-2";
        String deprecatedPrefix3 = "prefix-3";

        String designatedPrefix1 = "p1";
        String designatedPrefix2 = "ppp";
        String designatedPrefix3 = "zzz";

        String depr1 = deprecatedPrefix1 + "var";
        String depr2 = deprecatedPrefix2 + "env";
        String depr3 = deprecatedPrefix2 + "x";

        String desig1 = designatedPrefix1 + "var";
        String desig2 = designatedPrefix2 + "env";
        String desig3 = designatedPrefix2 + "x";

        String val1 = "1";
        String val2 = "2";
        String val3Depr = "3-";
        String val3Desig = "3+";

        // config contains only deprecated key 1, and for key 2 both deprecated and designated
        Configuration cfg = new Configuration();
        cfg.setString(depr1, val1);
        cfg.setString(depr2, val2);
        cfg.setString(depr3, val3Depr);
        cfg.setString(desig3, val3Desig);

        BootstrapTools.substituteDeprecatedConfigPrefix(cfg, deprecatedPrefix1, designatedPrefix1);
        BootstrapTools.substituteDeprecatedConfigPrefix(cfg, deprecatedPrefix2, designatedPrefix2);
        BootstrapTools.substituteDeprecatedConfigPrefix(cfg, deprecatedPrefix3, designatedPrefix3);

        assertThat(cfg.getString(desig1, null)).isEqualTo(val1);
        assertThat(cfg.getString(desig2, null)).isEqualTo(val2);
        assertThat(cfg.getString(desig3, null)).isEqualTo(val3Desig);

        // check that nothing with prefix 3 is contained
        for (String key : cfg.keySet()) {
            assertThat(key.startsWith(designatedPrefix3)).isFalse();
            assertThat(key.startsWith(deprecatedPrefix3)).isFalse();
        }
    }

    @Test
    void testUpdateTmpDirectoriesInConfiguration() {
        Configuration config = new Configuration();

        // test that default value is taken
        BootstrapTools.updateTmpDirectoriesInConfiguration(config, "default/directory/path");
        assertThat(config.get(CoreOptions.TMP_DIRS)).isEqualTo("default/directory/path");

        // test that we ignore default value is value is set before
        BootstrapTools.updateTmpDirectoriesInConfiguration(config, "not/default/directory/path");
        assertThat(config.get(CoreOptions.TMP_DIRS)).isEqualTo("default/directory/path");

        // test that empty value is not a magic string
        config.set(CoreOptions.TMP_DIRS, "");
        BootstrapTools.updateTmpDirectoriesInConfiguration(config, "some/new/path");
        assertThat(config.get(CoreOptions.TMP_DIRS)).isEmpty();
    }

    @Test
    void testShouldNotUpdateTmpDirectoriesInConfigurationIfNoValueConfigured() {
        Configuration config = new Configuration();
        BootstrapTools.updateTmpDirectoriesInConfiguration(config, null);
        assertThat(CoreOptions.TMP_DIRS.defaultValue()).isEqualTo(config.get(CoreOptions.TMP_DIRS));
    }

    @Test
    void testGetDynamicPropertiesAsString() {
        final Configuration baseConfig = new Configuration();
        baseConfig.setString("key.a", "a");
        baseConfig.setString("key.b", "b1");

        final Configuration targetConfig = new Configuration();
        targetConfig.setString("key.b", "b2");
        targetConfig.setString("key.c", "c");

        final String dynamicProperties =
                BootstrapTools.getDynamicPropertiesAsString(baseConfig, targetConfig);
        if (OperatingSystem.isWindows()) {
            assertThat(dynamicProperties).isEqualTo("-Dkey.b=\"b2\" -Dkey.c=\"c\"");
        } else {
            assertThat(dynamicProperties).isEqualTo("-Dkey.b='b2' -Dkey.c='c'");
        }
    }

    @Test
    void testEscapeDynamicPropertyValueWithSingleQuote() {
        final String value1 = "#a,b&c^d*e@f(g!h";
        assertThat(BootstrapTools.escapeWithSingleQuote(value1)).isEqualTo("'" + value1 + "'");

        final String value2 = "'foobar";
        assertThat(BootstrapTools.escapeWithSingleQuote(value2)).isEqualTo("''\\''foobar'");

        final String value3 = "foo''bar";
        assertThat(BootstrapTools.escapeWithSingleQuote(value3)).isEqualTo("'foo'\\'''\\''bar'");

        final String value4 = "'foo' 'bar'";
        assertThat(BootstrapTools.escapeWithSingleQuote(value4))
                .isEqualTo("''\\''foo'\\'' '\\''bar'\\'''");
    }

    @Test
    void testEscapeDynamicPropertyValueWithDoubleQuote() {
        final String value1 = "#a,b&c^d*e@f(g!h";
        assertThat(BootstrapTools.escapeWithDoubleQuote(value1))
                .isEqualTo("\"#a,b&c\"^^\"d*e@f(g!h\"");

        final String value2 = "foo\"bar'";
        assertThat(BootstrapTools.escapeWithDoubleQuote(value2)).isEqualTo("\"foo\\\"bar'\"");

        final String value3 = "\"foo\" \"bar\"";
        assertThat(BootstrapTools.escapeWithDoubleQuote(value3))
                .isEqualTo("\"\\\"foo\\\" \\\"bar\\\"\"");
    }

    @Test
    void testGetEnvironmentVariables() {
        Configuration testConf = new Configuration();
        testConf.setString("containerized.master.env.LD_LIBRARY_PATH", "/usr/lib/native");

        Map<String, String> res =
                ConfigurationUtils.getPrefixedKeyValuePairs("containerized.master.env.", testConf);

        assertThat(res).hasSize(1).containsEntry("LD_LIBRARY_PATH", "/usr/lib/native");
    }

    @Test
    void testGetEnvironmentVariablesErroneous() {
        Configuration testConf = new Configuration();
        testConf.setString("containerized.master.env.", "/usr/lib/native");

        Map<String, String> res =
                ConfigurationUtils.getPrefixedKeyValuePairs("containerized.master.env.", testConf);

        assertThat(res).isEmpty();
    }

    @Test
    void testWriteConfigurationAndReloadWithStandardYaml() throws Exception {
        testWriteConfigurationAndReloadInternal(true);
    }

    @Test
    void testWriteConfigurationAndReloadWithLegacyYaml() throws Exception {
        testWriteConfigurationAndReloadInternal(false);
    }

    private void testWriteConfigurationAndReloadInternal(boolean standardYaml) throws IOException {
        GlobalConfiguration.setStandardYaml(standardYaml);
        final File flinkConfDir = TempDirUtils.newFolder(temporaryFolder).getAbsoluteFile();
        final Configuration flinkConfig = new Configuration();

        final ConfigOption<List<String>> listStringConfigOption =
                ConfigOptions.key("test-list-string-key").stringType().asList().noDefaultValue();
        final List<String> list =
                Arrays.asList("A,B,C,D", "A'B'C'D", "A;BCD", "AB\"C\"D", "AB'\"D:B");
        flinkConfig.set(listStringConfigOption, list);
        assertThat(flinkConfig.get(listStringConfigOption))
                .containsExactlyInAnyOrderElementsOf(list);

        final ConfigOption<List<Duration>> listDurationConfigOption =
                ConfigOptions.key("test-list-duration-key")
                        .durationType()
                        .asList()
                        .noDefaultValue();
        final List<Duration> durationList =
                Arrays.asList(Duration.ofSeconds(3), Duration.ofMinutes(1));
        flinkConfig.set(listDurationConfigOption, durationList);
        assertThat(flinkConfig.get(listDurationConfigOption))
                .containsExactlyInAnyOrderElementsOf(durationList);

        final ConfigOption<List<MemorySize>> listMemoryConfigOption =
                ConfigOptions.key("test-list-memory-key").memoryType().asList().noDefaultValue();
        final List<MemorySize> memorySizeList =
                Arrays.asList(
                        MemorySize.ofMebiBytes(10), MemorySize.ofMebiBytes(20), MemorySize.ZERO);
        flinkConfig.set(listMemoryConfigOption, memorySizeList);
        assertThat(flinkConfig.get(listMemoryConfigOption))
                .containsExactlyInAnyOrderElementsOf(memorySizeList);

        final ConfigOption<Map<String, String>> mapConfigOption =
                ConfigOptions.key("test-map-key").mapType().noDefaultValue();
        final Map<String, String> map = new HashMap<>();
        map.put("key1", "A,B,C,D");
        map.put("key2", "A;BCD");
        map.put("key3", "A'B'C'D");
        map.put("key4", "AB\"C\"D");
        map.put("key5", "AB'\"D:B");
        flinkConfig.set(mapConfigOption, map);
        assertThat(flinkConfig.get(mapConfigOption)).containsAllEntriesOf(map);

        final ConfigOption<List<Map<String, String>>> listMapConfigOption =
                ConfigOptions.key("test-list-map-key").mapType().asList().noDefaultValue();
        List<Map<String, String>> listMap = Collections.singletonList(map);
        flinkConfig.set(listMapConfigOption, listMap);
        assertThat(flinkConfig.get(listMapConfigOption))
                .containsExactlyInAnyOrderElementsOf(listMap);

        final ConfigOption<Duration> durationConfigOption =
                ConfigOptions.key("test-duration-key").durationType().noDefaultValue();
        final Duration duration = Duration.ofMillis(3000);
        flinkConfig.set(durationConfigOption, duration);
        assertThat(flinkConfig.get(durationConfigOption)).isEqualTo(duration);

        final ConfigOption<MemorySize> memoryConfigOption =
                ConfigOptions.key("test-memory-key").memoryType().noDefaultValue();
        MemorySize memorySize = MemorySize.ofMebiBytes(10);
        flinkConfig.set(memoryConfigOption, memorySize);
        assertThat(memorySize).isEqualTo(flinkConfig.get(memoryConfigOption));

        BootstrapTools.writeConfiguration(
                flinkConfig, new File(flinkConfDir, GlobalConfiguration.getFlinkConfFilename()));
        final Configuration loadedFlinkConfig =
                GlobalConfiguration.loadConfiguration(flinkConfDir.getAbsolutePath());
        assertThat(loadedFlinkConfig.get(listStringConfigOption))
                .containsExactlyInAnyOrderElementsOf(list);
        assertThat(loadedFlinkConfig.get(listDurationConfigOption))
                .containsExactlyInAnyOrderElementsOf(durationList);
        assertThat(loadedFlinkConfig.get(mapConfigOption)).containsAllEntriesOf(map);
        assertThat(loadedFlinkConfig.get(durationConfigOption)).isEqualTo(duration);

        // clean the standard yaml flag to avoid impact to other cases.
        GlobalConfiguration.setStandardYaml(true);
    }
}
