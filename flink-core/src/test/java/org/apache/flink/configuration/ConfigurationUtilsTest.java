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

package org.apache.flink.configuration;

import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link ConfigurationUtils}. */
@ExtendWith(ParameterizedTestExtension.class)
public class ConfigurationUtilsTest {

    @Parameter public boolean standardYaml;

    @Parameters(name = "standardYaml: {0}")
    public static Collection<Boolean> parameters() {
        return Arrays.asList(true, false);
    }

    @TestTemplate
    void testPropertiesToConfiguration() {
        final Properties properties = new Properties();
        final int entries = 10;

        for (int i = 0; i < entries; i++) {
            properties.setProperty("key" + i, "value" + i);
        }

        final Configuration configuration = ConfigurationUtils.createConfiguration(properties);

        for (String key : properties.stringPropertyNames()) {
            assertThat(configuration.getString(key, "")).isEqualTo(properties.getProperty(key));
        }

        assertThat(configuration.toMap()).hasSize(properties.size());
    }

    @TestTemplate
    void testHideSensitiveValues() {
        final Map<String, String> keyValuePairs = new HashMap<>();
        keyValuePairs.put("foobar", "barfoo");
        final String secretKey1 = "secret.key";
        keyValuePairs.put(secretKey1, "12345");
        final String secretKey2 = "my.password";
        keyValuePairs.put(secretKey2, "12345");

        final Map<String, String> expectedKeyValuePairs = new HashMap<>(keyValuePairs);

        for (String secretKey : Arrays.asList(secretKey1, secretKey2)) {
            expectedKeyValuePairs.put(secretKey, GlobalConfiguration.HIDDEN_CONTENT);
        }

        final Map<String, String> hiddenSensitiveValues =
                ConfigurationUtils.hideSensitiveValues(keyValuePairs);

        assertThat(hiddenSensitiveValues).isEqualTo(expectedKeyValuePairs);
    }

    @TestTemplate
    void testGetPrefixedKeyValuePairs() {
        final String prefix = "test.prefix.";
        final Map<String, String> expectedKeyValuePairs =
                new HashMap<String, String>() {
                    {
                        put("k1", "v1");
                        put("k2", "v2");
                    }
                };

        final Configuration configuration = new Configuration();
        expectedKeyValuePairs.forEach((k, v) -> configuration.setString(prefix + k, v));

        final Map<String, String> resultKeyValuePairs =
                ConfigurationUtils.getPrefixedKeyValuePairs(prefix, configuration);

        assertThat(resultKeyValuePairs).isEqualTo(expectedKeyValuePairs);
    }

    @TestTemplate
    void testConvertToString() {
        // String
        assertThat(ConfigurationUtils.convertToString("Simple String", standardYaml))
                .isEqualTo("Simple String");

        // Duration
        assertThat(ConfigurationUtils.convertToString(Duration.ZERO, standardYaml))
                .isEqualTo("0 ms");
        assertThat(ConfigurationUtils.convertToString(Duration.ofMillis(123L), standardYaml))
                .isEqualTo("123 ms");
        assertThat(ConfigurationUtils.convertToString(Duration.ofMillis(1_234_000L), standardYaml))
                .isEqualTo("1234 s");
        assertThat(ConfigurationUtils.convertToString(Duration.ofHours(25L), standardYaml))
                .isEqualTo("25 h");

        // List
        List<Object> listElements = new ArrayList<>();
        listElements.add("Test;String");
        listElements.add(Duration.ZERO);
        listElements.add(42);
        if (standardYaml) {
            assertThat("[Test;String, 0 ms, 42]")
                    .isEqualTo(ConfigurationUtils.convertToString(listElements, true));
        } else {
            assertThat("'Test;String';0 ms;42")
                    .isEqualTo(ConfigurationUtils.convertToString(listElements, false));
        }
        // Map
        Map<Object, Object> mapElements = new HashMap<>();
        mapElements.put("A:,B", "C:,D");
        mapElements.put(10, 20);
        if (standardYaml) {
            assertThat("{'A:,B': 'C:,D', 10: 20}")
                    .isEqualTo(ConfigurationUtils.convertToString(mapElements, true));
        } else {
            assertThat("'''A:,B'':''C:,D''',10:20")
                    .isEqualTo(ConfigurationUtils.convertToString(mapElements, false));
        }
    }

    @TestTemplate
    void testRandomTempDirectorySelection() {
        final Configuration configuration = new Configuration();
        final StringBuilder tempDirectories = new StringBuilder();
        final int numberTempDirectories = 20;

        for (int i = 0; i < numberTempDirectories; i++) {
            tempDirectories.append(UUID.randomUUID()).append(',');
        }

        configuration.set(CoreOptions.TMP_DIRS, tempDirectories.toString());

        final Set<File> allTempDirectories =
                Arrays.stream(ConfigurationUtils.parseTempDirectories(configuration))
                        .map(File::new)
                        .collect(Collectors.toSet());

        final Set<File> drawnTempDirectories = new HashSet<>();
        final int numberDraws = 100;

        for (int i = 0; i < numberDraws; i++) {
            drawnTempDirectories.add(ConfigurationUtils.getRandomTempDirectory(configuration));
        }

        assertThat(drawnTempDirectories).hasSizeGreaterThan(1);
        assertThat(drawnTempDirectories).isSubsetOf(allTempDirectories);
    }
}
