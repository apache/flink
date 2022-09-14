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

import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

/** Tests for the {@link ConfigurationUtils}. */
public class ConfigurationUtilsTest extends TestLogger {

    @Test
    public void testPropertiesToConfiguration() {
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

    @Test
    public void testHideSensitiveValues() {
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

    @Test
    public void testGetPrefixedKeyValuePairs() {
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

    @Test
    public void testConvertToString() {
        // String
        assertEquals("Simple String", ConfigurationUtils.convertToString("Simple String"));

        // Duration
        assertEquals("0 ms", ConfigurationUtils.convertToString(Duration.ZERO));
        assertEquals("123 ms", ConfigurationUtils.convertToString(Duration.ofMillis(123L)));
        assertEquals("1234 s", ConfigurationUtils.convertToString(Duration.ofMillis(1_234_000L)));
        assertEquals("25 h", ConfigurationUtils.convertToString(Duration.ofHours(25L)));

        // List
        final List<Object> listElements = new ArrayList<>();
        listElements.add("Test;String");
        listElements.add(Duration.ZERO);
        listElements.add(42);
        assertEquals("'Test;String';0 ms;42", ConfigurationUtils.convertToString(listElements));

        // Map
        final Map<Object, Object> mapElements = new HashMap<>();
        mapElements.put("A:,B", "C:,D");
        mapElements.put(10, 20);
        assertEquals("'''A:,B'':''C:,D''',10:20", ConfigurationUtils.convertToString(mapElements));
    }

    @Test
    public void testRandomTempDirectorySelection() {
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
