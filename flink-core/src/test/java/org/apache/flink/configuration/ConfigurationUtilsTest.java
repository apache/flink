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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/** Tests for the {@link ConfigurationUtils}. */
public class ConfigurationUtilsTest extends TestLogger {

    private static final String[] TESTING_PATHS = {
        "C:\\\\", "current\\\\leaf", "/root/dir/./../spaced dir/dotted.d/leaf",
    };

    @Test
    public void testPropertiesToConfiguration() {
        final Properties properties = new Properties();
        final int entries = 10;

        for (int i = 0; i < entries; i++) {
            properties.setProperty("key" + i, "value" + i);
        }

        final Configuration configuration = ConfigurationUtils.createConfiguration(properties);

        for (String key : properties.stringPropertyNames()) {
            assertThat(configuration.getString(key, ""), is(equalTo(properties.getProperty(key))));
        }

        assertThat(configuration.toMap().size(), is(properties.size()));
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

        assertThat(hiddenSensitiveValues, is(equalTo(expectedKeyValuePairs)));
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

        assertThat(resultKeyValuePairs, is(equalTo(expectedKeyValuePairs)));
    }

    @Test
    public void testSplitPathsForEmptyPath() {
        String[] paths = ConfigurationUtils.splitPaths("");
        assertThat(paths, emptyArray());
    }

    @Test
    public void testSplitPathsForSupportedSeparators() {
        for (String separator : ConfigurationUtils.PATH_SEPARATORS) {
            List<String> expectedPaths =
                    Arrays.stream(TESTING_PATHS)
                            .filter(p -> !p.contains(separator))
                            .filter(p -> !p.contains(File.pathSeparator))
                            .collect(Collectors.toList());
            String separatedPaths = String.join(separator, expectedPaths);
            String[] actualPaths = ConfigurationUtils.splitPaths(separatedPaths);
            String message =
                    String.format(
                            "Separator %s pattern separators %s",
                            separator, Arrays.toString(ConfigurationUtils.PATH_SEPARATORS));
            assertEquals(message, expectedPaths, Arrays.asList(actualPaths));
        }
    }

    @Test
    public void testSplitPathsForNotSupportedSeparators() {
        String[] notSupportedSeparators =
                Stream.of("|", ":", ";")
                        .filter(s -> !s.equals(File.pathSeparator))
                        .toArray(String[]::new);
        for (String separator : notSupportedSeparators) {
            List<String> expectedPaths =
                    Arrays.stream(TESTING_PATHS)
                            .filter(p -> !p.contains(separator))
                            .filter(p -> !p.contains(File.pathSeparator))
                            .collect(Collectors.toList());
            String separatedPaths = String.join(separator, expectedPaths);
            String[] actualPaths = ConfigurationUtils.splitPaths(separatedPaths);
            String message =
                    String.format(
                            "Pattern separators %s separatedPaths %s actualPaths: %s",
                            Arrays.toString(ConfigurationUtils.PATH_SEPARATORS),
                            separatedPaths,
                            Arrays.toString(actualPaths));
            assertThat(message, actualPaths, arrayWithSize(1));
            assertEquals(message, separatedPaths, actualPaths[0]);
        }
    }

    @Test
    public void testSplitPathsForCustomSeparators() {
        String[] customSeparators =
                Stream.of(
                                Stream.of("|", "$"),
                                Stream.of(ConfigurationUtils.PATH_SEPARATORS),
                                // This way we can test both unix and windows path separator without
                                // ci test matrix.
                                Stream.of(":", ";"))
                        .flatMap(Function.identity())
                        .distinct()
                        .toArray(String[]::new);
        for (String separator : customSeparators) {
            List<String> expectedPaths =
                    Arrays.stream(TESTING_PATHS)
                            .filter(p -> Arrays.stream(customSeparators).noneMatch(p::contains))
                            .collect(Collectors.toList());
            String separatedPaths = String.join(separator, expectedPaths);
            String[] actualPaths = ConfigurationUtils.splitPaths(separatedPaths, customSeparators);
            String message =
                    String.format(
                            "Separator %s pattern separators %s",
                            separator, Arrays.toString(customSeparators));
            assertEquals(message, expectedPaths, Arrays.asList(actualPaths));
        }
    }
}
