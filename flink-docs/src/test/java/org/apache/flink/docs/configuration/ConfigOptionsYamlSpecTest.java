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

package org.apache.flink.docs.configuration;

import org.apache.flink.docs.util.ConfigurationOptionLocator;

import org.junit.jupiter.api.Test;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the YAML compliance of our config options.
 *
 * <p>Note: This test doesn't really belong into flink-docs, but it contains the required utils and
 * depends on the right modules.
 */
class ConfigOptionsYamlSpecTest {

    @Test
    void testNoKeyPrefixOfOtherKey() throws Exception {
        final Collection<String> allOptions = new ArrayList<>();
        new ConfigurationOptionLocator()
                .discoverOptionsAndApply(
                        Paths.get(ConfigOptionsDocGeneratorTest.getProjectRootDir()),
                        (aClass, optionWithMetaInfos) -> {
                            optionWithMetaInfos.stream()
                                    .filter(o -> o.field.getAnnotation(Deprecated.class) == null)
                                    .map(o -> o.option.key())
                                    .forEach(allOptions::add);
                        });
        final List<String> keys = allOptions.stream().sorted().collect(Collectors.toList());

        for (int x = 0; x < keys.size(); x++) {
            final String checkedKey = keys.get(x);

            final Stream<String> stringStream =
                    keys.subList(x + 1, keys.size()).stream()
                            .filter(key -> key.startsWith(checkedKey + "."));

            assertThat(stringStream)
                    .as("Key of option '" + checkedKey + "' is prefix of another option.")
                    .isEmpty();
        }
    }
}
