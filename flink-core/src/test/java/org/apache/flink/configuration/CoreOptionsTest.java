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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CoreOptions}. */
class CoreOptionsTest {
    @Test
    void testGetParentFirstLoaderPatterns() {
        testParentFirst(
                CoreOptions::getParentFirstLoaderPatterns,
                CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS,
                CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL);
    }

    @Test
    void testGetPluginParentFirstLoaderPatterns() {
        testParentFirst(
                CoreOptions::getPluginParentFirstLoaderPatterns,
                CoreOptions.PLUGIN_ALWAYS_PARENT_FIRST_LOADER_PATTERNS,
                CoreOptions.PLUGIN_ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL);
    }

    private void testParentFirst(
            Function<Configuration, String[]> patternGetter,
            ConfigOption<List<String>> patternOption,
            ConfigOption<List<String>> additionalOption) {
        Configuration config = new Configuration();
        assertThat(patternGetter.apply(config))
                .containsExactly(patternOption.defaultValue().toArray(new String[0]));

        config.set(patternOption, Arrays.asList("hello", "world"));

        assertThat(patternGetter.apply(config)).containsExactly("hello", "world");

        config.set(additionalOption, Arrays.asList("how", "are", "you"));

        assertThat(patternGetter.apply(config))
                .containsExactly("hello", "world", "how", "are", "you");

        config.set(patternOption, Collections.emptyList());

        assertThat(patternGetter.apply(config)).containsExactly("how", "are", "you");
    }
}
