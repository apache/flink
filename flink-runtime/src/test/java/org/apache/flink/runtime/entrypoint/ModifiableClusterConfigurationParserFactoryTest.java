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

package org.apache.flink.runtime.entrypoint;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link ModifiableClusterConfigurationParserFactory}. */
class ModifiableClusterConfigurationParserFactoryTest {

    private static final CommandLineParser<ModifiableClusterConfiguration> commandLineParser =
            new CommandLineParser<>(new ModifiableClusterConfigurationParserFactory());

    @Test
    void testModifiableClusterConfigurationParsing() throws FlinkParseException {
        final String configDir = "/foo/bar";
        final String key = "key";
        final String value = "value";
        final String newValue = "value2";
        final String[] args = {
            "--configDir",
            configDir,
            "--removeKey",
            key,
            String.format("-D%s=%s", key, value),
            "--removeKeyValue",
            String.format("%s=%s", key, value),
            "--replaceKeyValue",
            String.format("%s,%s,%s", key, value, newValue),
            "--flattenConfig"
        };

        ModifiableClusterConfiguration modifiableClusterConfiguration =
                commandLineParser.parse(args);

        assertThat(modifiableClusterConfiguration.getConfigDir()).isEqualTo(configDir);

        Properties dynamicProperties = modifiableClusterConfiguration.getDynamicProperties();
        assertThat(dynamicProperties).containsEntry(key, value);

        List<String> removeKeys = modifiableClusterConfiguration.getRemoveKeys();
        assertThat(removeKeys).containsExactly(key);

        Properties removeKeyValues = modifiableClusterConfiguration.getRemoveKeyValues();
        assertThat(removeKeyValues).containsEntry(key, value);

        List<Tuple3<String, String, String>> replaceKeyValues =
                modifiableClusterConfiguration.getReplaceKeyValues();
        assertThat(replaceKeyValues).containsExactly(Tuple3.of(key, value, newValue));

        assertThat(modifiableClusterConfiguration.flattenConfig()).isTrue();
    }

    @Test
    void testOnlyRequiredArguments() throws FlinkParseException {
        final String configDir = "/foo/bar";
        final String[] args = {"--configDir", configDir};

        ModifiableClusterConfiguration modifiableClusterConfiguration =
                commandLineParser.parse(args);

        assertThat(modifiableClusterConfiguration.getConfigDir()).isEqualTo(configDir);
    }

    @Test
    void testMissingRequiredArgument() {
        final String[] args = {};

        assertThatThrownBy(() -> commandLineParser.parse(args))
                .isInstanceOf(FlinkParseException.class);
    }
}
