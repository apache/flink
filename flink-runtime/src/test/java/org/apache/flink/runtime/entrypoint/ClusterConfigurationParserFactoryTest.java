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

import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link ClusterConfigurationParserFactory}. */
class ClusterConfigurationParserFactoryTest {

    private static final CommandLineParser<ClusterConfiguration> commandLineParser =
            new CommandLineParser<>(new ClusterConfigurationParserFactory());

    @Test
    void testEntrypointClusterConfigurationParsing() throws FlinkParseException {
        final String configDir = "/foo/bar";
        final String key = "key";
        final String value = "value";
        final String arg1 = "arg1";
        final String arg2 = "arg2";
        final String[] args = {
            "--configDir", configDir, String.format("-D%s=%s", key, value), arg1, arg2
        };

        final ClusterConfiguration clusterConfiguration = commandLineParser.parse(args);

        assertThat(clusterConfiguration.getConfigDir()).isEqualTo(configDir);
        final Properties dynamicProperties = clusterConfiguration.getDynamicProperties();

        assertThat(dynamicProperties).containsEntry(key, value);

        assertThat(clusterConfiguration.getArgs()).containsExactly(arg1, arg2);
    }

    @Test
    void testOnlyRequiredArguments() throws FlinkParseException {
        final String configDir = "/foo/bar";
        final String[] args = {"--configDir", configDir};

        final ClusterConfiguration clusterConfiguration = commandLineParser.parse(args);

        assertThat(clusterConfiguration.getConfigDir()).isEqualTo(configDir);
    }

    @Test
    void testMissingRequiredArgument() {
        final String[] args = {};

        assertThatThrownBy(() -> commandLineParser.parse(args))
                .isInstanceOf(FlinkParseException.class);
    }
}
