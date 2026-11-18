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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link EntrypointClusterConfigurationParserFactory}. */
class EntrypointClusterConfigurationParserFactoryTest {

    private static final CommandLineParser<EntrypointClusterConfiguration> commandLineParser =
            new CommandLineParser<>(new EntrypointClusterConfigurationParserFactory());

    @Test
    void testEntrypointClusterConfigurationParsing() throws FlinkParseException {
        final String configDir = "/foo/bar";
        final int restPort = 1234;
        final String key = "key";
        final String value = "value";
        final String arg1 = "arg1";
        final String arg2 = "arg2";
        final String[] args = {
            "--configDir",
            configDir,
            "-D",
            "jobmanager.rpc.address=localhost",
            "-D",
            "rest.port=" + restPort,
            String.format("-D%s=%s", key, value),
            arg1,
            arg2
        };

        final EntrypointClusterConfiguration clusterConfiguration = commandLineParser.parse(args);
        final Configuration dynamicPropertiesConfig =
                ConfigurationUtils.createConfiguration(clusterConfiguration.getDynamicProperties());

        assertThat(clusterConfiguration.getConfigDir()).isEqualTo(configDir);
        assertThat(dynamicPropertiesConfig.get(RestOptions.PORT)).isEqualTo(restPort);
        final Properties dynamicProperties = clusterConfiguration.getDynamicProperties();

        assertThat(dynamicProperties).containsEntry(key, value);

        assertThat(clusterConfiguration.getArgs()).containsExactly(arg1, arg2);
    }

    @Test
    void testOnlyRequiredArguments() throws FlinkParseException {
        final String configDir = "/foo/bar";
        final String[] args = {"--configDir", configDir};

        final EntrypointClusterConfiguration clusterConfiguration = commandLineParser.parse(args);
        final Configuration dynamicProperties =
                ConfigurationUtils.createConfiguration(clusterConfiguration.getDynamicProperties());

        assertThat(clusterConfiguration.getConfigDir()).isEqualTo(configDir);
        assertThat(dynamicProperties.get(RestOptions.PORT)).isEqualTo(8081);
    }

    @Test
    void testMissingRequiredArgument() {
        final String[] args = {};

        assertThatThrownBy(() -> commandLineParser.parse(args))
                .isInstanceOf(FlinkParseException.class);
    }
}
