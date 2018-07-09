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

package org.apache.flink.container.entrypoint;

import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Properties;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link StandaloneJobClusterConfigurationParserFactory}.
 */
public class StandaloneJobClusterConfigurationParserFactoryTest extends TestLogger {

	private static final CommandLineParser<StandaloneJobClusterConfiguration> commandLineParser = new CommandLineParser<>(new StandaloneJobClusterConfigurationParserFactory());

	@Test
	public void testEntrypointClusterConfigurationParsing() throws FlinkParseException {
		final String configDir = "/foo/bar";
		final String key = "key";
		final String value = "value";
		final int restPort = 1234;
		final String jobClassName = "foobar";
		final String arg1 = "arg1";
		final String arg2 = "arg2";
		final String[] args = {"--configDir", configDir, "--webui-port", String.valueOf(restPort), "--job-classname", jobClassName, String.format("-D%s=%s", key, value), arg1, arg2};

		final StandaloneJobClusterConfiguration clusterConfiguration = commandLineParser.parse(args);

		assertThat(clusterConfiguration.getConfigDir(), is(equalTo(configDir)));
		assertThat(clusterConfiguration.getJobClassName(), is(equalTo(jobClassName)));
		assertThat(clusterConfiguration.getRestPort(), is(equalTo(restPort)));
		final Properties dynamicProperties = clusterConfiguration.getDynamicProperties();

		assertThat(dynamicProperties, hasEntry(key, value));

		assertThat(clusterConfiguration.getArgs(), arrayContaining(arg1, arg2));
	}

	@Test
	public void testOnlyRequiredArguments() throws FlinkParseException {
		final String configDir = "/foo/bar";
		final String jobClassName = "foobar";
		final String[] args = {"--configDir", configDir, "--job-classname", jobClassName};

		final StandaloneJobClusterConfiguration clusterConfiguration = commandLineParser.parse(args);

		assertThat(clusterConfiguration.getConfigDir(), is(equalTo(configDir)));
		assertThat(clusterConfiguration.getJobClassName(), is(equalTo(jobClassName)));
		assertThat(clusterConfiguration.getRestPort(), is(equalTo(-1)));
	}

	@Test(expected = FlinkParseException.class)
	public void testMissingRequiredArgument() throws FlinkParseException {
		final String[] args = {};

		commandLineParser.parse(args);
	}
}
