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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Optional;
import java.util.Properties;

import static org.apache.flink.container.entrypoint.StandaloneJobClusterConfigurationParserFactory.DEFAULT_JOB_ID;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link StandaloneJobClusterConfigurationParserFactory}.
 */
public class StandaloneJobClusterConfigurationParserFactoryTest extends TestLogger {

	private static final CommandLineParser<StandaloneJobClusterConfiguration> commandLineParser = new CommandLineParser<>(new StandaloneJobClusterConfigurationParserFactory());
	private static final String JOB_CLASS_NAME = "foobar";
	private static final String CONFIG_DIR = "/foo/bar";

	@Test
	public void testEntrypointClusterConfigurationParsing() throws FlinkParseException {
		final String key = "key";
		final String value = "value";
		final int restPort = 1234;
		final String arg1 = "arg1";
		final String arg2 = "arg2";
		final String[] args = {"--configDir", CONFIG_DIR, "--webui-port", String.valueOf(restPort), "--job-classname", JOB_CLASS_NAME, String.format("-D%s=%s", key, value), arg1, arg2};

		final StandaloneJobClusterConfiguration clusterConfiguration = commandLineParser.parse(args);

		assertThat(clusterConfiguration.getConfigDir(), is(equalTo(CONFIG_DIR)));
		assertThat(clusterConfiguration.getJobClassName(), is(equalTo(JOB_CLASS_NAME)));
		assertThat(clusterConfiguration.getRestPort(), is(equalTo(restPort)));
		final Properties dynamicProperties = clusterConfiguration.getDynamicProperties();

		assertThat(dynamicProperties, hasEntry(key, value));

		assertThat(clusterConfiguration.getArgs(), arrayContaining(arg1, arg2));

		assertThat(clusterConfiguration.getSavepointRestoreSettings(), is(equalTo(SavepointRestoreSettings.none())));

		assertThat(clusterConfiguration.getJobId(), is(equalTo(DEFAULT_JOB_ID)));
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

	@Test
	public void testSavepointRestoreSettingsParsing() throws FlinkParseException {
		final String restorePath = "foobar";
		final String[] args = {"-c", CONFIG_DIR, "-j", JOB_CLASS_NAME, "-s", restorePath, "-n"};
		final StandaloneJobClusterConfiguration standaloneJobClusterConfiguration = commandLineParser.parse(args);

		final SavepointRestoreSettings savepointRestoreSettings = standaloneJobClusterConfiguration.getSavepointRestoreSettings();

		assertThat(savepointRestoreSettings.restoreSavepoint(), is(true));
		assertThat(savepointRestoreSettings.getRestorePath(), is(equalTo(restorePath)));
		assertThat(savepointRestoreSettings.allowNonRestoredState(), is(true));
	}

	@Test
	public void testSetJobIdManually() throws FlinkParseException {
		final JobID jobId = new JobID();
		final String[] args = {"--configDir", "/foo/bar", "--job-classname", "foobar", "--job-id", jobId.toString()};

		final StandaloneJobClusterConfiguration standaloneJobClusterConfiguration = commandLineParser.parse(args);

		assertThat(standaloneJobClusterConfiguration.getJobId(), is(equalTo(jobId)));
	}

	@Test
	public void testInvalidJobIdThrows() {
		final String invalidJobId = "0xINVALID";
		final String[] args = {"--configDir", "/foo/bar", "--job-classname", "foobar", "--job-id", invalidJobId};

		try {
			commandLineParser.parse(args);
			fail("Did not throw expected FlinkParseException");
		} catch (FlinkParseException e) {
			Optional<IllegalArgumentException> cause = ExceptionUtils.findThrowable(e, IllegalArgumentException.class);
			assertTrue(cause.isPresent());
			assertThat(cause.get().getMessage(), containsString(invalidJobId));
		}
	}

	@Test
	public void testShortOptions() throws FlinkParseException {
		final String configDir = "/foo/bar";
		final String jobClassName = "foobar";
		final JobID jobId = new JobID();
		final String savepointRestorePath = "s3://foo/bar";

		final String[] args = {
			"-c", configDir,
			"-j", jobClassName,
			"-jid", jobId.toString(),
			"-s", savepointRestorePath,
			"-n"};

		final StandaloneJobClusterConfiguration clusterConfiguration = commandLineParser.parse(args);

		assertThat(clusterConfiguration.getConfigDir(), is(equalTo(configDir)));
		assertThat(clusterConfiguration.getJobClassName(), is(equalTo(jobClassName)));
		assertThat(clusterConfiguration.getJobId(), is(equalTo(jobId)));

		final SavepointRestoreSettings savepointRestoreSettings = clusterConfiguration.getSavepointRestoreSettings();
		assertThat(savepointRestoreSettings.restoreSavepoint(), is(true));
		assertThat(savepointRestoreSettings.getRestorePath(), is(equalTo(savepointRestorePath)));
		assertThat(savepointRestoreSettings.allowNonRestoredState(), is(true));
	}

}
