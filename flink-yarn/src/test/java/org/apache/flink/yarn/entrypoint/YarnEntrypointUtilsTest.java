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

package org.apache.flink.yarn.entrypoint;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.util.TestLogger;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link YarnEntrypointUtils}.
 */
public class YarnEntrypointUtilsTest extends TestLogger {

	private static final Logger LOG = LoggerFactory.getLogger(YarnEntrypointUtilsTest.class);

	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	/**
	 * Tests that the REST ports are correctly set when loading a {@link Configuration}
	 * with unspecified REST options.
	 */
	@Test
	public void testRestPortOptionsUnspecified() throws IOException {
		final Configuration initialConfiguration = new Configuration();

		final Configuration configuration = loadConfiguration(initialConfiguration);

		// having not specified the ports should set the rest bind port to 0
		assertThat(configuration.getString(RestOptions.BIND_PORT), is(equalTo("0")));
	}

	/**
	 * Tests that the binding REST port is set to the REST port if set.
	 */
	@Test
	public void testRestPortSpecified() throws IOException {
		final Configuration initialConfiguration = new Configuration();
		final int port = 1337;
		initialConfiguration.setInteger(RestOptions.PORT, port);

		final Configuration configuration = loadConfiguration(initialConfiguration);

		// if the bind port is not specified it should fall back to the rest port
		assertThat(configuration.getString(RestOptions.BIND_PORT), is(equalTo(String.valueOf(port))));
	}

	/**
	 * Tests that the binding REST port has precedence over the REST port if both are set.
	 */
	@Test
	public void testRestPortAndBindingPortSpecified() throws IOException {
		final Configuration initialConfiguration = new Configuration();
		final int port = 1337;
		final String bindingPortRange = "1337-7331";
		initialConfiguration.setInteger(RestOptions.PORT, port);
		initialConfiguration.setString(RestOptions.BIND_PORT, bindingPortRange);

		final Configuration configuration = loadConfiguration(initialConfiguration);

		// bind port should have precedence over the rest port
		assertThat(configuration.getString(RestOptions.BIND_PORT), is(equalTo(bindingPortRange)));
	}

	@Nonnull
	private static Configuration loadConfiguration(Configuration initialConfiguration) throws IOException {
		final File workingDirectory = TEMPORARY_FOLDER.newFolder();
		final Map<String, String> env = new HashMap<>(4);
		env.put(ApplicationConstants.Environment.NM_HOST.key(), "foobar");

		BootstrapTools.writeConfiguration(initialConfiguration, new File(workingDirectory, "flink-conf.yaml"));
		return YarnEntrypointUtils.loadConfiguration(workingDirectory.getAbsolutePath(), env, LOG);
	}

}
