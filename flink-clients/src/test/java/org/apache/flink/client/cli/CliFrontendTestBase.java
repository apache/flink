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

package org.apache.flink.client.cli;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.util.TestLogger;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

/**
 * Base test class for {@link CliFrontend} tests that wraps the new vs. legacy mode.
 */
@RunWith(Parameterized.class)
public abstract class CliFrontendTestBase extends TestLogger {
	@Parameterized.Parameter
	public String mode;

	@Parameterized.Parameters(name = "Mode = {0}")
	public static List<String> parameters() {
		return Arrays.asList(CoreOptions.LEGACY_MODE, CoreOptions.NEW_MODE);
	}

	protected Configuration getConfiguration() {
		final Configuration configuration = GlobalConfiguration
			.loadConfiguration(CliFrontendTestUtils.getConfigDir());
		configuration.setString(CoreOptions.MODE, mode);
		return configuration;
	}

	static AbstractCustomCommandLine<?> getCli(Configuration configuration) {
		switch (configuration.getString(CoreOptions.MODE)) {
			case CoreOptions.LEGACY_MODE:
				return new LegacyCLI(configuration);
			case CoreOptions.NEW_MODE:
				return new DefaultCLI(configuration);
		}
		throw new IllegalStateException();
	}
}
