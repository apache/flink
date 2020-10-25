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

package org.apache.flink.runtime.util.bash;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for {@link FlinkConfigLoader}.
 */
public class FlinkConfigLoaderTest extends TestLogger {

	private static final String TEST_CONFIG_KEY = "test.key";
	private static final String TEST_CONFIG_VALUE = "test_value";

	@Rule
	public TemporaryFolder confDir = new TemporaryFolder() {
		@Override
		protected void before() throws Throwable {
			super.create();
			File flinkConfFile = newFile("flink-conf.yaml");
			FileWriter fw = new FileWriter(flinkConfFile);
			fw.write(TEST_CONFIG_KEY + ": " + TEST_CONFIG_VALUE + "\n");
			fw.close();
		}
	};

	@Test
	public void testLoadConfigurationConfigDirLongOpt() throws Exception {
		String[] args = {"--configDir", confDir.getRoot().getAbsolutePath()};
		Configuration configuration = FlinkConfigLoader.loadConfiguration(args);
		verifyConfiguration(configuration, TEST_CONFIG_KEY, TEST_CONFIG_VALUE);
	}

	@Test
	public void testLoadConfigurationConfigDirShortOpt() throws Exception {
		String[] args = {"-c", confDir.getRoot().getAbsolutePath()};
		Configuration configuration = FlinkConfigLoader.loadConfiguration(args);
		verifyConfiguration(configuration, TEST_CONFIG_KEY, TEST_CONFIG_VALUE);
	}

	@Test
	public void testLoadConfigurationDynamicPropertyWithSpace() throws Exception {
		String[] args = {"--configDir", confDir.getRoot().getAbsolutePath(), "-D", "key=value"};
		Configuration configuration = FlinkConfigLoader.loadConfiguration(args);
		verifyConfiguration(configuration, "key", "value");
	}

	@Test
	public void testLoadConfigurationDynamicPropertyWithoutSpace() throws Exception {
		String[] args = {"--configDir", confDir.getRoot().getAbsolutePath(), "-Dkey=value"};
		Configuration configuration = FlinkConfigLoader.loadConfiguration(args);
		verifyConfiguration(configuration, "key", "value");
	}

	@Test
	public void testLoadConfigurationIgnoreUnknownToken() throws Exception {
		String [] args = {"unknown", "-u", "--configDir", confDir.getRoot().getAbsolutePath(), "--unknown", "-Dkey=value"};
		Configuration configuration = FlinkConfigLoader.loadConfiguration(args);
		verifyConfiguration(configuration, TEST_CONFIG_KEY, TEST_CONFIG_VALUE);
		verifyConfiguration(configuration, "key", "value");
	}

	private void verifyConfiguration(Configuration config, String key, String expectedValue) {
		ConfigOption<String> option = key(key).stringType().noDefaultValue();
		assertThat(config.get(option), is(expectedValue));
	}
}
