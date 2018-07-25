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

package org.apache.flink.runtime.clusterframework;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;

import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

/**
 * Tests for {@link BootstrapToolsTest}.
 */
public class BootstrapToolsTest {

	@Test
	public void testSubstituteConfigKey() {
		String deprecatedKey1 = "deprecated-key";
		String deprecatedKey2 = "another-out_of-date_key";
		String deprecatedKey3 = "yet-one-more";

		String designatedKey1 = "newkey1";
		String designatedKey2 = "newKey2";
		String designatedKey3 = "newKey3";

		String value1 = "value1";
		String value2Designated = "designated-value2";
		String value2Deprecated = "deprecated-value2";

		// config contains only deprecated key 1, and for key 2 both deprecated and designated
		Configuration cfg = new Configuration();
		cfg.setString(deprecatedKey1, value1);
		cfg.setString(deprecatedKey2, value2Deprecated);
		cfg.setString(designatedKey2, value2Designated);

		BootstrapTools.substituteDeprecatedConfigKey(cfg, deprecatedKey1, designatedKey1);
		BootstrapTools.substituteDeprecatedConfigKey(cfg, deprecatedKey2, designatedKey2);
		BootstrapTools.substituteDeprecatedConfigKey(cfg, deprecatedKey3, designatedKey3);

		// value 1 should be set to designated
		assertEquals(value1, cfg.getString(designatedKey1, null));

		// value 2 should not have been set, since it had a value already
		assertEquals(value2Designated, cfg.getString(designatedKey2, null));

		// nothing should be in there for key 3
		assertNull(cfg.getString(designatedKey3, null));
		assertNull(cfg.getString(deprecatedKey3, null));
	}

	@Test
	public void testSubstituteConfigKeyPrefix() {
		String deprecatedPrefix1 = "deprecated-prefix";
		String deprecatedPrefix2 = "-prefix-2";
		String deprecatedPrefix3 = "prefix-3";

		String designatedPrefix1 = "p1";
		String designatedPrefix2 = "ppp";
		String designatedPrefix3 = "zzz";

		String depr1 = deprecatedPrefix1 + "var";
		String depr2 = deprecatedPrefix2 + "env";
		String depr3 = deprecatedPrefix2 + "x";

		String desig1 = designatedPrefix1 + "var";
		String desig2 = designatedPrefix2 + "env";
		String desig3 = designatedPrefix2 + "x";

		String val1 = "1";
		String val2 = "2";
		String val3Depr = "3-";
		String val3Desig = "3+";

		// config contains only deprecated key 1, and for key 2 both deprecated and designated
		Configuration cfg = new Configuration();
		cfg.setString(depr1, val1);
		cfg.setString(depr2, val2);
		cfg.setString(depr3, val3Depr);
		cfg.setString(desig3, val3Desig);

		BootstrapTools.substituteDeprecatedConfigPrefix(cfg, deprecatedPrefix1, designatedPrefix1);
		BootstrapTools.substituteDeprecatedConfigPrefix(cfg, deprecatedPrefix2, designatedPrefix2);
		BootstrapTools.substituteDeprecatedConfigPrefix(cfg, deprecatedPrefix3, designatedPrefix3);

		assertEquals(val1, cfg.getString(desig1, null));
		assertEquals(val2, cfg.getString(desig2, null));
		assertEquals(val3Desig, cfg.getString(desig3, null));

		// check that nothing with prefix 3 is contained
		for (String key : cfg.keySet()) {
			assertFalse(key.startsWith(designatedPrefix3));
			assertFalse(key.startsWith(deprecatedPrefix3));
		}
	}

	@Test
	public void testGetTaskManagerShellCommand() {
		final Configuration cfg = new Configuration();
		final ContaineredTaskManagerParameters containeredParams =
			new ContaineredTaskManagerParameters(1024, 768, 256, 4,
				new HashMap<String, String>());

		// no logging, with/out krb5
		final String java = "$JAVA_HOME/bin/java";
		final String jvmmem = "-Xms768m -Xmx768m -XX:MaxDirectMemorySize=256m";
		final String jvmOpts = "-Djvm"; // if set
		final String tmJvmOpts = "-DtmJvm"; // if set
		final String logfile = "-Dlog.file=./logs/taskmanager.log"; // if set
		final String logback =
			"-Dlogback.configurationFile=file:./conf/logback.xml"; // if set
		final String log4j =
			"-Dlog4j.configuration=file:./conf/log4j.properties"; // if set
		final String mainClass =
			"org.apache.flink.runtime.clusterframework.BootstrapToolsTest";
		final String args = "--configDir ./conf";
		final String redirects =
			"1> ./logs/taskmanager.out 2> ./logs/taskmanager.err";

		assertEquals(
			java + " " + jvmmem +
				" " + // jvmOpts
				" " + // logging
				" " + mainClass + " " + args + " " + redirects,
			BootstrapTools
				.getTaskManagerShellCommand(cfg, containeredParams, "./conf", "./logs",
					false, false, false, this.getClass()));

		final String krb5 = "-Djava.security.krb5.conf=krb5.conf";
		assertEquals(
			java + " " + jvmmem +
				" " + " " + krb5 + // jvmOpts
				" " + // logging
				" " + mainClass + " " + args + " " + redirects,
			BootstrapTools
				.getTaskManagerShellCommand(cfg, containeredParams, "./conf", "./logs",
					false, false, true, this.getClass()));

		// logback only, with/out krb5
		assertEquals(
			java + " " + jvmmem +
				" " + // jvmOpts
				" " + logfile + " " + logback +
				" " + mainClass + " " + args + " " + redirects,
			BootstrapTools
				.getTaskManagerShellCommand(cfg, containeredParams, "./conf", "./logs",
					true, false, false, this.getClass()));

		assertEquals(
			java + " " + jvmmem +
				" " + " " + krb5 + // jvmOpts
				" " + logfile + " " + logback +
				" " + mainClass + " " + args + " " + redirects,
			BootstrapTools
				.getTaskManagerShellCommand(cfg, containeredParams, "./conf", "./logs",
					true, false, true, this.getClass()));

		// log4j, with/out krb5
		assertEquals(
			java + " " + jvmmem +
				" " + // jvmOpts
				" " + logfile + " " + log4j +
				" " + mainClass + " " + args + " " + redirects,
			BootstrapTools
				.getTaskManagerShellCommand(cfg, containeredParams, "./conf", "./logs",
					false, true, false, this.getClass()));

		assertEquals(
			java + " " + jvmmem +
				" " + " " + krb5 + // jvmOpts
				" " + logfile + " " + log4j +
				" " + mainClass + " " + args + " " + redirects,
			BootstrapTools
				.getTaskManagerShellCommand(cfg, containeredParams, "./conf", "./logs",
					false, true, true, this.getClass()));

		// logback + log4j, with/out krb5
		assertEquals(
			java + " " + jvmmem +
				" " + // jvmOpts
				" " + logfile + " " + logback + " " + log4j +
				" " + mainClass + " " + args + " " + redirects,
			BootstrapTools
				.getTaskManagerShellCommand(cfg, containeredParams, "./conf", "./logs",
					true, true, false, this.getClass()));

		assertEquals(
			java + " " + jvmmem +
				" " + " " + krb5 + // jvmOpts
				" " + logfile + " " + logback + " " + log4j +
				" " + mainClass + " " + args + " " + redirects,
			BootstrapTools
				.getTaskManagerShellCommand(cfg, containeredParams, "./conf", "./logs",
					true, true, true, this.getClass()));

		// logback + log4j, with/out krb5, different JVM opts
		cfg.setString(CoreOptions.FLINK_JVM_OPTIONS, jvmOpts);
		assertEquals(
			java + " " + jvmmem +
				" " + jvmOpts +
				" " + logfile + " " + logback + " " + log4j +
				" " + mainClass + " " + args + " " + redirects,
			BootstrapTools
				.getTaskManagerShellCommand(cfg, containeredParams, "./conf", "./logs",
					true, true, false, this.getClass()));

		assertEquals(
			java + " " + jvmmem +
				" " + jvmOpts + " " + krb5 + // jvmOpts
				" " + logfile + " " + logback + " " + log4j +
				" " + mainClass + " " + args + " " + redirects,
			BootstrapTools
				.getTaskManagerShellCommand(cfg, containeredParams, "./conf", "./logs",
					true, true, true, this.getClass()));

		// logback + log4j, with/out krb5, different JVM opts
		cfg.setString(CoreOptions.FLINK_TM_JVM_OPTIONS, tmJvmOpts);
		assertEquals(
			java + " " + jvmmem +
				" " + jvmOpts + " " + tmJvmOpts +
				" " + logfile + " " + logback + " " + log4j +
				" " + mainClass + " " + args + " " + redirects,
			BootstrapTools
				.getTaskManagerShellCommand(cfg, containeredParams, "./conf", "./logs",
					true, true, false, this.getClass()));

		assertEquals(
			java + " " + jvmmem +
				" " + jvmOpts + " " + tmJvmOpts + " " + krb5 + // jvmOpts
				" " + logfile + " " + logback + " " + log4j +
				" " + mainClass + " " + args + " " + redirects,
			BootstrapTools
				.getTaskManagerShellCommand(cfg, containeredParams, "./conf", "./logs",
					true, true, true, this.getClass()));

		// now try some configurations with different yarn.container-start-command-template

		cfg.setString(ConfigConstants.YARN_CONTAINER_START_COMMAND_TEMPLATE,
			"%java% 1 %jvmmem% 2 %jvmopts% 3 %logging% 4 %class% 5 %args% 6 %redirects%");
		assertEquals(
			java + " 1 " + jvmmem +
				" 2 " + jvmOpts + " " + tmJvmOpts + " " + krb5 + // jvmOpts
				" 3 " + logfile + " " + logback + " " + log4j +
				" 4 " + mainClass + " 5 " + args + " 6 " + redirects,
			BootstrapTools
				.getTaskManagerShellCommand(cfg, containeredParams, "./conf", "./logs",
					true, true, true, this.getClass()));

		cfg.setString(ConfigConstants.YARN_CONTAINER_START_COMMAND_TEMPLATE,
			"%java% %logging% %jvmopts% %jvmmem% %class% %args% %redirects%");
		assertEquals(
			java +
				" " + logfile + " " + logback + " " + log4j +
				" " + jvmOpts + " " + tmJvmOpts + " " + krb5 + // jvmOpts
				" " + jvmmem +
				" " + mainClass + " " + args + " " + redirects,
			BootstrapTools
				.getTaskManagerShellCommand(cfg, containeredParams, "./conf", "./logs",
					true, true, true, this.getClass()));

	}

	@Test
	public void testUpdateTmpDirectoriesInConfiguration() {
		Configuration config = new Configuration();

		// test that default value is taken
		BootstrapTools.updateTmpDirectoriesInConfiguration(config, "default/directory/path");
		assertEquals(config.getString(CoreOptions.TMP_DIRS), "default/directory/path");

		// test that we ignore default value is value is set before
		BootstrapTools.updateTmpDirectoriesInConfiguration(config, "not/default/directory/path");
		assertEquals(config.getString(CoreOptions.TMP_DIRS), "default/directory/path");

		//test that empty value is not a magic string
		config.setString(CoreOptions.TMP_DIRS, "");
		BootstrapTools.updateTmpDirectoriesInConfiguration(config, "some/new/path");
		assertEquals(config.getString(CoreOptions.TMP_DIRS), "");
	}

	@Test
	public void testShouldNotUpdateTmpDirectoriesInConfigurationIfNoValueConfigured() {
		Configuration config = new Configuration();
		BootstrapTools.updateTmpDirectoriesInConfiguration(config, null);
		assertEquals(config.getString(CoreOptions.TMP_DIRS), CoreOptions.TMP_DIRS.defaultValue());
	}
}
